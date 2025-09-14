package es

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
)

type EsCfg struct {
	Es   elasticsearch.Config
	Ping bool
}

func GetEsInstance(cfg EsCfg) (*elasticsearch.Client, error) {
	client, err := elasticsearch.NewClient(cfg.Es)
	if err != nil {
		return nil, err
	}
	return client, nil
}

type IndexInfo struct {
	Index string `json:"index"`
}

type SearchResponse struct {
	ScrollID string `json:"_scroll_id"`
	Hits     struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
		Hits []struct {
			Index  string          `json:"_index"`
			ID     string          `json:"_id"`
			Source json.RawMessage `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type BulkDoc struct {
	Index string
	ID    string
	Doc   json.RawMessage
}

func bulkIndex(client *elasticsearch.Client, docs []BulkDoc) error {
	const minBatchSize = 1

	if len(docs) == 0 {
		return nil
	}

	var buf bytes.Buffer

	for _, doc := range docs {
		action := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": doc.Index,
				"_id":    doc.ID,
			},
		}

		actionJSON, err := json.Marshal(action)
		if err != nil {
			return err
		}

		buf.Write(actionJSON)
		buf.WriteByte('\n')
		buf.Write(doc.Doc)
		buf.WriteByte('\n')
	}

	res, err := client.Bulk(
		&buf,
		client.Bulk.WithRefresh("false"),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == 413 {
		log.Printf("Got 413 error with batch size %d, retrying with smaller batch", len(docs))

		if len(docs) <= minBatchSize {
			return fmt.Errorf("document too large: cannot process even with batch size of 1")
		}

		mid := len(docs) / 2

		if err := bulkIndex(client, docs[:mid]); err != nil {
			return err
		}

		if err := bulkIndex(client, docs[mid:]); err != nil {
			return err
		}

		return nil
	}

	if res.IsError() {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("bulk error with status %d: %s", res.StatusCode, res.Status())
		}

		var bulkResp map[string]interface{}
		if err := json.Unmarshal(body, &bulkResp); err != nil {
			if strings.Contains(string(body), "413") || strings.Contains(string(body), "entity too large") {
				log.Printf("Got 413 error in response body with batch size %d, retrying with smaller batch", len(docs))

				if len(docs) <= minBatchSize {
					return fmt.Errorf("document too large: cannot process even with batch size of 1")
				}

				mid := len(docs) / 2
				if err := bulkIndex(client, docs[:mid]); err != nil {
					return err
				}
				if err := bulkIndex(client, docs[mid:]); err != nil {
					return err
				}
				return nil
			}
			return fmt.Errorf("bulk error: %s, body: %s", res.Status(), string(body))
		}

		if errors, ok := bulkResp["errors"].(bool); ok && errors {
			if items, ok := bulkResp["items"].([]interface{}); ok {
				for _, item := range items {
					if indexItem, ok := item.(map[string]interface{})["index"].(map[string]interface{}); ok {
						if status, ok := indexItem["status"].(float64); ok && int(status) == 413 {
							log.Printf("Got 413 error in bulk response with batch size %d, retrying with smaller batch", len(docs))

							if len(docs) <= minBatchSize {
								return fmt.Errorf("document too large: cannot process even with batch size of 1")
							}

							mid := len(docs) / 2
							if err := bulkIndex(client, docs[:mid]); err != nil {
								return err
							}
							if err := bulkIndex(client, docs[mid:]); err != nil {
								return err
							}
							return nil
						}
					}
				}
			}
			return fmt.Errorf("bulk operation had errors: %v", bulkResp)
		}
	}

	return nil
}
