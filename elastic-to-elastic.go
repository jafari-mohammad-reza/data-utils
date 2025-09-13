package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

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

func main() {
	sourceCfg := elasticsearch.Config{
		Addresses: []string{
			"https://10.202.18.33:9200",
		},
		Username: "siwan",
		Password: "@Siwan@853976",
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 10,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	source, err := elasticsearch.NewClient(sourceCfg)
	if err != nil {
		panic(err)
	}

	destCfg := elasticsearch.Config{
		Addresses: []string{
			"https://10.202.18.100:9200",
		},
		Username: "m.jafari",
		Password: "njj3bAatnSHWmbK",
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 10,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	dest, err := elasticsearch.NewClient(destCfg)
	if err != nil {
		panic(err)
	}
	// filterDetectors, err := getIndices(source, "mitigated-attacks*")
	// filterDetectors, err := getIndices(source, "irr")
	// filterDetectors, err := getIndices(source, "mitigator_lookup_log*")
	// filterDetectors, err := getIndices(source, "public-chart*")
	// filterDetectors, err := getIndices(source, "rx-tx*")
	filterDetectors, err := getIndices(source, "stage_ms_reporter*")
	if err != nil {
		log.Fatalf("Failed to get indices: %v", err)
	}
	wg := sync.WaitGroup{}
	semaphore := make(chan struct{}, 3)
	for _, indice := range filterDetectors {
		wg.Go(func() {
			fmt.Printf("copying %s\n", indice)
			semaphore <- struct{}{} // to block the main routine to maximum concurrent fetch
			defer func() { <-semaphore }()
			if err := copyMapping(source, dest, indice); err != nil {
				fmt.Printf("failed to copy %s from source to dest: %s\n", indice, err.Error())
				return
			}
			if err := copyIndexData(source, dest, indice); err != nil {
				log.Printf("failed to copy %s data: %s", indice, err.Error())
				return
			}
			log.Printf("Successfully copied index: %s", indice)

		})
	}
	wg.Wait()
}
func getIndices(client *elasticsearch.Client, pattern string) ([]string, error) {
	res, err := client.Cat.Indices(
		client.Cat.Indices.WithIndex(pattern),
		client.Cat.Indices.WithFormat("json"),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error response: %s", res.String())
	}

	var indices []IndexInfo
	if err := json.NewDecoder(res.Body).Decode(&indices); err != nil {
		return nil, err
	}

	result := make([]string, len(indices))
	for i, idx := range indices {
		result[i] = idx.Index
	}

	return result, nil
}
func copyMapping(source, dest *elasticsearch.Client, indexName string) error {
	res, err := source.Indices.GetMapping(
		source.Indices.GetMapping.WithIndex(indexName),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to get mapping: %s", res.String())
	}

	mapping, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	var mappingData map[string]interface{}
	if err := json.Unmarshal(mapping, &mappingData); err != nil {
		return err
	}

	indexMapping, ok := mappingData[indexName].(map[string]interface{})
	if !ok {
		return fmt.Errorf("mapping not found for index %s", indexName)
	}

	mappingJSON, err := json.Marshal(indexMapping)
	if err != nil {
		return err
	}

	res, err = dest.Indices.Create(
		indexName,
		dest.Indices.Create.WithBody(bytes.NewReader(mappingJSON)),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		if strings.Contains(res.String(), "resource_already_exists_exception") {
			log.Printf("Index %s already exists, skipping mapping creation", indexName)
			return nil
		}
		return fmt.Errorf("failed to create index: %s", res.String())
	}

	return nil
}

func copyIndexData(source, dest *elasticsearch.Client, indexName string) error {

	size := 5000

	res, err := source.Search(
		source.Search.WithIndex(indexName),
		source.Search.WithSize(size),
		source.Search.WithScroll(time.Duration(2)*time.Minute),
		source.Search.WithSort("_doc"),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("search error: %s", res.String())
	}

	var searchResp SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&searchResp); err != nil {
		return err
	}

	totalDocs := searchResp.Hits.Total.Value
	log.Printf("Index %s has %d documents to copy", indexName, totalDocs)

	processed := 0

	if len(searchResp.Hits.Hits) > 0 {
		docs := make([]BulkDoc, len(searchResp.Hits.Hits))
		for i, hit := range searchResp.Hits.Hits {
			docs[i] = BulkDoc{
				Index: hit.Index,
				ID:    hit.ID,
				Doc:   hit.Source,
			}
		}

		if err := bulkIndex(dest, docs); err != nil {
			return err
		}

		processed += len(docs)
		log.Printf("Index %s: processed %d/%d documents", indexName, processed, totalDocs)
	}

	scrollID := searchResp.ScrollID
	for {
		res, err := source.Scroll(
			source.Scroll.WithScrollID(scrollID),
			source.Scroll.WithScroll(time.Duration(2)*time.Minute),
		)
		if err != nil {
			return err
		}

		if res.IsError() {
			res.Body.Close()
			return fmt.Errorf("scroll error: %s", res.String())
		}

		var scrollResp SearchResponse
		if err := json.NewDecoder(res.Body).Decode(&scrollResp); err != nil {
			res.Body.Close()
			return err
		}
		res.Body.Close()

		if len(scrollResp.Hits.Hits) == 0 {
			break
		}

		docs := make([]BulkDoc, len(scrollResp.Hits.Hits))
		for i, hit := range scrollResp.Hits.Hits {
			docs[i] = BulkDoc{
				Index: hit.Index,
				ID:    hit.ID,
				Doc:   hit.Source,
			}
		}

		if err := bulkIndex(dest, docs); err != nil {
			return err
		}

		processed += len(docs)
		log.Printf("Index %s: processed %d/%d documents", indexName, processed, totalDocs)

		scrollID = scrollResp.ScrollID
	}

	clearScrollReq := esapi.ClearScrollRequest{
		ScrollID: []string{scrollID},
	}
	clearRes, err := clearScrollReq.Do(context.Background(), source)
	if err == nil {
		clearRes.Body.Close()
	}

	return nil
}

func bulkIndex(client *elasticsearch.Client, docs []BulkDoc) error {
	// Minimum batch size to prevent infinite recursion
	const minBatchSize = 1

	// Check if batch is too small to split further
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

	// Check for 413 error
	if res.StatusCode == 413 {
		log.Printf("Got 413 error with batch size %d, retrying with smaller batch", len(docs))

		// If we're already at minimum batch size, we can't split further
		if len(docs) <= minBatchSize {
			return fmt.Errorf("document too large: cannot process even with batch size of 1")
		}

		// Split the batch in half
		mid := len(docs) / 2

		// Process first half
		if err := bulkIndex(client, docs[:mid]); err != nil {
			return err
		}

		// Process second half
		if err := bulkIndex(client, docs[mid:]); err != nil {
			return err
		}

		return nil
	}

	if res.IsError() {
		// Check if it's another type of error that might include 413 in the body
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("bulk error with status %d: %s", res.StatusCode, res.Status())
		}

		// Try to parse the error response
		var bulkResp map[string]interface{}
		if err := json.Unmarshal(body, &bulkResp); err != nil {
			// If we can't parse JSON, check if the raw response mentions 413
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

		// Check for errors in the parsed response
		if errors, ok := bulkResp["errors"].(bool); ok && errors {
			// Check if any item has a 413 error
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
