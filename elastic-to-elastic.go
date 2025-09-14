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
		if err := copyIndiceWithDateRange(source, dest, "filtered_detector*", "2024-02-20T03:30:00.000Z", "2024-12-03T20:30:00.000Z"); err != nil {
		panic(err)
	}
}

func copyIndiceWithDateRange(source, dest *elasticsearch.Client, indexPattern, gte, lte string) error {
	indices, err := getIndices(source, indexPattern)
	if err != nil {
		return fmt.Errorf("failed to get indices: %w", err)
	}

	wg := sync.WaitGroup{}
	semaphore := make(chan struct{}, 3)

	for _, index := range indices {
		wg.Add(1)
		go func(idx string) {
			defer wg.Done()

			fmt.Printf("copying %s with date range\n", idx)
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := copyIndexDataWithRange(source, dest, idx, gte, lte); err != nil {
				log.Printf("failed to copy %s data with range: %s", idx, err.Error())
				return
			}

			log.Printf("Successfully copied index: %s with date range", idx)
		}(index)
	}

	wg.Wait()
	return nil
}

func copyIndexDataWithRange(source, dest *elasticsearch.Client, indexName string, gte, lte string) error {
	searchBody := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"filter": []map[string]interface{}{
					{
						"range": map[string]interface{}{
							"endedAt": map[string]string{
								"gte": gte,
								"lte": lte,
							},
						},
					},
				},
			},
		},
	}

	queryJSON, err := json.Marshal(searchBody)
	if err != nil {
		return fmt.Errorf("failed to marshal search query: %w", err)
	}

	size := 5000

	res, err := source.Search(
		source.Search.WithIndex(indexName),
		source.Search.WithBody(bytes.NewReader(queryJSON)),
		source.Search.WithSize(size),
		source.Search.WithScroll(time.Duration(2)*time.Minute),
		source.Search.WithSort("_doc"),
	)
	if err != nil {
		return fmt.Errorf("initial search error for index %s: %w", indexName, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("initial search error for index %s: %s", indexName, res.String())
	}

	var searchResp SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&searchResp); err != nil {
		return fmt.Errorf("failed to decode initial search response for index %s: %w", indexName, err)
	}

	totalDocs := searchResp.Hits.Total.Value
	log.Printf("Index %s: Found %d documents matching date range for copy", indexName, totalDocs)

	if totalDocs == 0 {
		log.Printf("Index %s: No documents found matching the date range, skipping", indexName)
		return nil
	}

	res, err = dest.Indices.Exists([]string{indexName})
	if err != nil {
		return fmt.Errorf("failed to check if index exists: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		if err := copyMapping(source, dest, indexName); err != nil {
			return fmt.Errorf("failed to copy mapping for new index %s: %w", indexName, err)
		}
	}

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
			return fmt.Errorf("failed to bulk index first batch for index %s: %w", indexName, err)
		}
		processed += len(docs)
		log.Printf("Index %s: Processed %d/%d documents (initial batch)", indexName, processed, totalDocs)
	}

	scrollID := searchResp.ScrollID
	for {
		if processed >= totalDocs {
			break
		}

		scrollRes, err := source.Scroll(
			source.Scroll.WithScrollID(scrollID),
			source.Scroll.WithScroll(time.Duration(2)*time.Minute),
		)
		if err != nil {
			return fmt.Errorf("scroll error for index %s: %w", indexName, err)
		}
		defer scrollRes.Body.Close()

		if scrollRes.IsError() {
			return fmt.Errorf("scroll error for index %s: %s", indexName, scrollRes.String())
		}

		var scrollResp SearchResponse
		if err := json.NewDecoder(scrollRes.Body).Decode(&scrollResp); err != nil {
			return fmt.Errorf("failed to decode scroll response for index %s: %w", indexName, err)
		}

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
			return fmt.Errorf("failed to bulk index scroll batch for index %s: %w", indexName, err)
		}

		processed += len(docs)
		log.Printf("Index %s: Processed %d/%d documents", indexName, processed, totalDocs)

		scrollID = scrollResp.ScrollID
	}

	clearScrollReq := esapi.ClearScrollRequest{
		ScrollID: []string{scrollID},
	}
	if _, err := clearScrollReq.Do(context.Background(), source); err != nil {
		log.Printf("Warning: failed to clear scroll context for index %s: %v", indexName, err)
	}

	return nil
}

func copyIndice(source, dest *elasticsearch.Client) {
	indice, err := getIndices(source, "filtered_detector")
	if err != nil {
		log.Fatalf("Failed to get indices: %v", err)
	}
	wg := sync.WaitGroup{}
	semaphore := make(chan struct{}, 3)
	for _, indice := range indice {
		wg.Add(1)
		go func(idx string) {
			defer wg.Done()
			fmt.Printf("copying %s\n", idx)
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			if err := copyMapping(source, dest, idx); err != nil {
				fmt.Printf("failed to copy %s from source to dest: %s\n", idx, err.Error())
				return
			}
			if err := copyIndexData(source, dest, idx); err != nil {
				log.Printf("failed to copy %s data: %s", idx, err.Error())
				return
			}
			log.Printf("Successfully copied index: %s", idx)

		}(indice)
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

	size := 1000

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
