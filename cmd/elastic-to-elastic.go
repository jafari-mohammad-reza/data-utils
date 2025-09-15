package main

import (
	"bytes"
	"context"
	"cpd/utils"
	"cpd/utils/es"
	"runtime"

	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "",
	Short: "es_to_es",
	Long:  `elastic to elastic data copy util`,
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Help()
		os.Exit(0)
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

func main() {
	rangeCommand := &cobra.Command{
		Use:   "range",
		Short: "copy range of data for specified index from source to dest",
		RunE: func(cmd *cobra.Command, args []string) error {

			index, _ := cmd.Flags().GetString("index")
			gte, _ := cmd.Flags().GetString("gte")
			lte, _ := cmd.Flags().GetString("lte")
			batch, _ := cmd.Flags().GetInt32("batch")
			timeout, _ := cmd.Flags().GetFloat32("timeout")
			background, _ := cmd.Flags().GetBool("background")

			gteTime, err := time.Parse(time.DateOnly, gte)
			if err != nil {
				return fmt.Errorf("invalid gte format: %s", err.Error())
			}
			lteTime, err := time.Parse(time.DateOnly, lte)
			if err != nil {
				return fmt.Errorf("invalid lte format: %s", err.Error())
			}
			if lteTime.Before(gteTime) {
				return fmt.Errorf("lte cannot be before gte")
			}

			if background {
				return utils.RunInBackground(cmd, []string{
					"copy", "range",
					"--index", index,
					"--gte", gte,
					"--lte", lte,
					"--batch", fmt.Sprintf("%d", batch),
					"--timeout", fmt.Sprintf("%f", timeout),
				})
			}
			source, dest, err := utils.GetEsSourceDest(cmd, args)
			if err != nil {
				return err
			}
			return copyIndiceWithDateRange(source, dest, index, gte, lte, batch, timeout)
		},
	}
	rangeCommand.Flags().String("source", "", "source elastic host")
	rangeCommand.MarkFlagRequired("source")

	rangeCommand.Flags().String("source-creds", "", "source-creds elastic credentials")
	rangeCommand.MarkFlagRequired("source-creds")

	rangeCommand.Flags().String("dest", "", "dest elastic host")
	rangeCommand.MarkFlagRequired("dest")

	rangeCommand.Flags().String("dest-creds", "", "dest-creds elastic credentials")
	rangeCommand.MarkFlagRequired("dest-creds")

	rangeCommand.Flags().String("index", "", "index to copy")
	rangeCommand.MarkFlagRequired("index")

	rangeCommand.Flags().String("gte", "", "define start of copy span")
	rangeCommand.MarkFlagRequired("gte")

	rangeCommand.Flags().String("lte", "", "define end of copy span")
	rangeCommand.MarkFlagRequired("lte")
	rangeCommand.Flags().Int32("batch", 10000, "batch size for each iteration")
	rangeCommand.Flags().Float32("timeout", 0.5, "timeout seconds in each iteration")
	rangeCommand.Flags().Bool("background", false, "Run the copy operation in background")

	rootCmd.AddCommand(rangeCommand)

	fullCpCommand := &cobra.Command{
		Use:   "full",
		Short: "copy all indices from source to dest",
		RunE: func(cmd *cobra.Command, args []string) error {
			index, _ := cmd.Flags().GetString("index")
			batch, _ := cmd.Flags().GetInt32("batch")
			workers, _ := cmd.Flags().GetInt32("workers")
			timeout, _ := cmd.Flags().GetFloat32("timeout")
			background, _ := cmd.Flags().GetBool("background")

			if background {
				return utils.RunInBackground(cmd, []string{
					"copy", "full",
					"--index", index,
					"--batch", fmt.Sprintf("%d", batch),
					"--workers", fmt.Sprintf("%d", workers),
					"--timeout", fmt.Sprintf("%f", timeout),
				})
			}
			source, dest, err := utils.GetEsSourceDest(cmd, args)
			if err != nil {
				return err
			}
			indexes, err := getIndices(source, fmt.Sprintf("%s*", index))
			if err != nil {
				return fmt.Errorf("failed to get indexes: %v", err)
			}

			wg := sync.WaitGroup{}
			semaphore := make(chan struct{}, workers)
			for _, indice := range indexes {
				wg.Add(1)
				go func(idx string) {
					defer wg.Done()
					semaphore <- struct{}{}
					defer func() { <-semaphore }()

					if err := copyMapping(source, dest, idx); err != nil {
						log.Printf("failed to copy mapping %s: %s", idx, err.Error())
						return
					}
					if err := copyIndexData(source, dest, idx, batch, timeout); err != nil {
						log.Printf("failed to copy data %s: %s", idx, err.Error())
						return
					}
				}(indice)
			}
			wg.Wait()
			return nil
		},
	}
	fullCpCommand.Flags().String("index", "", "index to copy")
	fullCpCommand.MarkFlagRequired("index")
	fullCpCommand.Flags().String("source", "", "source elastic host")
	fullCpCommand.MarkFlagRequired("source")

	fullCpCommand.Flags().String("source-creds", "", "source-creds elastic credentials")
	fullCpCommand.MarkFlagRequired("source-creds")

	fullCpCommand.Flags().String("dest", "", "dest elastic host")
	fullCpCommand.MarkFlagRequired("dest")

	fullCpCommand.Flags().String("dest-creds", "", "dest-creds elastic credentials")
	fullCpCommand.MarkFlagRequired("dest-creds")
	fullCpCommand.Flags().Int32("batch", 10000, "batch size for each iteration")
	fullCpCommand.Flags().Float32("timeout", 0.5, "timeout seconds in each iteration")
	fullCpCommand.Flags().Int32("workers", int32(runtime.NumCPU())/3, "number of concurrent workers")
	fullCpCommand.Flags().Bool("background", false, "Run the copy operation in background")
	rootCmd.AddCommand(fullCpCommand)
	cpMapping := &cobra.Command{
		Use:   "mappings",
		Short: "only copy mappings of indices from source to dest",
		RunE: func(cmd *cobra.Command, args []string) error {
			index, _ := cmd.Flags().GetString("index")
			source, dest, err := utils.GetEsSourceDest(cmd, args)
			if err != nil {
				return err
			}
			if err := copyMapping(source, dest, index); err != nil {
				fmt.Printf("failed to copy %s mappings from source to dest: %s\n", index, err.Error())
				return err
			}
			return nil
		},
	}
	cpMapping.Flags().String("index", "", "index to copy")
	cpMapping.MarkFlagRequired("index")
	cpMapping.Flags().String("source", "", "source elastic host")
	cpMapping.MarkFlagRequired("source")

	cpMapping.Flags().String("source-creds", "", "source-creds elastic credentials")
	cpMapping.MarkFlagRequired("source-creds")

	cpMapping.Flags().String("dest", "", "dest elastic host")
	cpMapping.MarkFlagRequired("dest")

	cpMapping.Flags().String("dest-creds", "", "dest-creds elastic credentials")
	cpMapping.MarkFlagRequired("dest-creds")
	rootCmd.AddCommand(cpMapping)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func copyIndiceWithDateRange(source, dest *elasticsearch.Client, indexPattern, gte, lte string, batch int32, timeout float32) error {
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

			if err := copyIndexDataWithRange(source, dest, idx, gte, lte, batch, timeout); err != nil {
				log.Printf("failed to copy %s data with range: %s", idx, err.Error())
				return
			}

			log.Printf("Successfully copied index: %s with date range", idx)
		}(index)
	}

	wg.Wait()
	return nil
}

type EsQuery map[string]any

func copyIndexDataWithRange(source, dest *elasticsearch.Client, indexName string, gte, lte string, batch int32, timeout float32) error {
	searchBody := EsQuery{
		"query": EsQuery{
			"bool": EsQuery{
				"filter": []EsQuery{
					{
						"range": EsQuery{
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

	res, err := source.Search(
		source.Search.WithIndex(indexName),
		source.Search.WithBody(bytes.NewReader(queryJSON)),
		source.Search.WithSize(int(batch)),
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

	var searchResp es.SearchResponse
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
		docs := make([]es.BulkDoc, len(searchResp.Hits.Hits))
		for i, hit := range searchResp.Hits.Hits {
			docs[i] = es.BulkDoc{
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
		time.Sleep(time.Duration(timeout) * time.Second)
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

		var scrollResp es.SearchResponse
		if err := json.NewDecoder(scrollRes.Body).Decode(&scrollResp); err != nil {
			return fmt.Errorf("failed to decode scroll response for index %s: %w", indexName, err)
		}

		if len(scrollResp.Hits.Hits) == 0 {
			break
		}

		docs := make([]es.BulkDoc, len(scrollResp.Hits.Hits))
		for i, hit := range scrollResp.Hits.Hits {
			docs[i] = es.BulkDoc{
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
		time.Sleep(time.Duration(timeout) * time.Second)
	}

	clearScrollReq := esapi.ClearScrollRequest{
		ScrollID: []string{scrollID},
	}
	if _, err := clearScrollReq.Do(context.Background(), source); err != nil {
		log.Printf("Warning: failed to clear scroll context for index %s: %v", indexName, err)
	}

	return nil
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

	var indices []es.IndexInfo
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

	var mappingData EsQuery
	if err := json.Unmarshal(mapping, &mappingData); err != nil {
		return err
	}

	indexMapping, ok := mappingData[indexName].(EsQuery)
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

func copyIndexData(source, dest *elasticsearch.Client, indexName string, batch int32, timeout float32) error {

	res, err := source.Search(
		source.Search.WithIndex(indexName),
		source.Search.WithSize(int(batch)),
		source.Search.WithScroll(time.Duration(timeout)*time.Minute),
		source.Search.WithSort("_doc"),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("search error: %s", res.String())
	}

	var searchResp es.SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&searchResp); err != nil {
		return err
	}

	totalDocs := searchResp.Hits.Total.Value
	log.Printf("Index %s has %d documents to copy", indexName, totalDocs)

	processed := 0

	if len(searchResp.Hits.Hits) > 0 {
		docs := make([]es.BulkDoc, len(searchResp.Hits.Hits))
		for i, hit := range searchResp.Hits.Hits {
			docs[i] = es.BulkDoc{
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

		var scrollResp es.SearchResponse
		if err := json.NewDecoder(res.Body).Decode(&scrollResp); err != nil {
			res.Body.Close()
			return err
		}
		res.Body.Close()

		if len(scrollResp.Hits.Hits) == 0 {
			break
		}

		docs := make([]es.BulkDoc, len(scrollResp.Hits.Hits))
		for i, hit := range scrollResp.Hits.Hits {
			docs[i] = es.BulkDoc{
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

func bulkIndex(client *elasticsearch.Client, docs []es.BulkDoc) error {
	const minBatchSize = 1

	if len(docs) == 0 {
		return nil
	}

	var buf bytes.Buffer

	for _, doc := range docs {
		action := EsQuery{
			"index": EsQuery{
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

		var bulkResp EsQuery
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
					if indexItem, ok := item.(EsQuery)["index"].(EsQuery); ok {
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
