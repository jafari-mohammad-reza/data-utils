package main

import (
	"bytes"
	"context"
	"cpd/utils"
	"cpd/utils/es"
	"cpd/utils/md"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var rootCmd = &cobra.Command{
	Use:   "",
	Short: "mongo_to_es",
	Long:  `mongo to elastic data copy util`,
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
			db, _ := cmd.Flags().GetString("database")
			coll, _ := cmd.Flags().GetString("collection")

			source, _ := cmd.Flags().GetString("source")
			sourceCreds, _ := cmd.Flags().GetString("source-creds")

			dest, _ := cmd.Flags().GetString("dest")
			destCreds, _ := cmd.Flags().GetString("dest-creds")

			gte, _ := cmd.Flags().GetString("gte")
			lte, _ := cmd.Flags().GetString("lte")
			batch, _ := cmd.Flags().GetInt32("batch")
			timeout, _ := cmd.Flags().GetFloat32("timeout")
			background, _ := cmd.Flags().GetBool("background")

			if gte != "" {
				_, err := time.Parse(time.DateOnly, gte)
				if err != nil {
					return fmt.Errorf("invalid gte format: %s", err.Error())
				}
			}
			if lte != "" {
				_, err := time.Parse(time.DateOnly, lte)
				if err != nil {
					return fmt.Errorf("invalid lte format: %s", err.Error())
				}

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
			creds := strings.Split(sourceCreds, ":")
			username := ""
			passwd := ""
			if len(creds) == 2 {
				username = creds[0]
				passwd = creds[1]
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			md, err := md.GetMongoInstance(ctx, source, username, passwd, "")
			if err != nil {
				return err
			}
			destUsername := ""
			destPasswd := ""
			if len(strings.Split(destCreds, ":")) == 2 {
				destUsername = strings.Split(destCreds, ":")[0]
				destPasswd = strings.Split(destCreds, ":")[1]
			}
			destEs, err := es.GetEsInstanceWithCreds(dest, destUsername, destPasswd)
			if err != nil {
				return err
			}
			collection := md.Database(db).Collection(coll)
			allowDisk := true
			opts := options.FindOptions{
				AllowDiskUse: &allowDisk,
				BatchSize:    &batch,
				Sort:         bson.M{"createdAt": -1},
			}
			cur, err := collection.Find(ctx, bson.D{}, &opts)
			fetched := []bson.M{}
			for cur.Next(ctx) {
				var result bson.M
				err := cur.Decode(&result)
				if err != nil {
					log.Fatal(err)
				}
				fetched = append(fetched, result)
			}
			wg := sync.WaitGroup{}
			for i := 0; i < len(fetched); i += int(batch) {
				end := min(i+int(batch), len(fetched))
				chunk := fetched[i:end]
				wg.Go(func() {
					var buf bytes.Buffer
					for _, item := range chunk {
						actionLine := map[string]any{
							"index": map[string]any{"_index": index},
						}
						actionData, err := json.Marshal(actionLine)
						if err != nil {
							log.Fatal(fmt.Sprintf("Failed to marshal action for item: %v", err))
							continue
						}
						buf.Write(actionData)
						buf.WriteByte('\n')
						docData, err := json.Marshal(item)
						if err != nil {
							log.Fatal(fmt.Sprintf("Failed to marshal doc for item: %v", err))
							continue
						}

						if len(docData) <= 2 {
							log.Fatal(fmt.Sprintf("Empty document for item: %+v", item))
							continue
						}
						buf.Write(docData)
						buf.WriteByte('\n')
					}
					resp, err := destEs.Bulk(
						&buf,
						destEs.Bulk.WithContext(ctx),
						destEs.Bulk.WithIndex(index),
					)
					if err != nil {
						log.Fatal(fmt.Errorf("bulk request failed: %w", err))
					}
					defer resp.Body.Close()

					if resp.StatusCode >= 400 {
						var rs any
						_ = json.NewDecoder(resp.Body).Decode(&rs)
						log.Fatal(fmt.Errorf("bulk request error (status=%d): %v", resp.StatusCode, rs))
					}

					var bulkResp struct {
						Errors bool `json:"errors"`
						Items  []map[string]struct {
							Status int            `json:"status"`
							Error  map[string]any `json:"error"`
						} `json:"items"`
					}
					if err := json.NewDecoder(resp.Body).Decode(&bulkResp); err != nil {
						log.Fatal(fmt.Errorf("failed to decode bulk response: %w", err))
					}

					if bulkResp.Errors {
						for _, item := range bulkResp.Items {
							for _, op := range item {
								if op.Status >= 400 {
									log.Fatal(fmt.Sprintf("item failed: status=%d, error=%v", op.Status, op.Error))
								}
							}
						}
						return
					}
				})
			}

			wg.Wait()
			return nil
		},
	}
	rangeCommand.Flags().String("source", "", "source mongo host")
	rangeCommand.MarkFlagRequired("source")
	rangeCommand.Flags().String("source-creds", "", "source-creds mongo credentials")

	rangeCommand.Flags().String("dest", "", "dest elastic host")
	rangeCommand.MarkFlagRequired("dest")

	rangeCommand.Flags().String("dest-creds", "", "dest-creds elastic credentials")

	rangeCommand.Flags().String("index", "", "index to copy into")
	rangeCommand.MarkFlagRequired("index")

	rangeCommand.Flags().String("collection", "", "collection to copy from")
	rangeCommand.MarkFlagRequired("collection")

	rangeCommand.Flags().String("database", "", "database to copy from")
	rangeCommand.MarkFlagRequired("database")

	rangeCommand.Flags().String("gte", "", "define start of copy span")

	rangeCommand.Flags().String("lte", "", "define end of copy span")

	rangeCommand.Flags().Int32("batch", 10000, "batch size for each iteration")
	rangeCommand.Flags().Float32("timeout", 0.5, "timeout seconds in each iteration")
	rangeCommand.Flags().Bool("background", false, "Run the copy operation in background")

	rootCmd.AddCommand(rangeCommand)
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
