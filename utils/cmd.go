package utils

import (
	"cpd/utils/es"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/spf13/cobra"
)

func RunInBackground(cmd *cobra.Command, cmdArgs []string) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}

	logFile := fmt.Sprintf("es_copy_%s.log", time.Now().Format("20060102_150405"))
	log, err := os.Create(logFile)
	if err != nil {
		return fmt.Errorf("failed to create log file: %v", err)
	}
	defer log.Close()

	procAttr := &os.ProcAttr{
		Files: []*os.File{nil, log, log},
	}

	process, err := os.StartProcess(executable, cmdArgs, procAttr)
	if err != nil {
		return fmt.Errorf("failed to start background process: %v", err)
	}

	fmt.Printf("PID: %d, Log: %s\n", process.Pid, logFile)
	process.Release()
	return nil
}

func GetEsSourceDest(cmd *cobra.Command, cmdArgs []string) (*elasticsearch.Client, *elasticsearch.Client, error) {
	source, _ := cmd.Flags().GetString("source")
	sourceCreds, _ := cmd.Flags().GetString("source-creds")
	if len(strings.Split(sourceCreds, ":")) != 2 {
		return nil, nil, fmt.Errorf("invalid source creds")
	}

	dest, _ := cmd.Flags().GetString("dest")
	destCreds, _ := cmd.Flags().GetString("dest-creds")
	if len(strings.Split(destCreds, ":")) != 2 {
		return nil, nil, fmt.Errorf("invalid dest creds")
	}
	sourceEs, err := es.GetEsInstanceWithCreds(source, strings.Split(sourceCreds, ":")[0], strings.Split(sourceCreds, ":")[1])
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create source instance: %s", err.Error())
	}
	destEs, err := es.GetEsInstanceWithCreds(dest, strings.Split(destCreds, ":")[0], strings.Split(destCreds, ":")[1])
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create dest instance: %s", err.Error())
	}
	return sourceEs, destEs, nil
}
