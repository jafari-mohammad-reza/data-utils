package utils

import (
	"fmt"
	"os"
	"time"

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
