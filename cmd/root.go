/*
Copyright © 2022 Vaero hello@vaero.co
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/vaerohq/vaero/log"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "vaero",
	Short: "Vaero collects, transforms, and routes your log data",
	Long: `Vaero is a modern log shipper that lets you specify your log pipelines in Python. Pipelines are executed in Go. This application is a command line interface for Vaero.
To get started, add a pipeline file and then start:
vaero add pipelines/pipe.py
vaero start
`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		log.InitLogger()
		c.InitTables()
		CheckPython()
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		log.SyncLogger()
	},
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.vaero.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	//rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
