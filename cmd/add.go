/*
Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
*/
package cmd

import (
	"github.com/spf13/cobra"
	"github.com/vaerohq/vaero/log"
)

// addCmd represents the add command
var addCmd = &cobra.Command{
	Use:   "add [file path]",
	Short: "Add a pipeline to Vaero based on the specified pipeline file",
	Long:  `Add a pipeline to Vaero by reading the specified pipeline file. The pipeline is staged until the start command is received.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			log.Logger.Fatal("Requires 1 argument")
		}
		c.AddHandler(args[0])
	},
}

func init() {
	rootCmd.AddCommand(addCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// addCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	//addCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	//var source string
	//addCmd.Flags().StringVarP(&source, "source", "s", "", "Source directory to read from")
}
