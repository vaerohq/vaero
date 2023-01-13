/*
Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
*/
package cmd

import (
	"strconv"

	"github.com/spf13/cobra"
	"github.com/vaerohq/vaero/log"
	"go.uber.org/zap"
)

// detailCmd represents the detail command
var detailCmd = &cobra.Command{
	Use:   "detail [pipeline ID]",
	Short: "Display details of the specified pipeline",
	Long:  `Display details for the specified pipeline. Find pipeline IDs using the list command.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			log.Logger.Fatal("Requires 1 argument")
		}

		id, err := strconv.Atoi(args[0])

		if err != nil {
			log.Logger.Fatal("Argument must be an integer", zap.String("Error", err.Error()))
		}

		c.DetailHandler(id)
	},
}

func init() {
	rootCmd.AddCommand(detailCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// detailCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// detailCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
