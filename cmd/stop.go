/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"strconv"

	"github.com/spf13/cobra"
	"github.com/vaerohq/vaero/log"
	"go.uber.org/zap"
)

// stopCmd represents the stop command
var stopCmd = &cobra.Command{
	Use:   "stop [pipeline ID]",
	Short: "Stop the specified pipeline",
	Long:  `Stops the specified pipeline. Find pipeline IDs using the list command.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			log.Logger.Fatal("Requires 1 argument")
		}

		id, err := strconv.Atoi(args[0])

		if err != nil {
			log.Logger.Fatal("Argument must be an integer", zap.String("Error", err.Error()))
		}

		c.StopHandler(id)
	},
}

func init() {
	rootCmd.AddCommand(stopCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// stopCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// stopCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
