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

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete [pipeline ID]",
	Short: "Delete the specified pipeline from Vaero",
	Long:  `Delete the specified pipeline from Vaero. Find pipeline IDs using the list command.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			log.Logger.Fatal("Requires 1 argument")
		}

		id, err := strconv.Atoi(args[0])

		if err != nil {
			log.Logger.Fatal("Argument must be an integer", zap.String("Error", err.Error()))
		}

		c.DeleteHandler(id)
	},
}

func init() {
	rootCmd.AddCommand(deleteCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// deleteCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// deleteCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
