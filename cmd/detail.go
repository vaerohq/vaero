/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"log"
	"strconv"

	"github.com/spf13/cobra"
)

// detailCmd represents the detail command
var detailCmd = &cobra.Command{
	Use:   "detail",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("detail called")

		if len(args) < 1 {
			log.Fatal()
		}

		id, err := strconv.Atoi(args[0])

		if err != nil {
			log.Fatal()
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
