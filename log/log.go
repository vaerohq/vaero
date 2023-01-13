/*
Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
*/
package log

import (
	"log"

	"go.uber.org/zap"
)

var Logger *zap.Logger
var LogLevel zap.AtomicLevel

// InitLogger initializes a zap logger and should be called on program start
func InitLogger() {
	var err error
	logConfig := zap.NewDevelopmentConfig()
	//logConfig.Level.SetLevel(zap.ErrorLevel)
	LogLevel = logConfig.Level

	Logger, err = logConfig.Build() // development logger
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
}

// SyncLogger syncs the logger by flushing any buffered logs
func SyncLogger() {
	Logger.Sync() // Sync on close in case some logs are buffered
	Logger.Info("Synced logger")
}
