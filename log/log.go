package log

import (
	"log"

	"go.uber.org/zap"
)

var Logger *zap.Logger

// InitLogger initializes a zap logger and should be called on program start
func InitLogger() {
	var err error
	Logger, err = zap.NewDevelopment() // development logger
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	Logger.Info("Initialized logger")
}

// SyncLogger syncs the logger by flushing any buffered logs
func SyncLogger() {
	Logger.Sync() // Sync on close in case some logs are buffered
	Logger.Info("Synced logger")
}
