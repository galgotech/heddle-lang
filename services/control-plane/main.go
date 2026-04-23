package main

import (
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
)

func main() {
	// Initialize logger
	if err := logger.Init(logger.Config{Development: true}); err != nil {
		panic(err)
	}
	defer logger.Sync()

	port := 50051
	logger.L().Info("Heddle Control Plane starting", zap.Int("port", port))
	logger.L().Info("Arrow Flight server initializing")

	StartServer(port)
}
