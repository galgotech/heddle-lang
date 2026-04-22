package main

import (
	"fmt"
)

func main() {
	port := 50051
	fmt.Printf("🚀 Heddle Control Plane starting on port %d...\n", port)
	fmt.Println("📡 Arrow Flight server initializing...")
	StartServer(port)
}
