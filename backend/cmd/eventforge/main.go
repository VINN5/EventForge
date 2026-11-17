package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"eventforge/src/core"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := core.NewPool(ctx)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer pool.Close()

	fmt.Println("EventForge connected to PostgreSQL")
	<-ctx.Done()
	fmt.Println("shutting down")
}
