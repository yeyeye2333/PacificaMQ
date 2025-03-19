package main

import (
	"context"
	"fmt"
	"time"

	"github.com/yeyeye2333/PacificaMQ/internal/logger"

	"github.com/segmentio/kafka-go"
)

func main() {
	// to consume messages
	topic := "my-topic"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:29092", topic, partition)
	if err != nil {
		logger.Fatal("failed to dial leader:", err)
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	b := make([]byte, 10e3) // 10KB max per message
	for {
		n, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b[:n]))
	}

	if err := batch.Close(); err != nil {
		logger.Fatal("failed to close batch:", err)
	}

	if err := conn.Close(); err != nil {
		logger.Fatal("failed to close connection:", err)
	}
}
