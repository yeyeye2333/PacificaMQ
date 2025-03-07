package main

import (
	"context"
	"time"

	"github.com/yeyeye2333/PacificaMQ/logger"

	"github.com/segmentio/kafka-go"
)

func main() {
	// to produce messages
	topic := "my-topic"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:29092", topic, partition)
	if err != nil {
		logger.Fatal("failed to dial leader:", err)
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
	)
	if err != nil {
		logger.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		logger.Fatal("failed to close writer:", err)
	}
}
