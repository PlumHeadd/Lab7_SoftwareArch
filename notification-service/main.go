package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

type PaymentEvent struct {
	EventID string  `json:"eventId"`
	OrderID string  `json:"orderId"`
	UserID  string  `json:"userId"`
	Amount  float64 `json:"amount"`
	Type    string  `json:"type"`
	Message string  `json:"message"`
}

type NotificationHandler struct{}

func (h *NotificationHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *NotificationHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *NotificationHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var event PaymentEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("failed to parse event: %v", err)
			session.MarkMessage(msg, "")
			continue
		}

		// Handle payment events
		switch event.Type {
		case "PaymentCompleted":
			log.Printf("Notification: Payment Success for Order %s (User: %s, Amount: %.2f)",
				event.OrderID, event.UserID, event.Amount)
		case "PaymentFailed":
			log.Printf("Notification: Payment Failed for Order %s (User: %s, Amount: %.2f)",
				event.OrderID, event.UserID, event.Amount)
		default:
			log.Printf("Unknown event type: %s", event.Type)
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup([]string{broker}, "notification-service-group", config)
	if err != nil {
		log.Fatalf("failed to create notification consumer group: %v", err)
	}
	defer group.Close()

	handler := &NotificationHandler{}
	log.Println("Notification Service started")

	ctx := context.Background()

	for {
		if err := group.Consume(ctx, []string{"order-events"}, handler); err != nil {
			log.Printf("notification consumer error: %v", err)
			time.Sleep(2 * time.Second)
		}
	}
}
