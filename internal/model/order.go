// Package model holds the one message type these programs exchange.
//
// The payload is plain JSON on purpose. Confluent's Schema Registry with Avro or
// Protobuf is the right answer in production, but it would put a serialisation
// lesson in the middle of a configuration lesson. Everything here is about the
// producer and consumer knobs, so the value stays readable in
// kafka-console-consumer.
package model

import (
	"encoding/json"
	"fmt"
	"time"
)

// Order is one event on the topic.
type Order struct {
	OrderID     string    `json:"order_id"`
	CustomerID  string    `json:"customer_id"`
	SKU         string    `json:"sku"`
	Quantity    int       `json:"quantity"`
	AmountCents int64     `json:"amount_cents"`
	CreatedAt   time.Time `json:"created_at"`
	Sequence    int       `json:"sequence"`
	// Run identifies the producer PROCESS that emitted this message. Kafka orders
	// messages within a partition, but every producer numbers its own messages
	// from 1, so two producer runs writing to one partition interleave two
	// independent sequences. Checking monotonicity per (partition, run) tests the
	// ordering guarantee; checking it per partition alone just detects that the
	// producer was restarted.
	Run string `json:"run"`
}

// Key is the message key, and the key is the whole ordering story: Kafka
// guarantees order within a partition, and the default partitioner sends equal
// keys to the same partition. Keying by customer therefore means one customer's
// events are strictly ordered, while different customers spread across the
// cluster for parallelism. Producing with a nil key round-robins instead and
// gives you no ordering at all.
func (o Order) Key() []byte { return []byte(o.CustomerID) }

// Encode serialises the order for the message value.
func (o Order) Encode() ([]byte, error) { return json.Marshal(o) }

// Decode parses a message value back into an Order.
func Decode(b []byte) (Order, error) {
	var o Order
	if err := json.Unmarshal(b, &o); err != nil {
		return Order{}, fmt.Errorf("model: decode: %w", err)
	}
	return o, nil
}

// Short renders an order compactly enough to read as it scrolls past.
func (o Order) Short() string {
	return fmt.Sprintf("%s seq=%d cust=%s sku=%s qty=%d", o.OrderID, o.Sequence, o.CustomerID, o.SKU, o.Quantity)
}
