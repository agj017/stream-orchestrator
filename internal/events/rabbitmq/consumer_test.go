package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeAcknowledger struct {
	acked int
	nacked int
}

func (f *fakeAcknowledger) Ack(_ uint64, _ bool) error {
	f.acked++
	return nil
}

func (f *fakeAcknowledger) Nack(_ uint64, _ bool, _ bool) error {
	f.nacked++
	return nil
}

func (f *fakeAcknowledger) Reject(_ uint64, _ bool) error {
	return nil
}

func TestConsumerConsumeDeliveries_AckOnSuccess(t *testing.T) {
	c := &Consumer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dch := make(chan amqp.Delivery, 1)
	ack := &fakeAcknowledger{}
	dch <- amqp.Delivery{
		Acknowledger: ack,
		DeliveryTag:  1,
		Type:         "STREAM_CREATED",
		Body:         []byte(`{"a":"b"}`),
	}
	close(dch)

	err := c.consumeDeliveries(ctx, dch, func(_ context.Context, _ Delivery) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected channel closed error, got nil")
	}
	if ack.acked != 1 {
		t.Fatalf("expected ack=1, got %d", ack.acked)
	}
	if ack.nacked != 0 {
		t.Fatalf("expected nack=0, got %d", ack.nacked)
	}
}

func TestConsumerConsumeDeliveries_NackOnFailure(t *testing.T) {
	c := &Consumer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dch := make(chan amqp.Delivery, 1)
	ack := &fakeAcknowledger{}
	dch <- amqp.Delivery{
		Acknowledger: ack,
		DeliveryTag:  2,
		Type:         "STREAM_CREATED",
		Body:         []byte(`{"a":"b"}`),
	}
	close(dch)

	err := c.consumeDeliveries(ctx, dch, func(_ context.Context, _ Delivery) error {
		return errors.New("failed")
	})
	if err == nil {
		t.Fatal("expected channel closed error, got nil")
	}
	if ack.nacked != 1 {
		t.Fatalf("expected nack=1, got %d", ack.nacked)
	}
	if ack.acked != 0 {
		t.Fatalf("expected ack=0, got %d", ack.acked)
	}
}

func TestConsumerConsumeDeliveries_ContextCancel(t *testing.T) {
	c := &Consumer{}
	ctx, cancel := context.WithCancel(context.Background())
	dch := make(chan amqp.Delivery)

	done := make(chan error, 1)
	go func() {
		done <- c.consumeDeliveries(ctx, dch, func(_ context.Context, _ Delivery) error { return nil })
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for consumeDeliveries to stop")
	}
}

