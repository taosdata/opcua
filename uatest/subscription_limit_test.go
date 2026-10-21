//go:build integration
// +build integration

package uatest

import (
	"context"
	"testing"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/errors"
	"github.com/gopcua/opcua/ua"
)

func TestSubscriptionLimitKeepsConnection(t *testing.T) {
	server := NewServer("subscription_limit_server.py")
	defer server.Close()

	options := append(server.Opts, opcua.AutoReconnect(false))
	client, err := opcua.NewClient(server.Endpoint, options...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer client.Close(ctx)

	_, err = client.Subscribe(ctx, nil, make(chan *opcua.PublishNotificationData, 1))
	if err != nil {
		t.Fatal(err)
	}
	secureChannel := client.SecureChannel()

	_, err = client.Subscribe(ctx, nil, make(chan *opcua.PublishNotificationData, 1))
	if !errors.Is(err, ua.StatusBadTooManySubscriptions) {
		t.Fatalf("second Subscribe() error = %v, want StatusBadTooManySubscriptions", err)
	}

	response, err := client.Read(ctx, &ua.ReadRequest{
		NodesToRead:        []*ua.ReadValueID{{NodeID: ua.NewNumericNodeID(0, 2258), AttributeID: ua.AttributeIDValue}},
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	})
	if err != nil {
		t.Fatalf("Read() after rejected subscription: %v", err)
	}
	if len(response.Results) != 1 || response.Results[0].Status != ua.StatusOK {
		t.Fatalf("Read() after rejected subscription returned %v", response.Results)
	}

	checkConnection := func() {
		if client.State() != opcua.Connected || client.SecureChannel() != secureChannel {
			t.Fatalf("connection changed after rejected subscription: state=%s", client.State())
		}
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-ticker.C:
			checkConnection()
		case <-deadline.C:
			checkConnection()
			if err := client.CloseSession(ctx); err != nil {
				t.Fatalf("CloseSession() after rejected subscription: %v", err)
			}
			return
		}
	}
}
