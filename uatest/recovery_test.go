//go:build integration
// +build integration

package uatest

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

type recoveryServer struct {
	commands io.WriteCloser
	output   *bufio.Scanner
}

func (server *recoveryServer) expect(t *testing.T, expected string) {
	t.Helper()
	if !server.output.Scan() || server.output.Text() != expected {
		t.Fatalf("server: want %q, got %q, error %v", expected, server.output.Text(), server.output.Err())
	}
}

func (server *recoveryServer) command(t *testing.T, command, expected string) {
	t.Helper()
	if _, err := fmt.Fprintln(server.commands, command); err != nil {
		t.Fatal(err)
	}
	server.expect(t, expected)
}

func startRecoveryClient(t *testing.T) (context.Context, *opcua.Client, *recoveryServer) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := fmt.Sprintf("opc.tcp://%s/recovery/", listener.Addr())
	listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	python := os.Getenv("OPCUA_TEST_PYTHON")
	if python == "" {
		python = "python3"
	}
	process := exec.CommandContext(ctx, python, "recovery_server.py", endpoint)
	serverErrors, err := os.Create(filepath.Join(t.TempDir(), "server.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { serverErrors.Close() })
	process.Stderr = serverErrors
	commands, err := process.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := process.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		fmt.Fprintln(commands, "quit")
		process.Wait()
	})
	server := &recoveryServer{commands: commands, output: bufio.NewScanner(output)}
	server.expect(t, "READY")
	client, err := opcua.NewClient(endpoint,
		opcua.SecurityMode(ua.MessageSecurityModeNone),
		opcua.SecurityPolicy(ua.SecurityPolicyURINone),
		opcua.AuthAnonymous(),
		opcua.AutoReconnect(true),
		opcua.ReconnectInterval(200*time.Millisecond),
		opcua.RequestTimeout(time.Second),
		opcua.SessionTimeout(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(context.Background()) })
	return ctx, client, server
}

func recoveryMonitor(t *testing.T, ctx context.Context, subscription *opcua.Subscription, nodeID, handle uint32) uint32 {
	t.Helper()
	response, err := subscription.Monitor(ctx, ua.TimestampsToReturnBoth,
		opcua.NewMonitoredItemCreateRequestWithDefaults(ua.NewNumericNodeID(2, nodeID), ua.AttributeIDValue, handle))
	if err != nil {
		t.Fatal(err)
	}
	if response.Results[0].StatusCode != ua.StatusOK {
		t.Fatal(response.Results[0].StatusCode)
	}
	return response.Results[0].MonitoredItemID
}

func recoveryValue(t *testing.T, ctx context.Context, notifications <-chan *opcua.PublishNotificationData, handle uint32, after time.Time) {
	t.Helper()
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-timer.C:
			t.Fatalf("no fresh data for handle %d after fault", handle)
		case notification := <-notifications:
			if notification.Error != nil {
				t.Logf("notification: %v", notification.Error)
				continue
			}
			data, ok := notification.Value.(*ua.DataChangeNotification)
			if !ok {
				continue
			}
			for _, item := range data.MonitoredItems {
				if item.ClientHandle == handle && item.Value.Status == ua.StatusOK && item.Value.SourceTimestamp.After(after) {
					return
				}
			}
		}
	}
}

func TestSubscriptionRecovery(t *testing.T) {
	for _, fault := range []string{"restart", "cut", "cut_reject_activate", "restart_missing", "restart_reject_subscription", "restart_drop_subscription_response", "restart_drop_subscription_disconnect", "restart_reject_items", "restart_reject_item"} {
		t.Run(fault, func(t *testing.T) {
			ctx, client, server := startRecoveryClient(t)
			notifications := make(chan *opcua.PublishNotificationData, 128)
			subscription, err := client.Subscribe(ctx, &opcua.SubscriptionParameters{Interval: 100 * time.Millisecond}, notifications)
			if err != nil {
				t.Fatal(err)
			}
			recoveryMonitor(t, ctx, subscription, 1001, 1)
			recoveryMonitor(t, ctx, subscription, 1002, 2)
			recoveryValue(t, ctx, notifications, 1, time.Time{})
			session := client.Session()
			for round := 0; round < 2; round++ {
				acknowledgement := "RESTARTED"
				if fault == "cut" || fault == "cut_reject_activate" {
					acknowledgement = "CUT"
				}
				server.command(t, fault, acknowledgement)
				after := time.Now()
				recoveryValue(t, ctx, notifications, 1, after)
				if fault != "restart_missing" {
					recoveryValue(t, ctx, notifications, 2, after)
				}
				if fault == "cut" && client.Session() != session {
					t.Fatal("transport reconnect replaced the valid session")
				}
				server.command(t, "stats", "SESSIONS=1")
				server.command(t, "subscriptions", "SUBSCRIPTIONS=1")
			}
		})
	}
}

func TestSubscriptionRecoveryConcurrentMonitor(t *testing.T) {
	ctx, client, server := startRecoveryClient(t)
	notifications := make(chan *opcua.PublishNotificationData, 128)
	subscription, err := client.Subscribe(ctx, &opcua.SubscriptionParameters{Interval: 100 * time.Millisecond}, notifications)
	if err != nil {
		t.Fatal(err)
	}
	recoveryMonitor(t, ctx, subscription, 1001, 1)
	recoveryValue(t, ctx, notifications, 1, time.Time{})
	server.command(t, "restart_hold_items", "RESTARTED")
	server.expect(t, "RESTORING")
	result := make(chan error, 1)
	go func() {
		response, err := subscription.Monitor(ctx, ua.TimestampsToReturnBoth,
			opcua.NewMonitoredItemCreateRequestWithDefaults(ua.NewNumericNodeID(2, 1003), ua.AttributeIDValue, 3))
		if err == nil {
			err = response.Results[0].StatusCode
			if err == ua.StatusOK {
				err = nil
			}
		}
		result <- err
	}()
	server.command(t, "release", "RELEASED")
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	recoveryValue(t, ctx, notifications, 3, time.Now())
	server.command(t, "restart", "RESTARTED")
	recoveryValue(t, ctx, notifications, 3, time.Now())
	server.command(t, "stats", "SESSIONS=1")
}

func TestSubscriptionRecoveryCancel(t *testing.T) {
	ctx, client, server := startRecoveryClient(t)
	notifications := make(chan *opcua.PublishNotificationData, 128)
	subscription, err := client.Subscribe(ctx, &opcua.SubscriptionParameters{Interval: 100 * time.Millisecond}, notifications)
	if err != nil {
		t.Fatal(err)
	}
	recoveryMonitor(t, ctx, subscription, 1001, 1)
	recoveryValue(t, ctx, notifications, 1, time.Time{})
	server.command(t, "restart_hold_items", "RESTARTED")
	server.expect(t, "RESTORING")
	result := make(chan error, 1)
	go func() { result <- subscription.Cancel(ctx) }()
	server.command(t, "release", "RELEASED")
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if ids := client.SubscriptionIDs(); len(ids) != 0 {
		t.Fatalf("cancelled subscription remains registered: %v", ids)
	}
	if _, err := subscription.Monitor(ctx, ua.TimestampsToReturnBoth,
		opcua.NewMonitoredItemCreateRequestWithDefaults(ua.NewNumericNodeID(2, 1003), ua.AttributeIDValue, 3)); err != ua.StatusBadSubscriptionIDInvalid {
		t.Fatalf("Monitor on cancelled subscription: %v", err)
	}
}

func TestSubscriptionRecoveryPartialFailure(t *testing.T) {
	ctx, client, server := startRecoveryClient(t)
	notifications := make(chan *opcua.PublishNotificationData, 128)
	good, err := client.Subscribe(ctx, &opcua.SubscriptionParameters{Interval: 100 * time.Millisecond}, notifications)
	if err != nil {
		t.Fatal(err)
	}
	bad, err := client.Subscribe(ctx, &opcua.SubscriptionParameters{Interval: 100 * time.Millisecond}, notifications)
	if err != nil {
		t.Fatal(err)
	}
	recoveryMonitor(t, ctx, good, 1001, 1)
	recoveryMonitor(t, ctx, bad, 1002, 2)
	recoveryValue(t, ctx, notifications, 1, time.Time{})
	server.command(t, "restart_reject_items_always", "RESTARTED")
	recoveryValue(t, ctx, notifications, 1, time.Now())
	server.command(t, "cut", "CUT")
	recoveryValue(t, ctx, notifications, 1, time.Now())
	if err := bad.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	server.command(t, "cut", "CUT")
	recoveryValue(t, ctx, notifications, 1, time.Now())
	server.command(t, "stats", "SESSIONS=1")
}

func TestSubscriptionRecoveryBlockedNotifications(t *testing.T) {
	ctx, client, server := startRecoveryClient(t)
	notifications := make(chan *opcua.PublishNotificationData, 1)
	subscription, err := client.Subscribe(ctx, &opcua.SubscriptionParameters{Interval: 100 * time.Millisecond}, notifications)
	if err != nil {
		t.Fatal(err)
	}
	recoveryMonitor(t, ctx, subscription, 1001, 1)
	recoveryValue(t, ctx, notifications, 1, time.Time{})
	server.command(t, "cut_republish", "CUT")
	time.Sleep(1500 * time.Millisecond)
	cancelled := make(chan error, 1)
	go func() { cancelled <- subscription.Cancel(ctx) }()
	select {
	case err := <-cancelled:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocked notifications prevented cancellation")
	}
	server.command(t, "cut", "CUT")
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		_, err := client.Read(ctx, &ua.ReadRequest{NodesToRead: []*ua.ReadValueID{{NodeID: ua.NewNumericNodeID(0, 2259), AttributeID: ua.AttributeIDValue}}})
		if err == nil && client.State() == opcua.Connected {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("blocked notifications prevented connection recovery")
		case <-time.After(50 * time.Millisecond):
		}
	}
	server.command(t, "stats", "SESSIONS=1")
}

func TestSubscriptionRecoveryInFlightPublish(t *testing.T) {
	for _, fault := range []string{"hold_publish", "hold_publish_error"} {
		t.Run(fault, func(t *testing.T) {
			ctx, client, server := startRecoveryClient(t)
			notifications := make(chan *opcua.PublishNotificationData, 128)
			original, err := client.Subscribe(ctx, &opcua.SubscriptionParameters{Interval: 100 * time.Millisecond}, notifications)
			if err != nil {
				t.Fatal(err)
			}
			recoveryMonitor(t, ctx, original, 1001, 1)
			recoveryValue(t, ctx, notifications, 1, time.Time{})
			server.command(t, fault, "ARMED")
			server.expect(t, "PUBLISH_HELD")
			if err := original.Cancel(ctx); err != nil {
				t.Fatal(err)
			}
			replacement, err := client.Subscribe(ctx, &opcua.SubscriptionParameters{Interval: 100 * time.Millisecond}, notifications)
			if err != nil {
				t.Fatal(err)
			}
			recoveryMonitor(t, ctx, replacement, 1001, 2)
			server.command(t, "release", "RELEASED")
			recoveryValue(t, ctx, notifications, 2, time.Now())
			recoveryValue(t, ctx, notifications, 2, time.Now())
			server.command(t, "stats", "SESSIONS=1")
			server.command(t, "subscriptions", "SUBSCRIPTIONS=1")
		})
	}
}
