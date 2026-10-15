//go:build integration
// +build integration

package uatest

import (
	"bytes"
	"context"
	"testing"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

// TestRead performs an integration test to read values
// from an OPC/UA server.
func TestReadUnknowNodeID(t *testing.T) {
	ctx := context.Background()

	srv := NewServer("read_unknow_node_id_server.py")
	defer srv.Close()

	c, err := opcua.NewClient(srv.Endpoint, srv.Opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	// read node with unknown extension object
	// This should be OK
	nodeWithUnknownType := ua.NewStringNodeID(2, "IntValZero")
	resp, err := c.Read(ctx, &ua.ReadRequest{
		NodesToRead: []*ua.ReadValueID{
			{NodeID: nodeWithUnknownType},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if got, want := resp.Results[0].Status, ua.StatusOK; got != want {
		t.Errorf("got status %v want %v for a node with an unknown type", got, want)
	}
	extension, ok := resp.Results[0].Value.Value().(*ua.ExtensionObject)
	if !ok {
		t.Fatalf("unknown type was not preserved as an ExtensionObject: %T", resp.Results[0].Value.Value())
	}
	body, ok := extension.Value.([]byte)
	if !ok || !bytes.Equal(body, make([]byte, 8)) {
		t.Fatalf("unknown binary body was not preserved: %v", extension.Value)
	}

	// check that the connection is still usable by reading another node.
	_, err = c.Read(ctx, &ua.ReadRequest{
		NodesToRead: []*ua.ReadValueID{
			{
				NodeID: ua.NewNumericNodeID(0, id.Server_ServerStatus_State),
			},
		},
	})
	if err != nil {
		t.Error(err)
	}
}
