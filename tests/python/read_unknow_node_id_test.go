//go:build integration

package uatest

import (
	"context"
	"testing"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
	"github.com/stretchr/testify/require"
)

// TestRead performs an integration test to read values
// from an OPC/UA server.
func TestReadUnknowNodeID(t *testing.T) {
	ctx := context.Background()

	srv := NewPythonServer("read_unknow_node_id_server.py")
	defer srv.Close()

	c, err := opcua.NewClient(srv.Endpoint, srv.Opts...)
	require.NoError(t, err, "NewClient failed")

	err = c.Connect(ctx)
	require.NoError(t, err, "Connect failed")
	defer c.Close(ctx)

	// read node with unknown extension object
	// This should be OK
	nodeWithUnknownType := ua.NewStringNodeID(2, "IntValZero")
	resp, err := c.Read(ctx, &ua.ReadRequest{
		NodesToRead: []*ua.ReadValueID{
			{NodeID: nodeWithUnknownType},
		},
	})
	require.NoError(t, err, "Read failed")

	require.Equal(t, ua.StatusBadDataTypeIDUnknown, resp.Results[0].Status, "got different status for a node with an unknown type")
	extensionObject := resp.Results[0].Value.ExtensionObject()
	require.NotNil(t, extensionObject)
	require.Equal(t, ua.NewStringNodeID(2, "IntValType"), extensionObject.TypeID.NodeID)
	require.Equal(t, ua.ExtensionObjectBinary, int(extensionObject.EncodingMask))
	require.Equal(t, make([]byte, 8), extensionObject.Value, "unknown extension object should retain its raw Int64 body")

	// check that the connection is still usable by reading another node.
	_, err = c.Read(ctx, &ua.ReadRequest{
		NodesToRead: []*ua.ReadValueID{
			{
				NodeID: ua.NewNumericNodeID(0, id.Server_ServerStatus_State),
			},
		},
	})
	require.NoError(t, err, "Read failed")
}
