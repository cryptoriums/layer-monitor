// Package signerclient is a thin gRPC client for the bridge remote signer.
//
// The monitor uses it at startup to discover the reporter's wallet address
// (and, optionally, the chain ID) directly from the signer, so the address no
// longer has to be hand-maintained in the monitor's own config. It only calls
// read-only endpoints (GetAddress, GetChainID), so it connects insecurely — no
// mTLS required. If the signer is unreachable the caller is expected to fall
// back to the value already stored in the database — see cmd/main.go.
package signerclient

import (
	"context"
	"fmt"
	"time"

	signerv1 "github.com/tellor-io/bridge-remote-signer/api/gen/signer/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// addressPrefix is the bech32 human-readable part for Layer accounts.
const addressPrefix = "tellor"

// Config holds the connection details for the remote signer.
type Config struct {
	// Addr is the signer's gRPC address (host:port).
	Addr string
}

// Enabled reports whether a signer address is configured. When false the
// monitor should skip the signer query entirely and rely on the DB.
func (c Config) Enabled() bool { return c.Addr != "" }

// Client wraps a gRPC connection to the remote signer.
type Client struct {
	conn   *grpc.ClientConn
	signer signerv1.BridgeSignerClient
}

// Dial connects to the remote signer over an insecure connection. The monitor
// only calls read-only endpoints (GetAddress, GetChainID), which need no mTLS.
// The returned Client must be Closed.
func Dial(cfg Config) (*Client, error) {
	conn, err := grpc.NewClient(cfg.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial remote signer at %s: %w", cfg.Addr, err)
	}

	return &Client{conn: conn, signer: signerv1.NewBridgeSignerClient(conn)}, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// FetchAddress returns the reporter's bech32 wallet address from the signer.
func (c *Client) FetchAddress(ctx context.Context) (string, error) {
	resp, err := c.signer.GetAddress(ctx, &signerv1.GetAddressRequest{Prefix: addressPrefix})
	if err != nil {
		return "", fmt.Errorf("GetAddress from remote signer: %w", err)
	}
	if resp.Address == "" {
		return "", fmt.Errorf("remote signer returned an empty address")
	}
	return resp.Address, nil
}

// FetchChainID returns the chain ID the signer is configured for. Requires a
// signer built with the GetChainID RPC; older signers return an error.
func (c *Client) FetchChainID(ctx context.Context) (string, error) {
	resp, err := c.signer.GetChainID(ctx, &signerv1.GetChainIDRequest{})
	if err != nil {
		return "", fmt.Errorf("GetChainID from remote signer: %w", err)
	}
	return resp.ChainId, nil
}

// FetchAddressWithTimeout dials the signer, fetches the wallet address, and
// closes the connection — a convenience for one-shot startup discovery. The
// per-call timeout keeps a down signer from blocking monitor startup.
func FetchAddressWithTimeout(ctx context.Context, cfg Config, timeout time.Duration) (string, error) {
	client, err := Dial(cfg)
	if err != nil {
		return "", err
	}
	defer func() { _ = client.Close() }()

	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return client.FetchAddress(callCtx)
}
