package web

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/cryptoriums/layer-monitor/addr"
)

func TestToValcons(t *testing.T) {
	tests := []struct {
		name         string
		pubkeyBase64 string
		wantPrefix   string
		wantErr      bool
	}{
		{
			name:         "valid ed25519 pubkey",
			pubkeyBase64: "aMNLGHn2rTpAl4dGN+FoXGNjSuVQoSv7b95EZR2fRfI=",
			wantPrefix:   "tellorvalcons",
			wantErr:      false,
		},
		{
			name:         "another valid pubkey",
			pubkeyBase64: "5xT0nJWRZCb4QBB1GNy32p5QYBcRIBPpN+yLlOY/r3c=",
			wantPrefix:   "tellorvalcons",
			wantErr:      false,
		},
		{
			name:         "invalid base64",
			pubkeyBase64: "not-valid-base64!!!",
			wantErr:      true,
		},
		{
			name:         "empty pubkey",
			pubkeyBase64: "",
			wantPrefix:   "tellorvalcons",
			wantErr:      false, // empty decodes to empty bytes, still valid
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := addr.ToValcons(tt.pubkeyBase64)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Contains(t, result, tt.wantPrefix)
		})
	}
}

func TestTruncateAddress(t *testing.T) {
	tests := []struct {
		name    string
		address string
		want    string
	}{
		{
			name:    "tellorvalcons address",
			address: "tellorvalcons1x6n9dgye3qqn7sl9svlesxcca426tl9x",
			want:    "tellorvalcons...a426tl9x",
		},
		{
			name:    "tellor address",
			address: "tellor1x6n9dgye3qqn7sl9svlesxcca426tl9xcqu7c7",
			want:    "tellor...9xcqu7c7",
		},
		{
			name:    "short address",
			address: "short",
			want:    "short",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateAddress(tt.address)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestFormatLoya(t *testing.T) {
	tests := []struct {
		name   string
		amount uint64
		want   string
	}{
		{
			name:   "zero",
			amount: 0,
			want:   "0.00",
		},
		{
			name:   "one TRB",
			amount: 1_000_000,
			want:   "1.00",
		},
		{
			name:   "fractional TRB",
			amount: 1_500_000,
			want:   "1.50",
		},
		{
			name:   "large amount",
			amount: 123_456_789,
			want:   "123.46",
		},
		{
			name:   "small amount",
			amount: 500,
			want:   "0.00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatLoya(tt.amount)
			require.Equal(t, tt.want, got)
		})
	}
}
