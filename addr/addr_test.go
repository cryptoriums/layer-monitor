package addr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToValidatorOperator(t *testing.T) {
	tests := []struct {
		name       string
		wallet     string
		wantPrefix string
		wantEmpty  bool
	}{
		{
			name:       "valid wallet address",
			wallet:     "tellor1x6n9dgye3qqn7sl9svlesxcca426tl9xcqu7c7",
			wantPrefix: "tellorvaloper",
		},
		{
			name:      "empty address",
			wallet:    "",
			wantEmpty: true,
		},
		{
			name:      "invalid address",
			wallet:    "invalid",
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToValidatorOperator(tt.wallet)
			if tt.wantEmpty {
				require.Empty(t, got)
			} else {
				require.Contains(t, got, tt.wantPrefix)
			}
		})
	}
}

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
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ToValcons(tt.pubkeyBase64)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Contains(t, result, tt.wantPrefix)
		})
	}
}
