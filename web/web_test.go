package web

import (
	"testing"

	"github.com/cryptoriums/layer-monitor/addr"
	"github.com/stretchr/testify/require"
	reportertypes "github.com/tellor-io/layer/x/reporter/types"
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

func TestUnmarshalSelectionsJSONAllowsNewLayerFields(t *testing.T) {
	body := []byte(`{
		"reporter":"tellor128m9knt3039k5rmaeu50q0g7y608g2w5etj2ra",
		"selections":[{
			"selector":"tellor128m9knt3039k5rmaeu50q0g7y608g2w5etj2ra",
			"locked_until_time":"0001-01-01T00:00:00Z",
			"delegations_count":"1",
			"delegations_total":"5238923037",
			"individual_delegations":[{
				"validator_address":"tellorvaloper128m9knt3039k5rmaeu50q0g7y608g2w5vy7c6d",
				"amount":"5238923037"
			}],
			"dispute_locked_until":"0001-01-01T00:00:00Z"
		}]
	}`)

	var result reportertypes.QuerySelectionsToResponse
	require.NoError(t, unmarshalSelectionsJSON(body, &result))
	require.Equal(t, "tellor128m9knt3039k5rmaeu50q0g7y608g2w5etj2ra", result.Reporter)
	require.Len(t, result.Selections, 1)
	require.Equal(t, "tellor128m9knt3039k5rmaeu50q0g7y608g2w5etj2ra", result.Selections[0].Selector)
	require.Len(t, result.Selections[0].IndividualDelegations, 1)
	require.Equal(t,
		"tellorvaloper128m9knt3039k5rmaeu50q0g7y608g2w5vy7c6d",
		result.Selections[0].IndividualDelegations[0].ValidatorAddress,
	)
	require.Equal(t, uint64(5238923037), result.Selections[0].IndividualDelegations[0].Amount.Uint64())
}
