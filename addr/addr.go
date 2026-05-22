package addr

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/cryptoriums/layer-monitor/encoding"

	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
)

const defaultRequestTimeout = 10 * time.Second

// ToValidatorOperator converts a tellor1xxx address to tellorvaloper1xxx.
// Both addresses share the same underlying bytes, just different bech32 prefixes.
func ToValidatorOperator(walletAddr string) string {
	if walletAddr == "" {
		return ""
	}

	_, addrBytes, err := bech32.DecodeAndConvert(walletAddr)
	if err != nil {
		return ""
	}

	validatorAddr, err := bech32.ConvertAndEncode("tellorvaloper", addrBytes)
	if err != nil {
		return ""
	}

	return validatorAddr
}

// ToValcons derives tellorvalcons address from consensus pubkey.
// The pubkey is base64 encoded ed25519 public key.
// Process: base64 decode -> SHA256 hash -> take first 20 bytes -> bech32 encode.
func ToValcons(pubkeyBase64 string) (string, error) {
	pubkeyBytes, err := base64.StdEncoding.DecodeString(pubkeyBase64)
	if err != nil {
		return "", fmt.Errorf("decode pubkey: %w", err)
	}

	hash := sha256.Sum256(pubkeyBytes)
	addrBytes := hash[:20]

	valconsAddr, err := bech32.ConvertAndEncode("tellorvalcons", addrBytes)
	if err != nil {
		return "", fmt.Errorf("bech32 encode: %w", err)
	}

	return valconsAddr, nil
}

// FetchValcons fetches validator consensus address from API using wallet address.
// Returns empty string if wallet is not a validator or on error.
func FetchValcons(apiURLs []string, walletAddress string) string {
	if walletAddress == "" || len(apiURLs) == 0 {
		return ""
	}

	operatorAddr := ToValidatorOperator(walletAddress)
	if operatorAddr == "" {
		return ""
	}

	client := &http.Client{Timeout: defaultRequestTimeout}
	cdc := encoding.MakeCodec()

	for _, baseURL := range apiURLs {
		url := fmt.Sprintf("%s/cosmos/staking/v1beta1/validators?pagination.limit=100", baseURL)

		resp, err := client.Get(url)
		if err != nil {
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			continue
		}

		var result stakingtypes.QueryValidatorsResponse
		if err := cdc.UnmarshalJSON(readBody(resp), &result); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()

		for _, v := range result.Validators {
			if v.OperatorAddress == operatorAddr && v.ConsensusPubkey != nil {
				pubkey, err := extractPubkeyBase64(v.ConsensusPubkey)
				if err != nil {
					continue
				}
				valcons, err := ToValcons(pubkey)
				if err == nil {
					return valcons
				}
			}
		}
	}

	return ""
}

// readBody reads and returns the response body.
func readBody(resp *http.Response) []byte {
	body := make([]byte, 0)
	buf := make([]byte, 1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			body = append(body, buf[:n]...)
		}
		if err != nil {
			break
		}
	}
	return body
}

// extractPubkeyBase64 extracts the base64 key from a consensus pubkey Any type.
func extractPubkeyBase64(pubkey *codectypes.Any) (string, error) {
	if pubkey == nil {
		return "", fmt.Errorf("pubkey is nil")
	}
	if len(pubkey.Value) > 2 {
		keyBytes := pubkey.Value[2:]
		return base64.StdEncoding.EncodeToString(keyBytes), nil
	}
	return "", fmt.Errorf("pubkey value too short: %d bytes", len(pubkey.Value))
}

// FetchChainID fetches the chain ID from the node API.
// Tries each API URL until one succeeds.
func FetchChainID(apiURLs []string) string {
	if len(apiURLs) == 0 {
		return ""
	}

	client := &http.Client{Timeout: defaultRequestTimeout}

	for _, baseURL := range apiURLs {
		url := fmt.Sprintf("%s/cosmos/base/tendermint/v1beta1/node_info", baseURL)

		resp, err := client.Get(url)
		if err != nil {
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			continue
		}

		var result struct {
			DefaultNodeInfo struct {
				Network string `json:"network"`
			} `json:"default_node_info"`
		}
		if err := json.Unmarshal(readBody(resp), &result); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()

		if result.DefaultNodeInfo.Network != "" {
			return result.DefaultNodeInfo.Network
		}
	}

	return ""
}
