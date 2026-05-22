// Package encoding provides a shared codec for all cryptoriums packages.
package encoding

import (
	"sync"

	icq "github.com/cosmos/ibc-apps/modules/async-icq/v8"
	ica "github.com/cosmos/ibc-go/v8/modules/apps/27-interchain-accounts"
	ibctransfer "github.com/cosmos/ibc-go/v8/modules/apps/transfer"
	ibc "github.com/cosmos/ibc-go/v8/modules/core"
	ibctm "github.com/cosmos/ibc-go/v8/modules/light-clients/07-tendermint"
	"github.com/strangelove-ventures/globalfee/x/globalfee"
	bridgemodule "github.com/tellor-io/layer/x/bridge"
	disputemodule "github.com/tellor-io/layer/x/dispute"
	mintmodule "github.com/tellor-io/layer/x/mint"
	oraclemodule "github.com/tellor-io/layer/x/oracle"
	registrymodule "github.com/tellor-io/layer/x/registry/module"
	reportermodule "github.com/tellor-io/layer/x/reporter/module"

	evidencemodule "cosmossdk.io/x/evidence"
	feegrantmodule "cosmossdk.io/x/feegrant/module"
	upgrademodule "cosmossdk.io/x/upgrade"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	cryptocodec "github.com/cosmos/cosmos-sdk/crypto/codec"
	"github.com/cosmos/cosmos-sdk/std"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/module"
	authmodule "github.com/cosmos/cosmos-sdk/x/auth"
	authtx "github.com/cosmos/cosmos-sdk/x/auth/tx"
	vestingmodule "github.com/cosmos/cosmos-sdk/x/auth/vesting"
	authzmodule "github.com/cosmos/cosmos-sdk/x/authz/module"
	"github.com/cosmos/cosmos-sdk/x/bank"
	consensusmodule "github.com/cosmos/cosmos-sdk/x/consensus"
	distr "github.com/cosmos/cosmos-sdk/x/distribution"
	genutil "github.com/cosmos/cosmos-sdk/x/genutil"
	gov "github.com/cosmos/cosmos-sdk/x/gov"
	govclient "github.com/cosmos/cosmos-sdk/x/gov/client"
	groupmodule "github.com/cosmos/cosmos-sdk/x/group/module"
	slashingmodule "github.com/cosmos/cosmos-sdk/x/slashing"
	stakingmodule "github.com/cosmos/cosmos-sdk/x/staking"
)

var (
	once      sync.Once
	cdc       *codec.ProtoCodec
	txConfig  client.TxConfig
	txDecoder sdk.TxDecoder
)

func ensureInitialized() {
	once.Do(func() {
		registry := codectypes.NewInterfaceRegistry()

		// Register standard interfaces (required for tx decoding, keyring, etc.)
		std.RegisterInterfaces(registry)

		// Register crypto types (ed25519, secp256k1, etc.)
		cryptocodec.RegisterInterfaces(registry)

		// Register all module interfaces for tx decoding
		moduleBasics := module.NewBasicManager(
			genutil.AppModuleBasic{},
			authmodule.AppModuleBasic{},
			authzmodule.AppModuleBasic{},
			vestingmodule.AppModuleBasic{},
			bank.AppModuleBasic{},
			feegrantmodule.AppModuleBasic{},
			groupmodule.AppModuleBasic{},
			gov.NewAppModuleBasic([]govclient.ProposalHandler{}),
			slashingmodule.AppModuleBasic{},
			distr.AppModuleBasic{},
			stakingmodule.AppModuleBasic{},
			upgrademodule.AppModuleBasic{},
			evidencemodule.AppModuleBasic{},
			consensusmodule.AppModuleBasic{},
			ica.AppModuleBasic{},
			icq.AppModuleBasic{},
			ibc.AppModuleBasic{},
			ibctransfer.AppModuleBasic{},
			ibctm.AppModuleBasic{},
			mintmodule.AppModuleBasic{},
			oraclemodule.AppModuleBasic{},
			registrymodule.AppModuleBasic{},
			disputemodule.AppModuleBasic{},
			bridgemodule.AppModuleBasic{},
			reportermodule.AppModuleBasic{},
			globalfee.AppModuleBasic{},
		)
		moduleBasics.RegisterInterfaces(registry)

		// Create cached instances
		cdc = codec.NewProtoCodec(registry)
		txConfig = authtx.NewTxConfig(cdc, authtx.DefaultSignModes)

		protoDecoder := txConfig.TxDecoder()
		jsonDecoder := txConfig.TxJSONDecoder()
		txDecoder = func(txBytes []byte) (sdk.Tx, error) {
			if tx, err := protoDecoder(txBytes); err == nil {
				return tx, nil
			}
			return jsonDecoder(txBytes)
		}
	})
}

// MakeCodec returns the shared ProtoCodec with all required interfaces registered.
func MakeCodec() *codec.ProtoCodec {
	ensureInitialized()
	return cdc
}

// MakeTxConfig returns the shared TxConfig for encoding/decoding Layer transactions.
func MakeTxConfig() client.TxConfig {
	ensureInitialized()
	return txConfig
}

// MakeTxDecoder returns the shared TxDecoder for decoding Layer transactions.
// It tries protobuf first, then falls back to JSON.
func MakeTxDecoder() sdk.TxDecoder {
	ensureInitialized()
	return txDecoder
}
