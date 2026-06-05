package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/cryptoriums/layer-monitor/addr"
	"github.com/cryptoriums/layer-monitor/db"
	"github.com/cryptoriums/layer-monitor/monitors/balance"
	"github.com/cryptoriums/layer-monitor/monitors/block"
	"github.com/cryptoriums/layer-monitor/monitors/domain"
	"github.com/cryptoriums/layer-monitor/monitors/jail"
	"github.com/cryptoriums/layer-monitor/signerclient"
	"github.com/cryptoriums/layer-monitor/web"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"

	"cosmossdk.io/log"
)

const (
	// DefaultBackfillLookbackDays is the default number of days to look back for backfilling block data.
	DefaultBackfillLookbackDays = 7
	// defaultPollInterval is the default polling interval for fetching new blocks.
	defaultPollInterval = 800 * time.Millisecond
	// defaultDomainCheckInterval is the default interval to check domain/IP configuration.
	defaultDomainCheckInterval = 5 * time.Minute
	// defaultJailCheckInterval is the default interval to check validator jail status.
	defaultJailCheckInterval = 1 * time.Minute
	// defaultBalanceCheckInterval is the default interval to check balances.
	defaultBalanceCheckInterval = 5 * time.Minute
)

func main() {
	root := &cobra.Command{
		Use:   "monitord",
		Short: "Layer blockchain monitor",
	}

	cmd := &cobra.Command{
		Use:   "monitor",
		Short: "Run block monitor daemon",
		Long:  "monitor watches the Layer chain, stores metrics in ClickHouse, and serves a status dashboard.",
		Run:   runMonitor,
	}
	cmd.Flags().String("config", "/root/.layer/.env", "path to env config file")
	root.AddCommand(cmd)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func runMonitor(cmd *cobra.Command, _ []string) {
	configPath, _ := cmd.Flags().GetString("config")

	reg := prometheus.NewRegistry()

	// Load config from env file (Overload to override existing env vars from docker-compose).
	if err := godotenv.Overload(configPath); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not load config file %s: %v\n", configPath, err)
	}

	logger := log.NewLogger(os.Stdout)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	cfg, err := parseMonitorConfig()
	if err != nil {
		logger.Error("failed to parse config", "error", err)
		cancel()
		os.Exit(1) //nolint:gocritic // cancel() is called explicitly above
	}

	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%s/%s",
		cfg.clickhouseUser,
		cfg.clickhousePassword,
		cfg.clickhouseHost,
		cfg.clickhousePort,
		cfg.clickhouseDB,
	)
	sqlDB, err := sql.Open("clickhouse", dsn)
	if err != nil {
		logger.Error("failed to connect to clickhouse", "error", err)
		os.Exit(1)
	}
	defer func() { _ = sqlDB.Close() }()

	database, err := db.New(ctx, sqlDB)
	if err != nil {
		logger.Error("failed to initialize database", "error", err)
		os.Exit(1)
	}

	// Apply 1-month TTL to all tables (idempotent, safe to run every startup).
	if err := db.ConfigureTTL(ctx, database); err != nil {
		logger.Error("failed to configure TTL on ClickHouse tables", "error", err)
		os.Exit(1)
	}
	logger.Info("TTL configured on all ClickHouse tables (1 month)")

	walletAddress := resolveWalletAddress(ctx, logger, database, cfg.signer)
	if walletAddress == "" {
		logger.Error("could not determine reporter wallet address from the remote signer or the DB - refusing to start",
			"reason", "the wallet address is required for validator/reporter metrics, alerts, and the status page",
			"solution", "make the remote signer reachable (REMOTE_SIGNER_ADDR + mTLS certs), or ensure the reporter has run at least once so the address is stored in the DB")
		os.Exit(1)
	}

	validatorConsensusAddr := addr.FetchValcons(cfg.layerAPIURLs, walletAddress)
	if validatorConsensusAddr == "" {
		logger.Error("failed to derive validator consensus address from wallet - refusing to start",
			"wallet", walletAddress,
			"operator_addr", addr.ToValidatorOperator(walletAddress),
			"reason", "validator metrics and alerts would not work",
			"solution", "ensure wallet is a validator and API endpoints are reachable")
		os.Exit(1)
	}
	logger.Info("validator consensus address derived", "wallet", walletAddress, "valcons", validatorConsensusAddr)

	if err := db.UpsertAddress(ctx, database, db.AddressNameValidatorConsensus, validatorConsensusAddr); err != nil {
		logger.Error("failed to store validator consensus address", "error", err)
		os.Exit(1)
	}
	logger.Info("validator consensus address stored in DB", "valcons", validatorConsensusAddr)

	validatorOperatorAddr := addr.ToValidatorOperator(walletAddress)
	if err := db.UpsertAddress(ctx, database, db.AddressNameValidator, validatorOperatorAddr); err != nil {
		logger.Error("failed to store validator operator address", "error", err)
		os.Exit(1)
	}
	logger.Info("validator operator address stored in DB", "valoper", validatorOperatorAddr)

	monitorCfg := block.Config{
		Nodes:                     cfg.rpcNodes,
		LayerAPIURLs:              cfg.layerAPIURLs,
		BackfillLookback:          cfg.backfillLookback,
		PollInterval:              cfg.pollInterval,
		FetchWorkers:              cfg.fetchWorkers,
		WalletAddress:             walletAddress,
		ValidatorConsensusAddress: validatorConsensusAddr,
		Registerer:                reg,
	}

	blockMonitor, err := block.New(ctx, logger, monitorCfg, database)
	if err != nil {
		logger.Error("failed to create monitor", "error", err)
		os.Exit(1)
	}

	domainCfg := domain.Config{
		Domain:        cfg.domain,
		CheckInterval: cfg.domainCheckInterval,
	}
	domainChecker, err := domain.New(logger, domainCfg, reg)
	if err != nil {
		logger.Error("failed to create domain checker", "error", err)
		os.Exit(1)
	}
	go func() {
		if err := domainChecker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("domain checker stopped with error", "error", err)
		}
	}()

	jailCfg := jail.Config{
		LayerAPIURLs:  cfg.layerAPIURLs,
		WalletAddress: walletAddress,
		CheckInterval: cfg.jailCheckInterval,
	}
	jailMonitor, err := jail.New(logger, jailCfg, reg)
	if err != nil {
		logger.Error("failed to create jail monitor", "error", err)
		os.Exit(1)
	}
	go func() {
		if err := jailMonitor.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("jail monitor stopped with error", "error", err)
		}
	}()

	balanceCfg := balance.Config{
		LayerAPIURLs:  cfg.layerAPIURLs,
		WalletAddress: walletAddress,
		CheckInterval: defaultBalanceCheckInterval,
	}
	balanceMonitor, err := balance.New(logger, balanceCfg, reg)
	if err != nil {
		logger.Error("failed to create balance monitor", "error", err)
		os.Exit(1)
	}
	go func() {
		if err := balanceMonitor.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("balance monitor stopped with error", "error", err)
		}
	}()

	webCfg := web.Config{
		BindAddr:           cfg.webAddr,
		LayerAPIURLs:       cfg.layerAPIURLs,
		RPCNodes:           cfg.rpcNodes,
		PublicRPCURL:       cfg.publicRPCURL,
		PublicAPIURL:       cfg.publicAPIURL,
		ExplorerURL:        cfg.explorerURL,
		WalletAddress:      walletAddress,
		LookbackPeriodDays: cfg.backfillLookback,
		StatsPeriodDays:    web.DefaultStatsPeriodDays,
		TLSDomain:          cfg.domain,
		TLSEmail:           getEnv("TLS_EMAIL", ""),
		TLSCacheDir:        "/app/certs",
		Registry:           reg,
		Registerer:         reg,
	}
	webServer, err := web.New(logger, webCfg, database)
	if err != nil {
		logger.Error("failed to create web server", "error", err)
		os.Exit(1)
	}

	go func() {
		if err := webServer.Start(ctx); err != nil {
			logger.Error("web server stopped with error", "error", err)
		}
	}()

	logger.Info("starting monitor",
		"nodes", cfg.rpcNodes,
		"layer_api_urls", cfg.layerAPIURLs,
		"poll_interval", cfg.pollInterval,
		"backfill_lookback_days", cfg.backfillLookback,
		"web_addr", cfg.webAddr,
		"domain", cfg.domain,
		"wallet_address", walletAddress,
		"validator_consensus_address", validatorConsensusAddr,
	)

	if err := blockMonitor.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("monitor stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("monitor stopped")
}

// signerQueryTimeout bounds the startup wallet-address lookup so a down or slow
// signer cannot stall monitor startup — we fall back to the DB instead.
const signerQueryTimeout = 5 * time.Second

// resolveWalletAddress determines the reporter wallet address at startup.
//
// Order of precedence:
//  1. The remote signer (authoritative — it holds the key). On success the
//     address is persisted to the DB so it survives a later signer outage.
//  2. The address already stored in the DB (fallback when the signer is
//     unconfigured or unreachable).
//
// Returns "" only when neither source yields an address; the caller treats
// that as fatal.
func resolveWalletAddress(ctx context.Context, logger log.Logger, database db.Db, cfg signerclient.Config) string {
	if cfg.Enabled() {
		addr, err := signerclient.FetchAddressWithTimeout(ctx, cfg, signerQueryTimeout)
		if err != nil {
			logger.Warn("could not fetch wallet address from remote signer, falling back to DB",
				"signer_addr", cfg.Addr, "error", err)
		} else {
			logger.Info("reporter address fetched from remote signer", "wallet", addr, "signer_addr", cfg.Addr)
			if err := db.UpsertAddress(ctx, database, db.AddressNameReporter, addr); err != nil {
				// Non-fatal: we already have the address; persisting is a convenience
				// so the next startup can fall back to it if the signer is down.
				logger.Warn("failed to persist signer-provided reporter address to DB", "error", err)
			}
			return addr
		}
	} else {
		logger.Info("REMOTE_SIGNER_ADDR not set, using reporter address from DB")
	}

	addr, err := db.ReporterAddr(ctx, database)
	if err != nil {
		logger.Error("failed to read reporter address from DB", "error", err)
		return ""
	}
	if addr != "" {
		logger.Info("reporter address read from DB", "wallet", addr)
	}
	return addr
}

type monitorConfig struct {
	clickhouseHost      string
	clickhousePort      string
	clickhouseUser      string
	clickhousePassword  string
	clickhouseDB        string
	rpcNodes            []string
	pollInterval        time.Duration
	backfillLookback    int
	fetchWorkers        int
	webAddr             string
	layerAPIURLs        []string
	domain              string
	domainCheckInterval time.Duration
	jailCheckInterval   time.Duration
	publicRPCURL        string
	publicAPIURL        string
	explorerURL         string
	signer              signerclient.Config
}

func parseMonitorConfig() (monitorConfig, error) {
	var missing []string

	nodesStr := requireEnv("RPC_NODES", &missing)
	apiURLsStr := requireEnv("LAYER_API_URLS", &missing)

	cfg := monitorConfig{
		clickhouseHost:      requireEnv("CLICKHOUSE_HOST", &missing),
		clickhousePort:      requireEnv("CLICKHOUSE_PORT", &missing),
		clickhouseUser:      requireEnv("CLICKHOUSE_USER", &missing),
		clickhousePassword:  os.Getenv("CLICKHOUSE_PASSWORD"),
		clickhouseDB:        requireEnv("CLICKHOUSE_DB", &missing),
		pollInterval:        defaultPollInterval,
		backfillLookback:    DefaultBackfillLookbackDays,
		webAddr:             getEnv("WEB_ADDR", ":8080"),
		domain:              requireEnv("DOMAIN", &missing),
		domainCheckInterval: defaultDomainCheckInterval,
		jailCheckInterval:   defaultJailCheckInterval,
	}

	if len(missing) == 0 {
		if nodesStr != "" {
			cfg.rpcNodes = splitTrimmed(nodesStr)
		}
		if apiURLsStr != "" {
			cfg.layerAPIURLs = splitTrimmed(apiURLsStr)
		}
	}

	cfg.publicRPCURL = firstPublicURL(nodesStr)
	cfg.publicAPIURL = firstPublicURL(apiURLsStr)
	cfg.explorerURL = os.Getenv("EXPLORER_URL")

	// Remote signer connection (optional). When REMOTE_SIGNER_ADDR is set the
	// monitor queries the signer for the reporter wallet address at startup;
	// otherwise it relies on the address already stored in the DB.
	cfg.signer = signerclient.Config{
		Addr:       os.Getenv("REMOTE_SIGNER_ADDR"),
		CACert:     getEnv("REMOTE_SIGNER_CA_CERT", "/mtls/ca.crt"),
		ClientCert: getEnv("REMOTE_SIGNER_CLIENT_CERT", "/mtls/client.crt"),
		ClientKey:  getEnv("REMOTE_SIGNER_CLIENT_KEY", "/mtls/client.key"),
	}

	if len(missing) > 0 {
		return cfg, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	if pollStr := os.Getenv("POLL_INTERVAL"); pollStr != "" {
		d, err := time.ParseDuration(pollStr)
		if err != nil {
			return cfg, fmt.Errorf("invalid POLL_INTERVAL: %w", err)
		}
		cfg.pollInterval = d
	}

	if intervalStr := os.Getenv("DOMAIN_CHECK_INTERVAL"); intervalStr != "" {
		d, err := time.ParseDuration(intervalStr)
		if err != nil {
			return cfg, fmt.Errorf("invalid DOMAIN_CHECK_INTERVAL: %w", err)
		}
		cfg.domainCheckInterval = d
	}

	if lookbackStr := os.Getenv("BACKFILL_LOOKBACK"); lookbackStr != "" {
		days, err := strconv.Atoi(lookbackStr)
		if err != nil {
			return cfg, fmt.Errorf("invalid BACKFILL_LOOKBACK: %w", err)
		}
		cfg.backfillLookback = days
	}

	if workersStr := os.Getenv("FETCH_WORKERS"); workersStr != "" {
		workers, err := strconv.Atoi(workersStr)
		if err != nil {
			return cfg, fmt.Errorf("invalid FETCH_WORKERS: %w", err)
		}
		cfg.fetchWorkers = workers
	}

	return cfg, nil
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func requireEnv(key string, missing *[]string) string {
	val := os.Getenv(key)
	if val == "" {
		*missing = append(*missing, key)
	}
	return val
}

func splitTrimmed(s string) []string {
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// firstPublicURL returns the first URL from a comma-separated list that is not localhost.
func firstPublicURL(urlList string) string {
	if urlList == "" {
		return ""
	}
	for _, u := range strings.Split(urlList, ",") {
		u = strings.TrimSpace(u)
		lower := strings.ToLower(u)
		if strings.Contains(lower, "localhost") ||
			strings.Contains(lower, "127.0.0.1") ||
			strings.Contains(lower, "://layer:") {
			continue
		}
		return u
	}
	return ""
}
