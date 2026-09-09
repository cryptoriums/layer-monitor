package web

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"html/template"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	cryptoaddr "github.com/cryptoriums/layer-monitor/addr"
	blockdb "github.com/cryptoriums/layer-monitor/db"
	"github.com/cryptoriums/layer-monitor/encoding"
	monitor "github.com/cryptoriums/layer-monitor/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	reportertypes "github.com/tellor-io/layer/x/reporter/types"
	"golang.org/x/crypto/acme/autocert"

	"cosmossdk.io/log"

	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	"github.com/cosmos/gogoproto/jsonpb"
)

const (
	assetsDir         = "assets"
	valueNA           = "N/A"
	valueZero         = "0.00"
	OperatorStartYear = 2020
)

type Config struct {
	BindAddr           string        `yaml:"bind_addr"`
	Timeout            time.Duration `yaml:"timeout"`
	LayerAPIURLs       []string      `yaml:"layer_api_urls"`       // Multiple URLs for redundancy
	RPCNodes           []string      `yaml:"rpc_nodes"`            // RPC nodes (tcp:// format)
	PublicRPCURL       string        `yaml:"public_rpc_url"`       // Public RPC URL for browser clients
	PublicAPIURL       string        `yaml:"public_api_url"`       // Public API URL for browser clients
	ExplorerURL        string        `yaml:"explorer_url"`         // Block explorer URL (e.g., "https://tellorscan.com")
	WalletAddress      string        `yaml:"wallet_address"`       // Wallet address (tellor1xxx)
	DelegateReporter   string        `yaml:"delegate_reporter"`    // Override one-click delegation reporter (default: WalletAddress)
	DelegateValidator  string        `yaml:"delegate_validator"`   // Override one-click delegation validator (default: operator of WalletAddress)
	LookbackPeriodDays int           `yaml:"lookback_period_days"` // Days to look back for statistics (default 7)
	StatsPeriodDays    int           `yaml:"stats_period_days"`    // Days to display in Network Statistics section (default 30)

	// TLS/HTTPS configuration (Let's Encrypt)
	TLSDomain   string `yaml:"tls_domain"`    // Domain name for certificate (e.g., "status.example.com")
	TLSEmail    string `yaml:"tls_email"`     // Email for Let's Encrypt notifications
	TLSCacheDir string `yaml:"tls_cache_dir"` // Directory to cache certificates (default: ./certs)

	// Prometheus gatherer shared with jail/domain monitors so their metrics appear
	// on /metrics. The status page itself reads only from the DB, never Prometheus.
	Registry prometheus.Gatherer
}

type Server struct {
	assetVerOnce sync.Once
	assetVer     string

	cfg        Config
	logger     log.Logger
	db         blockdb.Db
	httpClient *http.Client
	cdc        *codec.ProtoCodec

	// Cache for validator tree (refreshed every hour or on demand)
	cacheMu           sync.RWMutex
	cachedTree        []ValidatorTree
	cacheTimestamp    time.Time
	cacheRefreshing   bool
	cachedRewardStats map[int]NetworkRewardStats // per-period network reward stats, refreshed with the tree

	// ctx is the server's root context, stored for background operations (e.g. cache refresh).
	ctx context.Context

	// TLS/HTTPS with Let's Encrypt
	certManager *autocert.Manager
	certMu      sync.RWMutex
	certExpiry  time.Time // Cached certificate expiry time
}

const (
	// CacheDuration is how long the validator tree cache is valid
	CacheDuration = 1 * time.Hour

	// DefaultLookbackPeriodDays is the default number of days to look back for statistics queries.
	DefaultLookbackPeriodDays = 7

	// DefaultStatsPeriodDays is the number of days shown in the Network Statistics section.
	// The DB may retain more data (e.g. 45 days as a buffer), but we display 30.
	DefaultStatsPeriodDays = 30
)

func New(logger log.Logger, cfg Config, db blockdb.Db) (*Server, error) {
	if cfg.BindAddr == "" {
		cfg.BindAddr = ":80"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.LookbackPeriodDays <= 0 {
		cfg.LookbackPeriodDays = DefaultLookbackPeriodDays
	}
	if cfg.StatsPeriodDays <= 0 {
		cfg.StatsPeriodDays = DefaultStatsPeriodDays
	}
	if len(cfg.LayerAPIURLs) == 0 {
		return nil, fmt.Errorf("LayerAPIURLs is required")
	}
	if cfg.TLSDomain == "" {
		return nil, fmt.Errorf("TLSDomain is required")
	}

	cdc := encoding.MakeCodec()

	if cfg.Registry == nil {
		cfg.Registry = prometheus.DefaultGatherer
	}

	s := &Server{
		cfg:    cfg,
		logger: logger.With("component", "web"),
		db:     db,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
		cdc: cdc,
	}

	cacheDir := cfg.TLSCacheDir
	if cacheDir == "" {
		cacheDir = "./certs"
	}
	s.certManager = &autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		HostPolicy: autocert.HostWhitelist(cfg.TLSDomain),
		Cache:      autocert.DirCache(cacheDir),
		Email:      cfg.TLSEmail,
	}
	logger.Info("TLS enabled with Let's Encrypt", "domain", cfg.TLSDomain, "cache_dir", cacheDir)

	return s, nil
}

func (s *Server) Start(ctx context.Context) error {
	s.ctx = ctx
	// Log TLS configuration at startup for debugging
	s.logger.Info("starting web server",
		"tls_domain", s.cfg.TLSDomain,
		"tls_email", s.cfg.TLSEmail,
		"tls_cache_dir", s.cfg.TLSCacheDir,
		"bind_addr", s.cfg.BindAddr,
	)

	// Pre-warm cache in background on startup
	go s.PrewarmCache(ctx)

	handler := s.routes()

	// Start certificate expiry checker in background
	go s.runCertExpiryChecker(ctx)

	// HTTP server for health checks, ACME HTTP-01 challenges, and HTTPS redirect
	httpSrv := &http.Server{
		Addr:              ":80",
		Handler:           s.httpHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// HTTPS server with autocert TLS config
	httpsSrv := &http.Server{
		Addr:              ":443",
		Handler:           handler,
		ReadHeaderTimeout: s.cfg.Timeout,
		TLSConfig: &tls.Config{
			GetCertificate: s.certManager.GetCertificate,
			MinVersion:     tls.VersionTLS12,
		},
	}

	// Graceful shutdown for both servers
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			s.logger.Error("http server shutdown error", "error", err)
		}
		if err := httpsSrv.Shutdown(shutdownCtx); err != nil {
			s.logger.Error("https server shutdown error", "error", err)
		}
	}()

	// Start HTTP server in background (must be ready for ACME challenges)
	httpReady := make(chan struct{})
	go func() {
		s.logger.Info("http server starting (ACME + redirect)", "addr", ":80")
		close(httpReady) // Signal that we're about to listen
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("http server error", "error", err)
		}
	}()

	// Wait for HTTP server to be ready before starting HTTPS
	<-httpReady
	time.Sleep(100 * time.Millisecond) // Small delay to ensure listener is up

	// Start HTTPS server (blocking)
	s.logger.Info("https server starting", "addr", ":443", "domain", s.cfg.TLSDomain)
	if err := httpsSrv.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// httpHandler returns the HTTP handler for port 80.
// It handles health checks, ACME challenges, and redirects everything else to HTTPS.
func (s *Server) httpHandler() http.Handler {
	acmeHandler := s.certManager.HTTPHandler(http.HandlerFunc(s.redirectToHTTPS))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		acmeHandler.ServeHTTP(w, r)
	})
}

// redirectToHTTPS redirects HTTP requests to HTTPS.
func (s *Server) redirectToHTTPS(w http.ResponseWriter, r *http.Request) {
	target := "https://" + s.cfg.TLSDomain + r.URL.RequestURI()
	http.Redirect(w, r, target, http.StatusMovedPermanently)
}

// runCertExpiryChecker periodically checks certificate expiry and logs warnings.
// Let's Encrypt certificates are valid for 90 days; autocert renews at ~30 days before expiry.
func (s *Server) runCertExpiryChecker(ctx context.Context) {
	// Initial check after startup
	time.Sleep(10 * time.Second)
	s.checkCertExpiry()

	// Check every 6 hours
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkCertExpiry()
		}
	}
}

// checkCertExpiry checks the current certificate expiry and updates cached value.
func (s *Server) checkCertExpiry() {
	if s.certManager == nil || s.cfg.TLSDomain == "" {
		return
	}

	// Get certificate from cache
	hello := &tls.ClientHelloInfo{ServerName: s.cfg.TLSDomain}
	cert, err := s.certManager.GetCertificate(hello)
	if err != nil {
		s.logger.Warn("failed to get certificate for expiry check", "error", err)
		return
	}

	if cert == nil || len(cert.Certificate) == 0 {
		s.logger.Warn("no certificate available for expiry check")
		return
	}

	// Parse the leaf certificate to get expiry
	leaf := cert.Leaf
	if leaf == nil {
		// If Leaf is not populated, parse it from raw certificate
		var parseErr error
		leaf, parseErr = parseCertificate(cert.Certificate[0])
		if parseErr != nil {
			s.logger.Warn("failed to parse certificate", "error", parseErr)
			return
		}
	}

	// Update cached expiry
	s.certMu.Lock()
	s.certExpiry = leaf.NotAfter
	s.certMu.Unlock()

	daysUntilExpiry := int(time.Until(leaf.NotAfter).Hours() / 24)
	s.logger.Info("certificate expiry checked",
		"domain", s.cfg.TLSDomain,
		"expires", leaf.NotAfter.Format("2006-01-02"),
		"days_until_expiry", daysUntilExpiry,
	)

	// Warn if certificate expires within 14 days (autocert should renew at 30 days)
	if daysUntilExpiry < 14 {
		s.logger.Warn("certificate expires soon - autocert should renew automatically",
			"domain", s.cfg.TLSDomain,
			"days_until_expiry", daysUntilExpiry,
		)
	}
}

// GetCertExpiry returns the cached certificate expiry time.
func (s *Server) GetCertExpiry() time.Time {
	s.certMu.RLock()
	defer s.certMu.RUnlock()
	return s.certExpiry
}

// parseCertificate parses a DER-encoded certificate.
func parseCertificate(der []byte) (*x509.Certificate, error) {
	return x509.ParseCertificate(der)
}

// PrewarmCache builds the validator tree cache in background on startup.
// This ensures the first page request doesn't have to wait for tree building.
func (s *Server) PrewarmCache(ctx context.Context) {
	s.logger.Info("pre-warming validator tree cache")
	s.refreshTreeCache(ctx)
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRoot)
	mux.HandleFunc("/api/tree", s.handleGetTree)
	mux.HandleFunc("/api/refresh-tree", s.handleRefreshTree)
	mux.HandleFunc("/assets/", s.handleAssets)
	mux.Handle("/metrics", promhttp.HandlerFor(s.cfg.Registry, promhttp.HandlerOpts{}))
	return mux
}

// handleAssets serves static files (CSS, JS, images) with aggressive caching and gzip compression.
func (s *Server) handleAssets(w http.ResponseWriter, r *http.Request) {
	// Asset URLs are versioned by the template (/assets/script.js?v=<hash>), so a pinned
	// request can be cached hard: its content cannot change without the URL changing.
	// Unversioned requests - a direct hit, or an old cached page - still revalidate, which
	// is cheap because the ETag below returns 304 when the file is unchanged.
	if r.URL.Query().Get("v") != "" {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "no-cache, must-revalidate")
	}
	w.Header().Set("Vary", "Accept-Encoding")

	fs := http.FileServer(http.Dir(assetsDir))
	ext := path.Ext(r.URL.Path)
	gzipOK := ext == ".css" || ext == ".js"

	trimmed := strings.TrimPrefix(r.URL.Path, "/assets/")
	clean := path.Clean(trimmed)
	if clean != "." && !strings.HasPrefix(clean, "..") {
		assetPath := filepath.Join(assetsDir, clean)
		if data, err := os.ReadFile(assetPath); err == nil {
			h := fnv.New64a()
			h.Write(data)
			sum := h.Sum(nil)
			hash := hex.EncodeToString(sum)[:8]
			w.Header().Set("ETag", `"`+hash+`"`)
			w.Header().Set("X-Asset-Hash", hash)
		}
	}

	if gzipOK && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer func() { _ = gz.Close() }()
		gzw := &gzipResponseWriter{ResponseWriter: w, Writer: gz}
		http.StripPrefix("/assets/", fs).ServeHTTP(gzw, r)
		return
	}

	http.StripPrefix("/assets/", fs).ServeHTTP(w, r)
}

type gzipResponseWriter struct {
	http.ResponseWriter
	io.Writer
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

// assetVersion returns a short content hash over the served assets, used to version their
// URLs (/assets/script.js?v=<hash>). It changes exactly when an asset changes, so a deploy
// invalidates browser caches while unchanged assets stay cached. Memoised; if an asset
// cannot be read it falls back to the process start time, which is still unique per deploy.
func (s *Server) assetVersion() string {
	s.assetVerOnce.Do(func() {
		h := sha256.New()
		for _, n := range []string{"style.css", "script.js"} {
			b, err := os.ReadFile(filepath.Join(assetsDir, n))
			if err != nil {
				s.assetVer = strconv.FormatInt(time.Now().Unix(), 10)
				return
			}
			h.Write(b)
		}
		s.assetVer = hex.EncodeToString(h.Sum(nil))[:12]
	})
	return s.assetVer
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	// The HTML carries the versioned asset URLs, so it must be revalidated rather than
	// served from cache - otherwise an old page keeps requesting old asset versions.
	// Browsers given no header at all apply heuristic caching and held this page for days.
	// The page is cheap to regenerate; the assets it points at are the expensive part and
	// those are cached hard, busted by their ?v= hash.
	w.Header().Set("Cache-Control", "no-cache, must-revalidate")

	ctx := r.Context()

	// Non-blocking: get whatever is in cache, don't wait for building
	validatorTree, cacheTime, isLoading := s.getValidatorTreeNonBlocking()

	// Fetch reporter data (fast, usually cached by HTTP client)
	reporters := s.fetchReporters(ctx)

	// Get total network power and our reporter power
	totalNetworkPower := s.calculateTotalPower(reporters)
	ourReporterPower := valueNA
	if reporter, ok := reporters[s.cfg.WalletAddress]; ok && reporter != nil {
		ourReporterPower = fmt.Sprintf("%d", reporter.Power)
	}

	// Parse optional period query param (1, 7, or 30 days); default to config value
	periodDays := s.cfg.StatsPeriodDays
	if p := r.URL.Query().Get("period"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && (n == 1 || n == 7 || n == 30) {
			periodDays = n
		}
	}

	// Get network reward statistics for the stats display period (served from cache; the
	// background refresh pre-computes all periods so this doesn't scan rewards per request).
	s.cacheMu.RLock()
	rewardStats, statsCached := s.cachedRewardStats[periodDays]
	s.cacheMu.RUnlock()
	if !statsCached {
		rewardStats = s.queryNetworkRewardStats(ctx, periodDays)
	}

	// Derive addresses and moniker from wallet
	ourReporterAddr := s.cfg.WalletAddress
	ourValidatorAddr := cryptoaddr.ToValidatorOperator(s.cfg.WalletAddress)

	// One-click delegation target. Defaults to the operator's own reporter/validator; the
	// env overrides let the flow be tested against a low-min reporter with a small stake.
	delegateReporterAddr := ourReporterAddr
	if s.cfg.DelegateReporter != "" {
		delegateReporterAddr = s.cfg.DelegateReporter
	}
	delegateValidatorAddr := ourValidatorAddr
	if s.cfg.DelegateValidator != "" {
		delegateValidatorAddr = s.cfg.DelegateValidator
	}
	ourMoniker := ""
	if reporter, ok := reporters[s.cfg.WalletAddress]; ok && reporter != nil && reporter.Metadata != nil {
		ourMoniker = reporter.Metadata.Moniker
	}
	if ourMoniker == "" {
		ourMoniker = truncateAddress(s.cfg.WalletAddress)
	}

	// Calculate experience years
	experienceYears := time.Now().Year() - OperatorStartYear
	if experienceYears < 1 {
		experienceYears = 1
	}

	// Format cache timestamp as relative time
	var cacheTimeStr string
	if !cacheTime.IsZero() {
		cacheTimeStr = formatRelativeTime(cacheTime)
	}

	tpl, err := loadTemplate()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tpl.Execute(w, map[string]any{
		"AssetVersion":      s.assetVersion(),
		"ValidatorTree":     validatorTree,
		"TreeCacheTime":     cacheTimeStr,
		"TreeLoading":       isLoading,
		"Period":            fmt.Sprintf("%d Days", periodDays),
		"PeriodDays":        periodDays,
		"TotalNetworkPower": totalNetworkPower,
		"OurReporterPower":  ourReporterPower,
		"TotalReporting":    rewardStats.TotalReporting,
		"OurReporting":      rewardStats.OurReporting,
		"TotalValidating":   rewardStats.TotalValidating,
		"OurValidating":     rewardStats.OurValidating,
		"OurMoniker":        ourMoniker,
		"OurReporterAddr":   delegateReporterAddr,
		"OurValidatorAddr":  delegateValidatorAddr,
		"OperatorStartYear": OperatorStartYear,
		"ExperienceYears":   experienceYears,
		"LayerAPIURL":       s.cfg.PublicAPIURL,
		"LayerRPCURL":       s.cfg.PublicRPCURL,
		"ChainID":           cryptoaddr.FetchChainID(s.cfg.LayerAPIURLs),
		"ExplorerURL":       s.cfg.ExplorerURL,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func loadTemplate() (*template.Template, error) {
	funcs := template.FuncMap{
		"inc": func(i int) int { return i + 1 },
	}
	templateContent, err := os.ReadFile(filepath.Join(assetsDir, "template.html"))
	if err != nil {
		return nil, err
	}
	return template.New("page").Funcs(funcs).Parse(string(templateContent))
}

// getValidatorTreeNonBlocking returns the current cache without waiting.
// Returns (tree, timestamp, isLoading) where isLoading is true if cache is being built.
// If the cache is stale (older than CacheDuration), triggers a background refresh.
func (s *Server) getValidatorTreeNonBlocking() ([]ValidatorTree, time.Time, bool) {
	s.cacheMu.RLock()
	tree := s.cachedTree
	timestamp := s.cacheTimestamp
	refreshing := s.cacheRefreshing
	stale := !timestamp.IsZero() && time.Since(timestamp) > CacheDuration && !refreshing
	s.cacheMu.RUnlock()

	if stale {
		// Trigger background refresh, return stale data immediately
		go s.refreshTreeCache(s.ctx)
	}

	return tree, timestamp, refreshing
}

// handleGetTree returns the validator tree as JSON for AJAX loading.
func (s *Server) handleGetTree(w http.ResponseWriter, r *http.Request) {
	tree, timestamp, isLoading := s.getValidatorTreeNonBlocking()

	// Parse optional period param; default to LookbackPeriodDays
	periodDays := s.cfg.LookbackPeriodDays
	if p := r.URL.Query().Get("period"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && (n == 1 || n == 7 || n == 30) {
			periodDays = n
		}
	}

	// Convert to cached (serialisable) tree
	validators := toCachedTree(tree)

	// If a non-default period was requested, overlay fresh rewards + missed metrics
	if periodDays != s.cfg.LookbackPeriodDays && len(validators) > 0 {
		s.populateCachedRewardsForPeriod(r.Context(), validators, periodDays)
		s.populateCachedMissedCyclesForPeriod(r.Context(), validators, periodDays)
		s.populateCachedMissedBlocksForPeriod(r.Context(), validators, periodDays)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"loading":    isLoading,
		"timestamp":  formatRelativeTime(timestamp),
		"validators": validators,
	})
}

// populateCachedRewardsForPeriod re-queries rewards for a specific period and updates
// the already-converted CachedValidatorTree slice in place.
func (s *Server) populateCachedRewardsForPeriod(ctx context.Context, validators []CachedValidatorTree, periodDays int) {
	if s.db == nil {
		return
	}
	// Collect addresses
	valAddrs := make([]string, 0, len(validators))
	for i := range validators {
		if validators[i].OperatorAddress != "" {
			valAddrs = append(valAddrs, validators[i].OperatorAddress)
		}
	}
	repAddrs := make([]string, 0)
	for i := range validators {
		for j := range validators[i].Reporters {
			if validators[i].Reporters[j].Address != "" {
				repAddrs = append(repAddrs, validators[i].Reporters[j].Address)
			}
		}
	}
	valRewards := s.getValidatorRewardsMap(ctx, valAddrs, periodDays)
	repRewards := s.getReporterRewardsMap(ctx, repAddrs, periodDays)

	for i := range validators {
		if r, ok := valRewards[validators[i].OperatorAddress]; ok {
			validators[i].Rewards = formatLoya(r)
		} else {
			validators[i].Rewards = valueZero
		}
		for j := range validators[i].Reporters {
			if r, ok := repRewards[validators[i].Reporters[j].Address]; ok {
				validators[i].Reporters[j].Rewards = formatLoya(r)
			} else {
				validators[i].Reporters[j].Rewards = valueZero
			}
		}
	}
}

// populateCachedMissedBlocksForPeriod re-queries missed blocks for a specific period
// and updates the CachedValidatorTree slice in place using the cached ValconsAddress.
func (s *Server) populateCachedMissedBlocksForPeriod(ctx context.Context, validators []CachedValidatorTree, periodDays int) {
	if s.db == nil {
		return
	}
	missedMap, totalBlocks := s.getMissedBlocksPerValidatorFromDB(ctx, periodDays)
	for i := range validators {
		missed := int64(0)
		if validators[i].ValconsAddress != "" {
			missed = missedMap[validators[i].ValconsAddress]
		}
		validators[i].MissedBlocks = missed
		validators[i].MissedBlocksPct = formatMissedPct(missed, totalBlocks)
		validators[i].Status = computeStatus(validators[i].Jailed, missed)
	}
}

// populateCachedMissedCyclesForPeriod re-queries missed cycles for a specific period
// and updates reporter nodes in the CachedValidatorTree slice in place.
func (s *Server) populateCachedMissedCyclesForPeriod(ctx context.Context, validators []CachedValidatorTree, periodDays int) {
	if s.db == nil {
		return
	}
	missedMap, totalCycles := s.getMissedCyclesPerReporterFromDBForPeriod(ctx, periodDays)
	for i := range validators {
		for j := range validators[i].Reporters {
			reporterAddr := validators[i].Reporters[j].Address
			if reporterAddr == "" {
				validators[i].Reporters[j].MissedCycles = 0
				validators[i].Reporters[j].MissedCyclesPct = "-"
				continue
			}

			if missedCycles, ok := missedMap[reporterAddr]; ok {
				validators[i].Reporters[j].MissedCycles = missedCycles
			} else {
				// Reporter submitted no cyclelist reports in this period.
				validators[i].Reporters[j].MissedCycles = totalCycles
			}
			validators[i].Reporters[j].MissedCyclesPct = formatMissedPct(validators[i].Reporters[j].MissedCycles, totalCycles)
			validators[i].Reporters[j].Status = computeStatus(validators[i].Reporters[j].Jailed, validators[i].Reporters[j].MissedCycles)
		}
	}
}

// ReportStats holds network-wide report statistics.
type ReportStats struct {
	TotalReports  int64
	OurReports    int64
	OurReportsPct string
}

// NetworkRewardStats holds aggregated reward totals for the stats period.
type NetworkRewardStats struct {
	TotalReporting  string // Total reporter_tip rewards across all reporters (TRB)
	OurReporting    string // Our reporter_tip rewards (TRB)
	TotalValidating string // Total validator_delegator rewards across all validators (TRB)
	OurValidating   string // Our validator_delegator rewards (TRB)
}

// queryNetworkRewardStats aggregates reporting and validating rewards for the stats display period.
func (s *Server) queryNetworkRewardStats(ctx context.Context, periodDays int) NetworkRewardStats {
	ourReporterAddr := s.cfg.WalletAddress
	ourValidatorAddr := cryptoaddr.ToValidatorOperator(s.cfg.WalletAddress)

	// One scan over the period computes all four sums via conditional aggregation, instead of
	// four separate full-table scans of the rewards table.
	q := fmt.Sprintf(`
		SELECT
			coalesce(sumIf(toFloat64(%[1]s), %[3]s = '%[5]s'), 0),
			coalesce(sumIf(toFloat64(%[1]s), %[3]s = '%[5]s' AND %[4]s = ?), 0),
			coalesce(sumIf(toFloat64(%[1]s), %[3]s = '%[6]s'), 0),
			coalesce(sumIf(toFloat64(%[1]s), %[3]s = '%[6]s' AND %[4]s = ?), 0)
		FROM %[2]s
		WHERE %[7]s >= now() - INTERVAL %[8]d DAY
	`, blockdb.ColAmount, blockdb.TableNameRewards, blockdb.ColType, blockdb.ColRecipient,
		blockdb.RewardTypeReporterTip, blockdb.RewardTypeValidatorDelegator,
		blockdb.ColBlockTime, periodDays)

	rows, err := s.db.Query(ctx, q, ourReporterAddr, ourValidatorAddr)
	if err != nil {
		s.logger.Error("failed to query network reward stats", "error", err)
		return NetworkRewardStats{}
	}
	defer func() { _ = rows.Close() }()
	var totalReporting, ourReporting, totalValidating, ourValidating float64
	if rows.Next() {
		_ = rows.Scan(&totalReporting, &ourReporting, &totalValidating, &ourValidating)
	}
	return NetworkRewardStats{
		TotalReporting:  formatLoya(uint64(totalReporting)),
		OurReporting:    formatLoya(uint64(ourReporting)),
		TotalValidating: formatLoya(uint64(totalValidating)),
		OurValidating:   formatLoya(uint64(ourValidating)),
	}
}

// calculateTotalPower returns the total network power for reporters.
func (s *Server) calculateTotalPower(reporters map[string]*reportertypes.Reporter) (totalPower string) {
	var total uint64
	for _, reporter := range reporters {
		if reporter == nil {
			continue
		}
		total += reporter.Power
	}
	totalPower = fmt.Sprintf("%d", total)

	return totalPower
}

// formatLoya formats loya amount to TRB with 2 decimal precision, no suffix.
// 1 TRB = 1,000,000 loya (6 decimals)
func formatLoya(amount uint64) string {
	if amount == 0 {
		return valueZero
	}

	// Convert to TRB with 2 decimal places
	trb := float64(amount) / monitor.LoyaPerTRB
	return fmt.Sprintf("%.2f", trb)
}

// Status constants for validators and reporters.
const (
	StatusActive   = "Active"
	StatusDegraded = "Degraded"
	StatusJailed   = "Jailed"
)

// computeStatus returns the status based on jailed state and missed count.
// Priority: Jailed > Degraded (missed > 10) > Active
func computeStatus(jailed bool, missedCount int64) string {
	if jailed {
		return StatusJailed
	}
	if missedCount > 10 {
		return StatusDegraded
	}
	return StatusActive
}

// MintRate holds the mint rate values for reporters and validators.
type MintRate struct {
	Reporters  string // TRB per day for reporters
	Validators string // TRB per day for validators
}

// getValidatorRewardsMap fetches rewards for multiple validators in a single query.
// Returns a map of validator operator address -> total rewards (loya).
func (s *Server) getValidatorRewardsMap(ctx context.Context, validatorAddresses []string, periodDays int) map[string]uint64 {
	if len(validatorAddresses) == 0 {
		return make(map[string]uint64)
	}

	// Build IN clause for all addresses
	placeholders := make([]string, len(validatorAddresses))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf(`
		SELECT
			%s,
			coalesce(sum(toFloat64(%s)), 0) AS total_rewards
		FROM %s
		WHERE %s IN (%s)
		  AND %s = ?
		  AND %s >= now() - INTERVAL %d DAY
		GROUP BY %s
	`, blockdb.ColRecipient, blockdb.ColAmount, blockdb.TableNameRewards,
		blockdb.ColRecipient, strings.Join(placeholders, ","),
		blockdb.ColType,
		blockdb.ColBlockTime, periodDays,
		blockdb.ColRecipient)

	// Build args slice with addresses + reward type
	args := make([]any, len(validatorAddresses)+1)
	for i, addr := range validatorAddresses {
		args[i] = addr
	}
	args[len(validatorAddresses)] = blockdb.RewardTypeValidatorDelegator

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		s.logger.Debug("validator rewards batch query error", "error", err)
		return make(map[string]uint64)
	}
	defer func() { _ = rows.Close() }()

	rewardsMap := make(map[string]uint64)
	for rows.Next() {
		var recipient string
		var totalRewards float64
		if err := rows.Scan(&recipient, &totalRewards); err != nil {
			s.logger.Debug("validator rewards batch scan error", "error", err)
			continue
		}
		rewardsMap[recipient] = uint64(totalRewards)
	}

	return rewardsMap
}

// getReporterRewardsMap fetches rewards for multiple reporters in a single query.
// Returns a map of reporter address -> total rewards (loya).
func (s *Server) getReporterRewardsMap(ctx context.Context, reporterAddresses []string, periodDays int) map[string]uint64 {
	if len(reporterAddresses) == 0 {
		return make(map[string]uint64)
	}

	// Build IN clause for all addresses
	placeholders := make([]string, len(reporterAddresses))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf(`
		SELECT
			%s,
			coalesce(sum(toFloat64(%s)), 0) AS total_rewards
		FROM %s
		WHERE %s IN (%s)
		  AND %s = ?
		  AND %s >= now() - INTERVAL %d DAY
		GROUP BY %s
	`, blockdb.ColRecipient, blockdb.ColAmount, blockdb.TableNameRewards,
		blockdb.ColRecipient, strings.Join(placeholders, ","),
		blockdb.ColType,
		blockdb.ColBlockTime, periodDays,
		blockdb.ColRecipient)

	// Build args slice with addresses + reward type
	args := make([]any, len(reporterAddresses)+1)
	for i, addr := range reporterAddresses {
		args[i] = addr
	}
	args[len(reporterAddresses)] = blockdb.RewardTypeReporterTip

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		s.logger.Debug("reporter rewards batch query error", "error", err)
		return make(map[string]uint64)
	}
	defer func() { _ = rows.Close() }()

	rewardsMap := make(map[string]uint64)
	for rows.Next() {
		var recipient string
		var totalRewards float64
		if err := rows.Scan(&recipient, &totalRewards); err != nil {
			s.logger.Debug("reporter rewards batch scan error", "error", err)
			continue
		}
		rewardsMap[recipient] = uint64(totalRewards)
	}

	return rewardsMap
}

// formatMissedPct returns the missed rate as a percentage string (e.g. "0.50%").
// Returns "0%" when total is zero (no data yet) or missed is zero.
func formatMissedPct(missed, total int64) string {
	if total <= 0 || missed <= 0 {
		return "0%"
	}
	pct := float64(missed) * 100 / float64(total)
	switch {
	case pct >= 10:
		return fmt.Sprintf("%.0f%%", pct)
	case pct >= 1:
		return fmt.Sprintf("%.1f%%", pct)
	case pct >= 0.01:
		return fmt.Sprintf("%.2f%%", pct)
	default:
		// Non-zero but smaller than 0.01% — avoid rendering as "0.00%"
		// which would be indistinguishable from "no misses".
		return "~0%"
	}
}

// formatRelativeTime formats a duration into a human-readable relative time string.
// Examples: "3h ago", "2d ago", "just now"
func formatRelativeTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	duration := time.Since(t)

	switch {
	case duration < time.Minute:
		return "just now"
	case duration < time.Hour:
		mins := int(duration.Minutes())
		return fmt.Sprintf("%dm ago", mins)
	case duration < 24*time.Hour:
		hours := int(duration.Hours())
		return fmt.Sprintf("%dh ago", hours)
	case duration < 7*24*time.Hour:
		days := int(duration.Hours() / 24)
		return fmt.Sprintf("%dd ago", days)
	default:
		weeks := int(duration.Hours() / (24 * 7))
		return fmt.Sprintf("%dw ago", weeks)
	}
}

// truncateAddress returns address in format "prefix...last8chars"
func truncateAddress(address string) string {
	if len(address) <= 12 {
		return address
	}

	prefix := getAddressPrefix(address)
	last8 := address[len(address)-8:]
	return prefix + "..." + last8
}

// getAddressPrefix returns the bech32 prefix for known address types.
func getAddressPrefix(address string) string {
	switch {
	case strings.HasPrefix(address, "tellorvalcons"):
		return "tellorvalcons"
	case strings.HasPrefix(address, "tellorvaloper"):
		return "tellorvaloper"
	case strings.HasPrefix(address, "tellor"):
		return "tellor"
	default:
		return ""
	}
}

// ValidatorTree represents the hierarchical validator-reporter-selector tree for display.
// It uses the existing SDK types where possible and adds computed display fields.
type ValidatorTree struct {
	// Validator is the underlying staking validator
	Validator *stakingtypes.Validator
	// Computed display fields
	Moniker         string
	ShortAddress    string
	PowerTRB        string
	Commission      string
	Rewards         string // Rewards earned in lookback period (TRB)
	MissedBlocks    int64  // Number of missed blocks (computed from DB)
	MissedBlocksPct string // Missed-blocks rate as percentage string (e.g. "0.50%")
	Status          string // Active, Degraded, or Jailed
	Jailed          bool   // Whether validator is jailed
	// Nested reporters under this validator
	Reporters    []ReporterTree
	HasReporters bool
	// Delegators lists every staking delegator to this validator, regardless of which
	// reporter (if any) they selected. Populated only for our validator, so a stake
	// delegated to us still shows even when the selector picked a different reporter.
	Delegators    []DelegatorTree
	HasDelegators bool
}

// DelegatorTree is a raw staking delegator to a validator, independent of reporter
// selection. Reporter is the reporter that delegator selected, or "" (none).
type DelegatorTree struct {
	ShortAddress string
	Stake        string
	Reporter     string
}

// ReporterTree represents a reporter under a validator for display.
type ReporterTree struct {
	// Reporter is the underlying reporter data
	Reporter *reportertypes.Reporter
	// Computed display fields
	Moniker        string
	ShortAddress   string
	Commission     string
	Rewards        string // Rewards earned in lookback period (TRB)
	IsSelfSelector bool   // True if reporter is also a selector (self-delegation)
	SelfStake      string // Stake amount when reporter is self-selector
	// Nested selectors under this reporter (excludes self if IsSelfSelector)
	Selectors       []SelectorTree
	TotalStake      string
	MissedCycles    int64  // Number of missed query cycles (computed from DB)
	MissedCyclesPct string // Missed-cycles rate as percentage string (e.g. "0.50%")
	Status          string // Active, Degraded, or Jailed
	Jailed          bool   // Whether reporter is jailed
}

// SelectorTree represents a selector under a reporter for display.
type SelectorTree struct {
	// Selection is the underlying selection data
	Selection *reportertypes.FormattedSelection
	// Computed display fields
	ShortAddress string
	Stake        string
}

// CachedValidatorTree is a JSON-serializable version of ValidatorTree for ClickHouse persistence.
// It only contains the display fields needed for rendering, not the full SDK types.
type CachedValidatorTree struct {
	OperatorAddress string               `json:"operator_address"`
	ValconsAddress  string               `json:"valcons_address"`
	Moniker         string               `json:"moniker"`
	ShortAddress    string               `json:"short_address"`
	PowerTRB        string               `json:"power_trb"`
	Commission      string               `json:"commission"`
	Rewards         string               `json:"rewards"`
	MissedBlocks    int64                `json:"missed_blocks"`
	MissedBlocksPct string               `json:"missed_blocks_pct"`
	Status          string               `json:"status"`
	Jailed          bool                 `json:"jailed"`
	Reporters       []CachedReporterTree `json:"reporters"`
	HasReporters    bool                 `json:"has_reporters"`
	Delegators      []CachedDelegator    `json:"delegators,omitempty"`
	HasDelegators   bool                 `json:"has_delegators,omitempty"`
}

// CachedDelegator is a JSON-serializable staking delegator to a validator, independent of
// reporter selection.
type CachedDelegator struct {
	ShortAddress string `json:"short_address"`
	Stake        string `json:"stake"`
	Reporter     string `json:"reporter"`
}

// CachedReporterTree is a JSON-serializable version of ReporterTree.
type CachedReporterTree struct {
	Address         string               `json:"address"`
	Moniker         string               `json:"moniker"`
	ShortAddress    string               `json:"short_address"`
	Commission      string               `json:"commission"`
	Rewards         string               `json:"rewards"`
	IsSelfSelector  bool                 `json:"is_self_selector"`
	SelfStake       string               `json:"self_stake,omitempty"`
	Selectors       []CachedSelectorTree `json:"selectors"`
	TotalStake      string               `json:"total_stake"`
	MissedCycles    int64                `json:"missed_cycles"`
	MissedCyclesPct string               `json:"missed_cycles_pct"`
	Status          string               `json:"status"`
	Jailed          bool                 `json:"jailed"`
}

// CachedSelectorTree is a JSON-serializable version of SelectorTree.
type CachedSelectorTree struct {
	Address      string `json:"address"`
	ShortAddress string `json:"short_address"`
	Stake        string `json:"stake"`
}

// extractPubkeyFromAny extracts the base64 encoded public key from a protobuf Any type.
// The Any.Value contains the protobuf-encoded pubkey where the first field is the key bytes.
func extractPubkeyFromAny(pubkey *codectypes.Any) string {
	if pubkey == nil || len(pubkey.Value) == 0 {
		return ""
	}
	// The protobuf encoding for ed25519 pubkey is: field 1 (tag 0x0a) + length + key bytes
	// Skip the tag (1 byte) and length (1 byte) to get the raw key bytes
	if len(pubkey.Value) > 2 && pubkey.Value[0] == 0x0a {
		keyLen := int(pubkey.Value[1])
		if len(pubkey.Value) >= 2+keyLen {
			keyBytes := pubkey.Value[2 : 2+keyLen]
			return base64.StdEncoding.EncodeToString(keyBytes)
		}
	}
	return ""
}

// buildValidatorTree builds the hierarchical validator-reporter-selector tree.
// It matches reporters to validators by moniker (most reporters run their own validator).
// Selectors are grouped under their respective reporter on the validator they staked to.
func (s *Server) buildValidatorTree(ctx context.Context) []ValidatorTree {
	// Fetch all validators sorted by power
	validators := s.fetchValidatorsForTree(ctx)
	if len(validators) == 0 {
		s.logger.Error("buildValidatorTree: no validators fetched")
		return nil
	}
	s.logger.Info("buildValidatorTree: validators fetched", "count", len(validators))

	// Fetch all reporters with their full data
	reporterMap := s.fetchReporters(ctx)
	if len(reporterMap) == 0 {
		s.logger.Error("buildValidatorTree: no reporters fetched")
		return nil
	}
	s.logger.Info("buildValidatorTree: reporters fetched", "count", len(reporterMap))

	// Pre-fetch every reporter's selections ONCE, concurrently. Previously
	// fetchSelectionsForReporter ran sequentially and twice per reporter, which
	// dominated page build time (~20s for ~57 reporters).
	selectionsByReporter := s.fetchAllSelectionsConcurrent(ctx, reporterMap)

	// Build a map of validator operator address -> validator index
	validatorIndex := make(map[string]int)
	for i := range validators {
		validatorIndex[validators[i].Validator.OperatorAddress] = i
	}

	// Build a map of validator moniker (lowercase) -> list of validator indices
	// (multiple validators can have the same moniker)
	validatorsByMoniker := make(map[string][]int)
	for i := range validators {
		moniker := strings.ToLower(validators[i].Moniker)
		if moniker != "" {
			validatorsByMoniker[moniker] = append(validatorsByMoniker[moniker], i)
		}
	}

	// Build a map of reporter address -> validator index
	// Priority: 1) moniker match with self-delegation, 2) moniker match only, 3) self-delegation only
	reporterToValidator := make(map[string]int)

	for reporterAddr, reporter := range reporterMap {
		reporterMoniker := ""
		if reporter.Metadata != nil {
			reporterMoniker = strings.ToLower(reporter.Metadata.Moniker)
		}

		// Get all validators the reporter has staked to (from all their selections)
		stakedValidators := make(map[int]bool)
		selections := selectionsByReporter[reporterAddr]
		for _, sel := range selections {
			if len(sel.IndividualDelegations) > 0 {
				for _, del := range sel.IndividualDelegations {
					if valIdx, ok := validatorIndex[del.ValidatorAddress]; ok {
						stakedValidators[valIdx] = true
					}
				}
			} else {
				delegations := s.fetchDelegationsForSelector(ctx, sel.Selector)
				for _, del := range delegations {
					if valIdx, ok := validatorIndex[del.Delegation.ValidatorAddress]; ok {
						stakedValidators[valIdx] = true
					}
				}
			}
		}

		matched := false

		// Priority 0: reporter operates a validator under the same account.
		// The reporter (tellor1…) and validator operator (tellorvaloper1…) share the
		// same underlying bytes, so this is the most reliable link and is moniker-independent.
		if valIdx, ok := validatorIndex[cryptoaddr.ToValidatorOperator(reporterAddr)]; ok {
			reporterToValidator[reporterAddr] = valIdx
			s.logger.Debug("reporter matched by operator address",
				"reporter", reporterAddr, "validator", validators[valIdx].Moniker)
			matched = true
		}

		// Priority 1: Match by moniker
		if !matched && reporterMoniker != "" {
			if matchingValidators, ok := validatorsByMoniker[reporterMoniker]; ok && len(matchingValidators) > 0 {
				// If multiple validators with same moniker, prefer one the reporter staked to
				for _, valIdx := range matchingValidators {
					if stakedValidators[valIdx] {
						reporterToValidator[reporterAddr] = valIdx
						s.logger.Debug("reporter matched by moniker+stake",
							"reporter", reporterAddr, "validator", validators[valIdx].Moniker)
						matched = true
						break
					}
				}
				// If no stake match, use the first validator with matching moniker
				if !matched {
					reporterToValidator[reporterAddr] = matchingValidators[0]
					s.logger.Debug("reporter matched by moniker",
						"reporter", reporterAddr, "validator", validators[matchingValidators[0]].Moniker)
					matched = true
				}
			}
		}

		// Priority 2: Use stake-based matching if no moniker match
		if !matched && len(stakedValidators) > 0 {
			// Use the first staked validator
			for valIdx := range stakedValidators {
				reporterToValidator[reporterAddr] = valIdx
				s.logger.Debug("reporter matched by stake",
					"reporter", reporterAddr, "validator", validators[valIdx].Moniker)
				matched = true
				break
			}
		}

		if !matched {
			s.logger.Debug("reporter has no home validator", "reporter", reporterAddr, "moniker", reporterMoniker)
		}
	}

	// Now build the tree: each reporter goes under their matched validator
	// All selectors for a reporter are shown under that reporter (with total stake across all validators)
	ourValidatorAddr := cryptoaddr.ToValidatorOperator(s.cfg.WalletAddress)
	ourReporterAddr := s.cfg.WalletAddress // Reporter address is same as wallet address

	for reporterAddr, reporter := range reporterMap {
		// Get the reporter's home validator
		homeValIdx, hasHomeVal := reporterToValidator[reporterAddr]
		if !hasHomeVal {
			// Reporter has no home validator - skip for now
			// (could add them under a separate "unmatched" section later)
			s.logger.Debug("reporter has no home validator", "reporter", reporterAddr)
			continue
		}

		// For our validator, only show our reporter (skip others with same moniker)
		homeValidator := &validators[homeValIdx]
		if homeValidator.Validator != nil && homeValidator.Validator.OperatorAddress == ourValidatorAddr {
			if reporterAddr != ourReporterAddr {
				s.logger.Debug("skipping non-our reporter under our validator", "reporter", reporterAddr)
				continue
			}
		}

		// Create reporter node under their home validator
		reporterNode := s.findOrCreateReporterTree(homeValidator, reporter)

		// Fetch all selectors for this reporter
		selections := selectionsByReporter[reporterAddr]
		for _, sel := range selections {
			// Calculate total stake for this selector across all validators
			var totalStake uint64
			if len(sel.IndividualDelegations) > 0 {
				for _, del := range sel.IndividualDelegations {
					totalStake += del.Amount.Uint64()
				}
			} else {
				delegations := s.fetchDelegationsForSelector(ctx, sel.Selector)
				for _, del := range delegations {
					totalStake += del.Balance.Amount.Uint64()
				}
			}

			// Add selector to reporter (once, with total stake)
			selectorNode := SelectorTree{
				Selection:    sel,
				ShortAddress: truncateAddress(sel.Selector),
				Stake:        formatLoya(totalStake),
			}
			reporterNode.Selectors = append(reporterNode.Selectors, selectorNode)
		}
	}

	// Get missed cycles per reporter and missed blocks per validator from DB (lookback period)
	missedCyclesMap, totalCycles := s.getMissedCyclesPerReporterFromDB(ctx)
	missedBlocksMap, totalBlocks := s.getMissedBlocksPerValidatorFromDB(ctx, s.cfg.LookbackPeriodDays)

	// Calculate total stake per reporter, detect self-selectors, set HasReporters flag, and populate metrics
	for i := range validators {
		validators[i].HasReporters = len(validators[i].Reporters) > 0

		// Set validator jailed status from SDK data
		if validators[i].Validator != nil {
			validators[i].Jailed = validators[i].Validator.Jailed
		}

		// For our validator, list its direct Delegations: delegators to the validator that
		// did NOT select our reporter. Those that did appear under our reporter's own
		// Delegations, so listing them here too would duplicate. This surfaces stake
		// delegated to us that selected a different reporter (or none), e.g. a test wallet.
		if validators[i].Validator != nil && validators[i].Validator.OperatorAddress == ourValidatorAddr {
			for _, d := range s.fetchValidatorDelegators(ctx, ourValidatorAddr) {
				addr := d.Delegation.DelegatorAddress
				rep := s.fetchSelectorReporter(ctx, addr)
				if rep == ourReporterAddr {
					continue
				}
				repLabel := "none"
				if rep != "" {
					repLabel = truncateAddress(rep)
				}
				validators[i].Delegators = append(validators[i].Delegators, DelegatorTree{
					ShortAddress: truncateAddress(addr),
					Stake:        formatLoya(d.Balance.Amount.Uint64()),
					Reporter:     repLabel,
				})
			}
			validators[i].HasDelegators = len(validators[i].Delegators) > 0
		}

		// Populate missed blocks for validator from Prometheus metrics
		// The metric uses the consensus address (tellorvalcons...), so we need to convert
		if validators[i].Validator != nil && validators[i].Validator.ConsensusPubkey != nil {
			// Extract pubkey and convert to consensus address
			pubkeyBase64 := extractPubkeyFromAny(validators[i].Validator.ConsensusPubkey)
			if pubkeyBase64 != "" {
				if valconsAddr, err := cryptoaddr.ToValcons(pubkeyBase64); err == nil {
					if missedBlocks, ok := missedBlocksMap[valconsAddr]; ok {
						validators[i].MissedBlocks = missedBlocks
					}
				}
			}
		}
		validators[i].MissedBlocksPct = formatMissedPct(validators[i].MissedBlocks, totalBlocks)

		for j := range validators[i].Reporters {
			reporterAddr := ""
			if validators[i].Reporters[j].Reporter != nil {
				reporterAddr = validators[i].Reporters[j].Reporter.Address
				// Set reporter jailed status from SDK data
				if validators[i].Reporters[j].Reporter.Metadata != nil {
					validators[i].Reporters[j].Jailed = validators[i].Reporters[j].Reporter.Metadata.Jailed
				}
			}

			// Separate self-selector from other selectors
			var totalStake uint64
			var filteredSelectors []SelectorTree
			for _, sel := range validators[i].Reporters[j].Selectors {
				stakeAmount := parseStakeTRB(sel.Stake)
				totalStake += stakeAmount

				// Check if this selector is the reporter itself (self-delegation)
				selectorAddr := ""
				if sel.Selection != nil {
					selectorAddr = sel.Selection.Selector
				}
				if selectorAddr != "" && selectorAddr == reporterAddr {
					validators[i].Reporters[j].IsSelfSelector = true
					validators[i].Reporters[j].SelfStake = sel.Stake
				} else {
					filteredSelectors = append(filteredSelectors, sel)
				}
			}
			validators[i].Reporters[j].Selectors = filteredSelectors
			validators[i].Reporters[j].TotalStake = formatLoya(totalStake)

			// Populate missed cycles from Prometheus metrics
			if reporterAddr != "" {
				if missedCycles, ok := missedCyclesMap[reporterAddr]; ok {
					validators[i].Reporters[j].MissedCycles = missedCycles
				} else {
					// Reporter submitted no reports in the lookback period - missed all cycles.
					validators[i].Reporters[j].MissedCycles = totalCycles
				}
				validators[i].Reporters[j].MissedCyclesPct = formatMissedPct(validators[i].Reporters[j].MissedCycles, totalCycles)
			} else {
				validators[i].Reporters[j].MissedCyclesPct = "-"
			}
			// Fallback: if this is our reporter under our validator and SelfStake is 0,
			// use validator power as the stake (selection data may be stale after redelegate)
			isOurValidator := validators[i].Validator != nil && validators[i].Validator.OperatorAddress == ourValidatorAddr
			isOurReporter := reporterAddr == ourReporterAddr
			selfStakeIsZero := validators[i].Reporters[j].SelfStake == "" || validators[i].Reporters[j].SelfStake == "0.00"

			if isOurValidator && isOurReporter && validators[i].Reporters[j].IsSelfSelector && selfStakeIsZero {
				// Use validator power as fallback (without " TRB" suffix, template adds it)
				validators[i].Reporters[j].SelfStake = validators[i].PowerTRB
				s.logger.Info("using validator power as reporter stake fallback",
					"reporter", reporterAddr, "stake", validators[i].PowerTRB)
			}

			// Compute reporter status: Jailed > Degraded > Active
			validators[i].Reporters[j].Status = computeStatus(validators[i].Reporters[j].Jailed, validators[i].Reporters[j].MissedCycles)
		}

		// Compute validator status: Jailed > Degraded > Active
		validators[i].Status = computeStatus(validators[i].Jailed, validators[i].MissedBlocks)
	}

	// Return top 10 validators, but always include our own validator
	// (ourValidatorAddr already defined above)

	if len(validators) > 10 {
		// Check if our validator is in top 10
		ourValInTop10 := false
		var ourValidator *ValidatorTree
		for i := 0; i < 10; i++ {
			if validators[i].Validator != nil && validators[i].Validator.OperatorAddress == ourValidatorAddr {
				ourValInTop10 = true
				break
			}
		}

		// Find and save our validator if not in top 10
		if !ourValInTop10 {
			for i := 10; i < len(validators); i++ {
				if validators[i].Validator != nil && validators[i].Validator.OperatorAddress == ourValidatorAddr {
					// Copy the value (not pointer) since we'll slice the array
					v := validators[i]
					ourValidator = &v
					break
				}
			}
		}

		// Take top 10
		validators = validators[:10]

		// If our validator was found outside top 10, append it
		if ourValidator != nil {
			s.logger.Info("including our validator outside top 10", "address", ourValidatorAddr)
			validators = append(validators, *ourValidator)
		}
	}

	// Populate rewards data for validators and reporters in batch
	s.populateRewardsData(ctx, validators)

	return validators
}

// populateRewardsData fetches and populates rewards for all validators and reporters in the tree.
func (s *Server) populateRewardsData(ctx context.Context, validators []ValidatorTree) {
	s.populateRewardsDataForPeriod(ctx, validators, s.cfg.LookbackPeriodDays)
}

// populateRewardsDataForPeriod fetches and populates rewards using a specific period.
func (s *Server) populateRewardsDataForPeriod(ctx context.Context, validators []ValidatorTree, periodDays int) {
	if s.db == nil {
		return
	}

	validatorAddrs := s.collectValidatorAddresses(validators)
	reporterAddrs := s.collectReporterAddresses(validators)

	validatorRewards := s.getValidatorRewardsMap(ctx, validatorAddrs, periodDays)
	reporterRewards := s.getReporterRewardsMap(ctx, reporterAddrs, periodDays)

	s.setValidatorRewards(validators, validatorRewards)
	s.setReporterRewards(validators, reporterRewards)
}

func (s *Server) collectValidatorAddresses(validators []ValidatorTree) []string {
	addrs := make([]string, 0, len(validators))
	for i := range validators {
		if validators[i].Validator == nil {
			continue
		}
		addrs = append(addrs, validators[i].Validator.OperatorAddress)
	}
	return addrs
}

func (s *Server) collectReporterAddresses(validators []ValidatorTree) []string {
	addrs := make([]string, 0)
	for i := range validators {
		for j := range validators[i].Reporters {
			if validators[i].Reporters[j].Reporter == nil {
				continue
			}
			addrs = append(addrs, validators[i].Reporters[j].Reporter.Address)
		}
	}
	return addrs
}

func (s *Server) setValidatorRewards(validators []ValidatorTree, rewards map[string]uint64) {
	for i := range validators {
		if validators[i].Validator == nil {
			continue
		}

		operatorAddr := validators[i].Validator.OperatorAddress
		rewardsLoya, ok := rewards[operatorAddr]
		if ok {
			validators[i].Rewards = formatLoya(rewardsLoya)
			continue
		}
		validators[i].Rewards = valueZero
	}
}

func (s *Server) setReporterRewards(validators []ValidatorTree, rewards map[string]uint64) {
	for i := range validators {
		for j := range validators[i].Reporters {
			if validators[i].Reporters[j].Reporter == nil {
				continue
			}

			reporterAddr := validators[i].Reporters[j].Reporter.Address
			rewardsLoya, ok := rewards[reporterAddr]
			if ok {
				validators[i].Reporters[j].Rewards = formatLoya(rewardsLoya)
				continue
			}
			validators[i].Reporters[j].Rewards = valueZero
		}
	}
}

// findOrCreateReporterTree finds an existing reporter node or creates a new one.
func (s *Server) findOrCreateReporterTree(validator *ValidatorTree, reporter *reportertypes.Reporter) *ReporterTree {
	for i := range validator.Reporters {
		if validator.Reporters[i].Reporter.Address == reporter.Address {
			return &validator.Reporters[i]
		}
	}

	// Create new reporter node
	moniker := ""
	commission := "0%"
	if reporter.Metadata != nil {
		moniker = reporter.Metadata.Moniker
		if !reporter.Metadata.CommissionRate.IsNil() {
			pct := reporter.Metadata.CommissionRate.MulInt64(100)
			commission = fmt.Sprintf("%.0f%%", pct.MustFloat64())
		}
	}

	reporterTree := ReporterTree{
		Reporter:     reporter,
		Moniker:      moniker,
		ShortAddress: truncateAddress(reporter.Address),
		Commission:   commission,
		Selectors:    []SelectorTree{},
	}
	validator.Reporters = append(validator.Reporters, reporterTree)
	return &validator.Reporters[len(validator.Reporters)-1]
}

// parseStakeTRB parses a TRB stake string back to uint64 loya (approximate).
func parseStakeTRB(stake string) uint64 {
	var trb float64
	if _, err := fmt.Sscanf(stake, "%f", &trb); err != nil {
		return 0
	}
	return uint64(trb * monitor.LoyaPerTRB)
}

// fetchValidatorsForTree fetches validators and returns them as ValidatorTree structs.
func (s *Server) fetchValidatorsForTree(ctx context.Context) []ValidatorTree {
	var validators []ValidatorTree

	s.logger.Info("fetchValidatorsForTree starting", "apiURLCount", len(s.cfg.LayerAPIURLs))

	for _, baseURL := range s.cfg.LayerAPIURLs {
		url := fmt.Sprintf("%s/cosmos/staking/v1beta1/validators?pagination.limit=100", baseURL)
		s.logger.Info("fetchValidatorsForTree trying", "url", url)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			s.logger.Debug("fetchValidatorsForTree request error", "node", baseURL, "error", err)
			continue
		}

		resp, err := s.httpClient.Do(req)
		if err != nil {
			s.logger.Debug("fetchValidatorsForTree fetch error", "node", baseURL, "error", err)
			continue
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			s.logger.Debug("fetchValidatorsForTree status error", "node", baseURL, "status", resp.StatusCode)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			s.logger.Debug("fetchValidatorsForTree read error", "node", baseURL, "error", err)
			continue
		}

		var result stakingtypes.QueryValidatorsResponse
		if err := s.cdc.UnmarshalJSON(body, &result); err != nil {
			s.logger.Error("fetchValidatorsForTree unmarshal error", "node", baseURL, "error", err, "bodyLen", len(body))
			continue
		}

		s.logger.Info("fetchValidatorsForTree response", "node", baseURL, "validatorCount", len(result.Validators))

		// Get our validator address to always include it even if jailed
		ourValidatorAddr := cryptoaddr.ToValidatorOperator(s.cfg.WalletAddress)

		for i := range result.Validators {
			v := &result.Validators[i]
			s.logger.Debug("fetchValidatorsForTree processing validator", "i", i, "operator", v.OperatorAddress, "moniker", v.Description.Moniker, "jailed", v.Jailed)

			// Skip jailed validators, except our own
			isOurValidator := v.OperatorAddress == ourValidatorAddr
			if v.Jailed && !isOurValidator {
				s.logger.Debug("fetchValidatorsForTree skipping jailed validator", "operator", v.OperatorAddress, "moniker", v.Description.Moniker)
				continue
			}

			commission := "0%"
			if v.Commission.Rate.IsPositive() {
				pct := v.Commission.Rate.MulInt64(100)
				commission = fmt.Sprintf("%.0f%%", pct.MustFloat64())
			}

			validators = append(validators, ValidatorTree{
				Validator:    v,
				Moniker:      v.Description.Moniker,
				ShortAddress: truncateAddress(v.OperatorAddress),
				PowerTRB:     formatLoya(v.Tokens.Uint64()),
				Commission:   commission,
				Reporters:    []ReporterTree{},
			})
		}

		if len(validators) > 0 {
			// Sort by power descending
			sort.Slice(validators, func(i, j int) bool {
				return validators[i].Validator.Tokens.GT(validators[j].Validator.Tokens)
			})
			s.logger.Info("validators for tree fetched", "count", len(validators))
			return validators
		}

		s.logger.Warn("fetchValidatorsForTree no validators from response", "node", baseURL, "resultValidatorsLen", len(result.Validators))
	}

	s.logger.Error("fetchValidatorsForTree returning empty", "triedNodes", len(s.cfg.LayerAPIURLs))
	return validators
}

// fetchReporters fetches all reporters and returns them as a map.
func (s *Server) fetchReporters(ctx context.Context) map[string]*reportertypes.Reporter {
	reporters := make(map[string]*reportertypes.Reporter)

	for _, baseURL := range s.cfg.LayerAPIURLs {
		url := fmt.Sprintf("%s/tellor-io/layer/reporter/reporters?pagination.limit=500", baseURL)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			s.logger.Debug("fetchReporters request error", "node", baseURL, "error", err)
			continue
		}

		resp, err := s.httpClient.Do(req)
		if err != nil {
			s.logger.Debug("fetchReporters fetch error", "node", baseURL, "error", err)
			continue
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			s.logger.Debug("fetchReporters status error", "node", baseURL, "status", resp.StatusCode)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			s.logger.Debug("fetchReporters read error", "node", baseURL, "error", err)
			continue
		}

		var result reportertypes.QueryReportersResponse
		if err := s.cdc.UnmarshalJSON(body, &result); err != nil {
			s.logger.Error("fetchReporters unmarshal error", "node", baseURL, "error", err, "bodyLen", len(body))
			continue
		}

		s.logger.Info("fetchReporters response", "node", baseURL, "reporterCount", len(result.Reporters))

		for _, r := range result.Reporters {
			if r != nil && r.Address != "" {
				reporters[r.Address] = r
			}
		}

		if len(reporters) > 0 {
			s.logger.Info("reporters fetched", "count", len(reporters))
			return reporters
		}
	}

	return reporters
}

// fetchAllSelectionsConcurrent fetches selections for every reporter in parallel
// (bounded worker pool) and returns them keyed by reporter address. This replaces
// the previous sequential, twice-per-reporter fetching that dominated build time.
func (s *Server) fetchAllSelectionsConcurrent(ctx context.Context, reporterMap map[string]*reportertypes.Reporter) map[string][]*reportertypes.FormattedSelection {
	result := make(map[string][]*reportertypes.FormattedSelection, len(reporterMap))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16) // cap concurrent chain queries
	for reporterAddr := range reporterMap {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			sels := s.fetchSelectionsForReporter(ctx, addr)
			mu.Lock()
			result[addr] = sels
			mu.Unlock()
		}(reporterAddr)
	}
	wg.Wait()
	return result
}

// fetchSelectionsForReporter fetches all selectors for a reporter.
// Returns FormattedSelection from the reporter module directly.
func (s *Server) fetchSelectionsForReporter(ctx context.Context, reporterAddr string) []*reportertypes.FormattedSelection {
	var selections []*reportertypes.FormattedSelection

	for _, baseURL := range s.cfg.LayerAPIURLs {
		url := fmt.Sprintf("%s/tellor-io/layer/reporter/selections-to/%s", baseURL, reporterAddr)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			continue
		}

		resp, err := s.httpClient.Do(req)
		if err != nil {
			continue
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			continue
		}

		var result reportertypes.QuerySelectionsToResponse
		if err := unmarshalSelectionsJSON(body, &result); err != nil {
			s.logger.Debug("selections decode error", "reporter", reporterAddr, "error", err)
			continue
		}

		selections = append(selections, result.Selections...)

		if len(selections) > 0 || result.Reporter != "" {
			return selections
		}
	}

	return selections
}

// unmarshalSelectionsJSON tolerates response fields added by newer Layer versions.
// The monitor only reads concrete reporter types here, so no interface unpacking is needed.
func unmarshalSelectionsJSON(body []byte, result *reportertypes.QuerySelectionsToResponse) error {
	unmarshaler := jsonpb.Unmarshaler{AllowUnknownFields: true}
	return unmarshaler.Unmarshal(strings.NewReader(string(body)), result)
}

// fetchDelegationsForSelector fetches all delegations for a selector.
// Returns DelegationResponse from staking types directly.
func (s *Server) fetchDelegationsForSelector(ctx context.Context, selectorAddr string) []stakingtypes.DelegationResponse {
	var delegations []stakingtypes.DelegationResponse

	for _, baseURL := range s.cfg.LayerAPIURLs {
		url := fmt.Sprintf("%s/cosmos/staking/v1beta1/delegations/%s", baseURL, selectorAddr)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			continue
		}

		resp, err := s.httpClient.Do(req)
		if err != nil {
			continue
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			continue
		}

		var result stakingtypes.QueryDelegatorDelegationsResponse
		if err := s.cdc.UnmarshalJSON(body, &result); err != nil {
			continue
		}

		for _, d := range result.DelegationResponses {
			if d.Balance.Denom == "loya" {
				delegations = append(delegations, d)
			}
		}

		if len(delegations) > 0 {
			return delegations
		}
	}

	return delegations
}

// fetchValidatorDelegators returns every staking delegator to a validator (loya balances),
// using the same failover-over-LayerAPIURLs pattern as the other staking queries.
func (s *Server) fetchValidatorDelegators(ctx context.Context, valoper string) []stakingtypes.DelegationResponse {
	var out []stakingtypes.DelegationResponse
	for _, baseURL := range s.cfg.LayerAPIURLs {
		url := fmt.Sprintf("%s/cosmos/staking/v1beta1/validators/%s/delegations?pagination.limit=1000", baseURL, valoper)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			continue
		}
		resp, err := s.httpClient.Do(req)
		if err != nil {
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil || resp.StatusCode != http.StatusOK {
			continue
		}
		var result stakingtypes.QueryValidatorDelegationsResponse
		if err := s.cdc.UnmarshalJSON(body, &result); err != nil {
			continue
		}
		for _, d := range result.DelegationResponses {
			if d.Balance.Denom == "loya" {
				out = append(out, d)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return out
}

// fetchSelectorReporter returns the reporter a selector has selected, or "" if none.
func (s *Server) fetchSelectorReporter(ctx context.Context, selector string) string {
	for _, baseURL := range s.cfg.LayerAPIURLs {
		url := fmt.Sprintf("%s/tellor-io/layer/reporter/selector-reporter/%s", baseURL, selector)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			continue
		}
		resp, err := s.httpClient.Do(req)
		if err != nil {
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil || resp.StatusCode != http.StatusOK {
			continue
		}
		var out struct {
			Reporter string `json:"reporter"`
		}
		if err := json.Unmarshal(body, &out); err == nil && out.Reporter != "" {
			return out.Reporter
		}
	}
	return ""
}

// refreshTreeCache rebuilds the validator tree cache in-memory.
func (s *Server) refreshTreeCache(ctx context.Context) ([]ValidatorTree, time.Time) {
	s.cacheMu.Lock()
	// Double-check after acquiring write lock
	if s.cacheRefreshing {
		// Another goroutine is already refreshing, return stale data
		tree := s.cachedTree
		timestamp := s.cacheTimestamp
		s.cacheMu.Unlock()
		return tree, timestamp
	}
	s.cacheRefreshing = true
	s.cacheMu.Unlock()

	// Build the tree (this can take a while)
	startTime := time.Now()
	tree := s.buildValidatorTree(ctx)

	// Pre-compute the network reward stats for each selectable period so page loads serve
	// them from cache instead of running the (uncached, unindexed) rewards scans per request.
	rewardStats := make(map[int]NetworkRewardStats, 3)
	for _, p := range []int{1, 7, 30} {
		rewardStats[p] = s.queryNetworkRewardStats(ctx, p)
	}
	duration := time.Since(startTime)

	s.logger.Info("validator tree cache refreshed", "duration", duration, "validators", len(tree))

	// Update in-memory cache
	s.cacheMu.Lock()
	s.cachedTree = tree
	s.cachedRewardStats = rewardStats
	s.cacheTimestamp = time.Now()
	s.cacheRefreshing = false
	timestamp := s.cacheTimestamp
	s.cacheMu.Unlock()

	return tree, timestamp
}

// handleRefreshTree handles the cache refresh API endpoint.
func (s *Server) handleRefreshTree(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	tree, timestamp := s.refreshTreeCache(ctx)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success":    true,
		"timestamp":  timestamp.Format(time.RFC3339),
		"validators": len(tree),
	})
}

func (s *Server) getMissedCyclesPerReporterFromDB(ctx context.Context) (map[string]int64, int64) {
	return s.getMissedCyclesPerReporterFromDBForPeriod(ctx, s.cfg.LookbackPeriodDays)
}

// getMissedCyclesPerReporterFromDBForPeriod queries missed reports from DB for all
// reporters in the provided period window.
// Returns a map of reporter address -> missed cycles count.
//
// Simple calculation: Missed = Total Cycles - Report Count
// This shows how many cycles the reporter didn't submit a report.
func (s *Server) getMissedCyclesPerReporterFromDBForPeriod(ctx context.Context, periodDays int) (map[string]int64, int64) {
	result := make(map[string]int64)
	if periodDays <= 0 {
		periodDays = s.cfg.LookbackPeriodDays
	}
	if periodDays <= 0 {
		periodDays = DefaultLookbackPeriodDays
	}
	minutes := periodDays * 24 * 60

	// Get total cycle rotations in lookback period
	cyclesQuery := fmt.Sprintf(`
		SELECT COUNT(*) as total_cycles
		FROM %s
		WHERE %s >= now() - INTERVAL %d MINUTE
	`, blockdb.TableNameCycleRotations, blockdb.ColTimestamp, minutes)

	var totalCycles int64
	rows, err := s.db.Query(ctx, cyclesQuery)
	if err != nil {
		s.logger.Debug("failed to query cycle rotations for map", "error", err)
		return result, 0
	}
	if rows.Next() {
		_ = rows.Scan(&totalCycles)
	}
	_ = rows.Close()

	if totalCycles == 0 {
		return result, 0
	}

	// Get reports count per reporter (only cyclelist reports)
	reportsQuery := fmt.Sprintf(`
		SELECT %s, COUNT(*) as report_count
		FROM %s
		WHERE %s >= now() - INTERVAL %d MINUTE
		  AND %s = 1
		GROUP BY %s
	`, blockdb.ColReporter, blockdb.TableNameReports,
		blockdb.ColTimestamp, minutes, blockdb.ColCyclelist, blockdb.ColReporter)

	rows, err = s.db.Query(ctx, reportsQuery)
	if err != nil {
		s.logger.Debug("failed to query reports count for map", "error", err)
		return result, totalCycles
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var reporter string
		var reportCount int64
		if err := rows.Scan(&reporter, &reportCount); err != nil {
			continue
		}

		// Simple calculation: missed = total cycles - reports submitted
		missed := totalCycles - reportCount
		if missed < 0 {
			missed = 0
		}

		result[reporter] = missed
	}

	return result, totalCycles
}

// getMissedBlocksPerValidatorFromDB queries missed blocks from DB for all validators (lookback period).
// Returns a map of validator consensus address -> missed blocks count.
//
// A block counts as missed whenever the validator did not sign it — INCLUDING
// blocks where it was jailed or otherwise out of the active set (it produces no
// block_signs row then). We therefore compute:
//
//	missed = total_distinct_blocks_in_range - distinct_blocks_the_validator_signed
//
// This counts every non-signed block uniformly for all validators (no special
// case), and captures jailed/out-of-set periods that signed=0 rows alone miss.
// Also returns the total distinct blocks seen in the lookback period (used to
// compute miss percentage on the status page).
//
// Note: a validator that was jailed/absent for the ENTIRE period has no rows at
// all, so it does not appear here; the caller treats a missing entry as 0. That
// only affects validators with zero activity in the whole window.
func (s *Server) getMissedBlocksPerValidatorFromDB(ctx context.Context, periodDays int) (map[string]int64, int64) {
	result := make(map[string]int64)

	// Total distinct blocks in lookback period - used as the denominator and as
	// the baseline every validator is expected to have signed.
	totalQuery := fmt.Sprintf(`
		SELECT COUNT(DISTINCT %s)
		FROM %s
		WHERE %s >= now() - INTERVAL %d DAY AND %s = '%s'
	`, blockdb.ColBlockHeight, blockdb.TableNameBlockSigns, blockdb.ColBlockTimestamp, periodDays,
		blockdb.ColSigType, blockdb.SigTypeConsensus)
	var totalBlocks int64
	if trows, err := s.db.Query(ctx, totalQuery); err == nil {
		if trows.Next() {
			_ = trows.Scan(&totalBlocks)
		}
		_ = trows.Close()
	} else {
		s.logger.Debug("failed to query total blocks", "error", err)
	}
	if totalBlocks == 0 {
		return result, 0
	}

	// Count the distinct blocks each validator actually signed (signed = 1).
	// missed = total - signed, so jailed/out-of-set blocks (no row) count as missed.
	query := fmt.Sprintf(`
		SELECT
			%s,
			COUNT(DISTINCT %s) AS signed_blocks
		FROM %s
		WHERE %s >= now() - INTERVAL %d DAY AND %s = 1 AND %s = '%s'
		GROUP BY %s
	`,
		blockdb.ColValidatorAddress,
		blockdb.ColBlockHeight,
		blockdb.TableNameBlockSigns,
		blockdb.ColBlockTimestamp, periodDays, blockdb.ColSigned,
		blockdb.ColSigType, blockdb.SigTypeConsensus,
		blockdb.ColValidatorAddress,
	)

	rows, err := s.db.Query(ctx, query)
	if err != nil {
		s.logger.Debug("failed to query missed blocks for map", "error", err)
		return result, totalBlocks
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var validatorAddr string
		var signedBlocks int64
		if err := rows.Scan(&validatorAddr, &signedBlocks); err != nil {
			continue
		}
		missed := totalBlocks - signedBlocks
		if missed < 0 {
			missed = 0
		}
		result[validatorAddr] = missed
	}

	return result, totalBlocks
}

// toCachedTree converts a ValidatorTree slice to CachedValidatorTree slice for JSON serialization.
func toCachedTree(tree []ValidatorTree) []CachedValidatorTree {
	cached := make([]CachedValidatorTree, len(tree))
	for i, v := range tree {
		cached[i] = CachedValidatorTree{
			OperatorAddress: "",
			Moniker:         v.Moniker,
			ShortAddress:    v.ShortAddress,
			PowerTRB:        v.PowerTRB,
			Commission:      v.Commission,
			Rewards:         v.Rewards,
			MissedBlocks:    v.MissedBlocks,
			MissedBlocksPct: v.MissedBlocksPct,
			Status:          v.Status,
			Jailed:          v.Jailed,
			HasReporters:    v.HasReporters,
			HasDelegators:   v.HasDelegators,
			Reporters:       make([]CachedReporterTree, len(v.Reporters)),
		}
		for _, d := range v.Delegators {
			cached[i].Delegators = append(cached[i].Delegators, CachedDelegator(d))
		}
		if v.Validator != nil {
			cached[i].OperatorAddress = v.Validator.OperatorAddress
			if v.Validator.ConsensusPubkey != nil {
				pubkeyBase64 := extractPubkeyFromAny(v.Validator.ConsensusPubkey)
				if pubkeyBase64 != "" {
					if valconsAddr, err := cryptoaddr.ToValcons(pubkeyBase64); err == nil {
						cached[i].ValconsAddress = valconsAddr
					}
				}
			}
		}
		for j, r := range v.Reporters {
			cached[i].Reporters[j] = CachedReporterTree{
				Address:         "",
				Moniker:         r.Moniker,
				ShortAddress:    r.ShortAddress,
				Commission:      r.Commission,
				Rewards:         r.Rewards,
				IsSelfSelector:  r.IsSelfSelector,
				SelfStake:       r.SelfStake,
				TotalStake:      r.TotalStake,
				MissedCycles:    r.MissedCycles,
				MissedCyclesPct: r.MissedCyclesPct,
				Status:          r.Status,
				Jailed:          r.Jailed,
				Selectors:       make([]CachedSelectorTree, len(r.Selectors)),
			}
			if r.Reporter != nil {
				cached[i].Reporters[j].Address = r.Reporter.Address
			}
			for k, s := range r.Selectors {
				cached[i].Reporters[j].Selectors[k] = CachedSelectorTree{
					Address:      "",
					ShortAddress: s.ShortAddress,
					Stake:        s.Stake,
				}
				if s.Selection != nil {
					cached[i].Reporters[j].Selectors[k].Address = s.Selection.Selector
				}
			}
		}
	}
	return cached
}
