package trader

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2"
	cryptoriums "github.com/cryptoriums/layer-monitor/metrics"
	bigpkg "github.com/cryptoriums/packages/big"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"cosmossdk.io/log"

	sdk "github.com/cosmos/cosmos-sdk/types"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
)

const (
	componentName       = "trader"
	minimumTradeBalance = 0.5
)

// Config controls reporter access and execution behavior.
type Config struct {
	Reporter      sdk.AccAddress        `json:"reporter" yaml:"reporter"`
	BankClient    banktypes.QueryClient `json:"-" yaml:"-"`
	ApiKey        string
	SecretKey     string
	BinanceAPIURL string
}

// Trader executes trades and tracks balances for the configured reporter.
type Trader struct {
	logger             log.Logger
	trader             *binance.Client
	btcConverted       *prometheus.CounterVec
	tokenBalanceTrader *prometheus.GaugeVec

	ctx        context.Context
	reporter   sdk.AccAddress
	bankClient banktypes.QueryClient

	mu              sync.Mutex
	accumulatedTRB  float64
	accumulatedUSDC float64
}

func New(
	ctx context.Context,
	logger log.Logger,
	reg prometheus.Registerer,
	cfg Config,
) (*Trader, error) {
	logger = logger.With("component", componentName)

	if cfg.Reporter.Empty() {
		return nil, fmt.Errorf("trader reporter must be set")
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	bclient := binance.NewClient(cfg.ApiKey, cfg.SecretKey)
	if cfg.BinanceAPIURL != "" {
		bclient.BaseURL = cfg.BinanceAPIURL
	}
	_, err := bclient.NewGetAccountService().Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("binance client initialization:%w", err)
		// invalid key/secret or permission issue (look at err.Error())
	}

	trader := &Trader{
		logger:     logger,
		trader:     bclient,
		ctx:        ctx,
		reporter:   cfg.Reporter,
		bankClient: cfg.BankClient,
		btcConverted: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: cryptoriums.MetricsNamespace,
			Subsystem: componentName,
			Name:      "btc_converted",
			Help:      "The total BTC forwarded to the reporter",
		}, []string{"reporter"}),
		tokenBalanceTrader: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: cryptoriums.MetricsNamespace,
			Subsystem: componentName,
			Name:      "balance_trader",
			Help:      "The current trader token balances",
		}, []string{"token"}),
	}

	go trader.awaitShutdown(ctx)

	return trader, nil
}

// HandleReward executes the trading flow when the reward belongs to the configured reporter.
func (m *Trader) HandleReward(reporter sdk.AccAddress, amount *big.Int) {
	if amount == nil {
		cryptoriums.IncError("nil_amount", componentName)
		m.logger.Error("received nil reward amount")
		return
	}
	if !reporter.Equals(m.reporter) {
		return
	}

	go func() {
		tradeCtx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
		defer cancel()
		if err := m.performTrade(tradeCtx, amount, false); err != nil {
			switch {
			case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
				cryptoriums.IncError("timeout", componentName)
				m.logger.Error("trade timed out", "err", err)
			default:
				cryptoriums.IncError("trade_execution", componentName)
				m.logger.Error("failed executing trade", "err", err)
			}
		}
	}()
}

// Run periodically diffs the reporter's loya balance and forwards any increase
// to performTrade so it can convert earned TRB rewards into BTC on Binance.
// It blocks until ctx is canceled.
func (m *Trader) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	const checkInterval = 10 * time.Minute
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	var prevBalance *big.Int

	check := func() {
		qCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		resp, err := m.bankClient.Balance(qCtx, &banktypes.QueryBalanceRequest{
			Address: m.reporter.String(),
			Denom:   "loya",
		})
		if err != nil {
			m.logger.Error("failed to query loya balance", "error", err)
			return
		}

		current := resp.Balance.Amount.BigInt()
		if prevBalance != nil && current.Cmp(prevBalance) > 0 {
			increase := new(big.Int).Sub(current, prevBalance)
			m.HandleReward(m.reporter, increase)
		}
		prevBalance = current
	}

	// Seed the initial balance so the first real diff is accurate.
	check()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			check()
		}
	}
}

// forceFlush drains accumulated balances regardless of thresholds.
func (m *Trader) forceFlush() error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(m.ctx), 5*time.Second)
	defer cancel()
	err := m.performTrade(ctx, big.NewInt(0), true)
	if err != nil {
		cryptoriums.IncError("force_flush", componentName)
	}
	return err
}

func (m *Trader) awaitShutdown(ctx context.Context) {
	<-ctx.Done()
	if err := m.forceFlush(); err != nil {
		m.logger.Error("force flush on shutdown", "err", err)
	}
}

func (m *Trader) performTrade(ctx context.Context, trbToConvertB *big.Int, force bool) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	defer func() {
		if balErr := m.refreshBalances(ctx); balErr != nil {
			m.logger.Error("updating trader balances", "err", balErr)
			cryptoriums.IncError("balance_refresh", componentName)
		}
	}()

	trbToConvert := bigpkg.ToFloatDiv(trbToConvertB, 1e18)
	if trbToConvert > 300 {
		return fmt.Errorf("unexpected value for a trade TRB:%v", trbToConvert)
	}

	m.accumulatedTRB += trbToConvert
	if m.accumulatedTRB < minimumTradeBalance && !force {
		m.logger.Info("not enough accumulated balance", "balance", m.accumulatedTRB)
		return nil
	}

	q := fmt.Sprintf("%.2f", m.accumulatedTRB)
	order, err := m.trader.NewCreateOrderService().
		Symbol("TRBUSDC").
		Side(binance.SideTypeSell).
		Type(binance.OrderTypeMarket).
		Quantity(q).
		Do(ctx)
	if err != nil {
		return fmt.Errorf("executing TRBUSDC trade accumulated TRB:%v: %w", q, err)
	}

	m.accumulatedTRB = 0

	cumulativeQuoteQuantity, err := strconv.ParseFloat(order.CummulativeQuoteQuantity, 64)
	if err != nil {
		return fmt.Errorf("parse CummulativeQuoteQuantity: %w", err)
	}

	m.accumulatedUSDC += cumulativeQuoteQuantity // Use accumulatedUSDC so that if the next trade fails it is executed on the next run.

	m.logger.Info(
		"executed order",
		"symbol", order.Symbol,
		"type", order.Side,
		"TRB-in", q,
		"USDC-out", order.CummulativeQuoteQuantity,
	)

	select {
	case <-time.After(time.Second): // The binance api needs time to process the first order.
	case <-ctx.Done():
		return ctx.Err()
	}

	amountAfterFee := m.accumulatedUSDC - (m.accumulatedUSDC * 0.01)
	btcOut, err := m.buyBTC(ctx, amountAfterFee)
	if err != nil {
		return fmt.Errorf("executing BTCUSDC trade accumulatedUSDC:%v: %w", amountAfterFee, err)
	}

	m.logger.Info(
		"executed order",
		"USDC-in-accumulated", m.accumulatedUSDC,
		"afterTradeFee", amountAfterFee,
		"BTC-out", btcOut,
	)
	m.accumulatedUSDC = 0

	m.btcConverted.WithLabelValues(m.reporter.String()).Add(btcOut)

	return nil
}

func (m *Trader) buyBTC(ctx context.Context, amountUSDC float64) (float64, error) {
	priceStr, err := m.trader.NewListPricesService().Symbol("BTCUSDC").Do(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting BTCUSDC price: %w", err)
	}
	price, err := strconv.ParseFloat(priceStr[0].Price, 64)
	if err != nil {
		return 0, fmt.Errorf("parse BTCUSDC price:%v: %w", priceStr[0].Price, err)
	}

	quantity := amountUSDC / price
	quantity = math.Round(quantity/0.0001) * 0.0001

	order, err := m.trader.NewCreateOrderService().
		Symbol("BTCUSDC").
		Side(binance.SideTypeBuy).
		Type(binance.OrderTypeMarket).
		Quantity(fmt.Sprintf("%f", quantity)).
		Do(ctx)
	if err != nil {
		return 0, fmt.Errorf("executing BTCUSDC trade: %w", err)
	}
	btcOut, err := strconv.ParseFloat(order.ExecutedQuantity, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing ExecutedQuantity to float: %w", err)
	}

	return btcOut, nil
}

func (m *Trader) refreshBalances(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()

	resp, err := m.trader.NewGetAccountService().Do(ctx)
	if err != nil {
		cryptoriums.IncError("balance_fetch", componentName)
		return fmt.Errorf("NewGetAccountService: %w", err)
	}

	for _, balance := range resp.Balances {
		value, err := strconv.ParseFloat(balance.Free, 64)
		if err != nil {
			cryptoriums.IncError("balance_parse", componentName)
			return fmt.Errorf("parsing trader balance to float: %w", err)
		}
		m.tokenBalanceTrader.WithLabelValues(balance.Asset).Set(value)
	}

	return nil
}
