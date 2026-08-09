package resync

import (
	"encoding/json"
	"fmt"
	"io"
	"market-normalizer/constants"
	"net/http"
	"os"
	"time"
)

const defaultBinanceDepthSnapshotURL = "https://api.binance.com/api/v3/depth"

var httpClient = &http.Client{Timeout: 10 * time.Second}

// infra-provided override so this points at the mock exchange server during
// load testing instead of real Binance (which blocks AWS IP ranges anyway)
func binanceDepthSnapshotURL() string {
	if v := os.Getenv("BINANCE_REST_URL"); v != "" {
		return v
	}
	return defaultBinanceDepthSnapshotURL
}

// calls binance depth
// independent of the adapter's WebSocket session , its not delivered on binance book stream
func FetchBinanceDepthSnapshot(symbol string) (*constants.BinanceDepthSnapshot, error) {
	url := fmt.Sprintf("%s?symbol=%s&limit=1000", binanceDepthSnapshotURL(), symbol)
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetching binance depth snapshot: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("binance depth snapshot returned status %d: %s", resp.StatusCode, string(body))
	}

	var snapshot constants.BinanceDepthSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("decoding binance depth snapshot: %w", err)
	}

	return &snapshot, nil
}
