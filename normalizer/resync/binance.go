package resync

import (
	"encoding/json"
	"fmt"
	"io"
	"market-normalizer/constants"
	"net/http"
	"time"
)

const binanceDepthSnapshotURL = "https://api.binance.com/api/v3/depth"

var httpClient = &http.Client{Timeout: 10 * time.Second}

// calls binance depth
// independent of the adapter's WebSocket session , its not delivered on binance book stream
func FetchBinanceDepthSnapshot(symbol string) (*constants.BinanceDepthSnapshot, error) {
	url := fmt.Sprintf("%s?symbol=%s&limit=1000", binanceDepthSnapshotURL, symbol)
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
