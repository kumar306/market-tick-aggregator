package internal

import "market-aggregator/constants"

// func to create windows - this is called when symbol enters a worker for first time
// wire up the windows from config. for each window wire up the metrics
// set it in the worker

// this should return a map[string]Window - each Window contains map[string]Metric

func BuildWindows(cfg []*constants.WindowConfig) map[string]*constants.Window {
	windowMap := make(map[string]*constants.Window)

	for _, w := range cfg {
		metrics := make(map[constants.MetricName]constants.Metric)

		for name, ctor := range MetricCtorRegistry {
			metrics[name] = ctor(w)
		}

		windowMap[w.Id] = &constants.Window{
			Id:             w.Id,
			DurationMs:     w.DurationMs,
			FlushCadencyMs: w.FlushCadencyMs,
			Metrics:        metrics,
			LastFlushTsMs:  0,
		}
	}

	return windowMap
}
