metrics to be calculated:

price and volume:

1. vwap - 1m, 5m, 10m, 30m, 1h, 24h rolling, session vwap - used for benchmark algos, slippage measurement
2. twap - used to measure distribution of orders
3.1 rolling volume - 1m, 5m, 10m, 30m, 1h, 24h - to calculate liquidity
3.2 volume acceleration (delta volume/delta time) 

volatility and risk:

1. volatility - welford algo - used for position sizing and stop placement - high volatility means we will buy less to manage our risk
2. average true range (ATR) - high ATR means there is a high fluctuation of price so we will buy lesser, whereas low ATR means low fluctuation so we can confidently buy more

trend and momentum:

1. EMA/SMA - to track price trends. EMA is faster and more sensitive whereas SMA is slower and smoother. I will enter a trade when i know price has crossed the EMA
2. returns - log returns, normal returns - it tells me the change in price for a candlestick period - by showing percentage gain or loss using end price and begin price for a period

behaviour:

1. maintain sets of windows each computing low latent stats, mid to long running stats
2. some stats are tumbling (candlesticks), some are rolling (EMA, SMA)
3. should be able to add new windows without modifying other windows logic
4. should be able to add more metrics to a window without modifying the code for a window processing
5. worker should remain stateless, his job is just to flush the windows on ticker. all symbols follow flush at same time. let the window handle the state. worker's job is to hold pointers to windows held for a symbol and he manages multiple symbols.
on a tick arrival, worker maintains pointer to 6-8 windows for the tick symbol and updates all its windows. each window has its flush time - which triggers for all the symbols. 
6. upon flush event, window state is read and persisted to kafka. if its a tumbling window, the state is reset. if its a rolling window, we will go with decayed window for metrics like EMA/SMA, volatility to take recent prices to have more priority rather than evicting - this is to keep compute fast. larger the window, smaller the alpha value. 1hour rolling window vs 2 hour rolling window - alpha is larger in the 1hour rolling window to make it more sensitive to newer data than the 2 hour window. for metrics like rolling volume, rolling vwap we will go with bucketed rolling. 
7. window handles metrics - but the actual metric itself is intelligent. the window doesnt care how the metric is aggregated. we can have many different kind of metrics in a single window (EMA/VWAP/candlestick, etc) which follow different rolling/tumbling patterns.

tick schema:
AggregatedTick
├── identity (exchange, channel, symbol)
├── window metadata (duration, start, end, type)
├── price-based metrics
│   ├── ohlc
│   ├── vwap
│   ├── twap
│   ├── microprice
├── volume-based metrics
│   ├── volume
│   ├── rolling_volume
│   ├── volume_acceleration
├── volatility & risk
│   ├── volatility
│   ├── atr
├── trend & momentum
│   ├── ema
│   ├── sma
│   ├── log_return
│   ├── simple_return
└── extensibility (future)


so tick arrives: loop through all windows for a bufferKey - call Update method for each metric present in each window - to update its metric - Metric implements Update(t Tick), Snapshot() T, Reset(), GetMetricType(). window doesnt do any updation on its own. Window only takes care of flushing the snapshot to kafka and persistence i guess. flush() on Window is dumb, it calls snapshot() of each metric - adds the value to the aggregated proto, persists to Kafka. the upstream tick is committed as it arrives, we are not coming for exact correctness, as metrics is a constructed report, not a source of truth. If the system crashes before flushing, it would converge back upon restart. or we could fetch state from previous and continue where we left off like that. Normal backpressure and circuit breaker tactics like normalizer can be applied here by monitoring the worker queue size/capacity and triggering pause partitions using the worker partition assignment map - and resuming once the threshold is back to normal - all obviously configurable metric also should have its own name setter, type setter - which is got back in snapshot etc (this is set when creating the windows i guess) - or create many metric classes - keep them separate with their implemented methods - when creating the windows with metrics on map insertion, i'll need to create instances of all these classes separately - using a registry of metric constructors I guess and calling the construct method, adding each inside []Metrics of Window. thinking about bucketed rolling metric. so e.g flush cadency = 5s, window duration = 5m, bucket size = 1s. so with each update, we check if now - lastBucketTs > bucket size, advance bucket Idx + 1. or get now - lastbucketTs - divide it by bucketSize and advance idx by that many buckets. so i will have a dedicated ticker routine to post flush events to the same worker channel - if i have 16 workers - 0 symbols in some,1 symbol in some, 2+ symbols in some. now these workers all got to flush their windows as per their flush cadency - one global clock - as soon as we start the workers, we start a goroutine sending flush event to every worker channel - flush event contains window id, flush_cadence, timestamp - flush event enters the worker. for worker having no symbols, skip. for worker having some symbols arleady, iterate its map of symbols - for its specific window - call window.Flush(), reset metric of that window if tumbling, etc. for that worker channel still lastFlushTime would be updated - even if it has no symbols. this would be maintained in a separate area, not inside the worker. but from goroutine sending flush events to each worker

backpressure: non blocking producer, non blocking dispatch channel, drop messages if channel blocking. no need that every metric message strictly needs to be flushed to downstream

testing plan:
1. similar records should be routed to same worker by dispatcher //
2. worker builds windows and wires metrics for each window on symbol insertion //
3. flush goroutine should correctly post events and aggregated tick to be constructed correctly //
4. tumbling metrics to be reset and rolling metrics to be no-op
5. rolling buckets test
6. no publish when circuit breaker is open

18/02/2026: 
1. need to add idempotency guard in aggregator to avoid duplicate publish from normalizer (dedupe key - topic:partition:offset) - same dedupe logic as upstream
2. since adding point 1, need to revert from auto commit to manual commit, so ensures that commit occurs only after downstream publish and mark for dedupe

20/02/2026:
1. check why twap, microprice < 0 for some coinbase records when open > 0 and whe