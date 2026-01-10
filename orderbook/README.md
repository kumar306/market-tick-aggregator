1. Plan the data structures for storing the orderbook internally - going with a skip list which comprises of layers of sorted linked lists - it gives me O(logN) insert, update, delete, min, max
2. Compute full order book internally but display top N in the snapshot consumed by UI
3. Plan for interval based snapshotting and recovery flow if a worker crashes
4. Backup snapshots to redis and re apply kafka updates. Commit to kafka post snapshot so no updates are lost
5. Compute several orderbooks segregated by exchange,symbol combinations
6. Every update from Kafka applied to orderbook needs to result in the same deterministic state
7. unit tests to verify correctness - application of updates to snapshot or empty book must give the same resultant final state for those applied prices. Reapplying updates should not change the state. Need to extract top N correctly when posting to downstream kafka consumed by UI.  