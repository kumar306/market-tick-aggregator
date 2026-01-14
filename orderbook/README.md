1. Plan the data structures for storing the orderbook internally - going with a skip list which comprises of layers of sorted linked lists - it gives me O(logN) insert, update, delete, min, max
2. Compute full order book internally but display top N in the snapshot consumed by UI
3. Plan for interval based snapshotting and recovery flow if a worker crashes
4. Backup snapshots to redis and re apply kafka updates. Commit to kafka post snapshot so no updates are lost
5. Compute several orderbooks segregated by exchange,symbol combinations
6. Every update from Kafka applied to orderbook needs to result in the same deterministic state
7. unit tests to verify correctness - application of updates to snapshot or empty book must give the same resultant final state for those applied prices. Reapplying updates should not change the state. Need to extract top N correctly when posting to downstream kafka consumed by UI.  

why a skip list?

Prefer a skip list over a balanced BST because i figure i am gonna have huge number of inserts, updates, deletes at any given second. because of that the tree will keep rebalancing to maintain logn height. AVL tree would do fixed rotations to maintain its height whereas red black trees would do its recoloring. this is a hindrance when i have multiple updates coming in per second. if the tree is being rebalanced, it has to be globally locked and would cause updates to slow down. its more complex to build. Skip list does not have rebalancing. bid and ask are O(1) here as head -> forward[0] and tail -> backward[0] and worst case is very very rare. height would be logn which ensures logn on insert, update, delete and topN