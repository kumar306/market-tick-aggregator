this module reads from aggregated ticks and aggregated book topics, batches the records, flushes to postgres via txn and commits the offsets. its a single threaded stateless process

to accomodate scaling, we can increase the batch size. fsync operation are expensive so batch aggressively, or increase consumer instances in the consumer group

if db write fails, we are good as we are not committing offsets and we can replay kafka. 
if db write succeeds and then kafka commit fails, we ensure idempotency in our postgres table to avoid inserting duplicates on replay

decided to have it as a single threaded synchronous process. 

LLD design: each of the components has a single responsiblity. 
e.g batcher doesnt know anything about its downstream (can switch from postgres to some other db later) - it will take in slice of records and call flushFn which is an input callback and be done with its processing. does offset commit after flush fn commits txn

postgres flushFn - only responsible for inserting records to postgres.
we can have other flush fns for other DBs if we want a new sink and just wire it up

