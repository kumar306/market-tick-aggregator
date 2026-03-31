proto command for my reference: protoc --go_out=. .\proto\*.proto

test plan:

wal tests:
1. first need to somehow make the breaker in open state. then need to test that append works and only append happens. 
2. need to toggle the breaker to open. then make it closed and then the replay triggers. so i need to ensure the downstream produce happened. and the file is cleared. 
3. mock a error for the 3rd or 4th record processing out of 10 records. then need to ensure the new file contains the 4th to 10th record. 
4. test for new message to enter the pipeline at the same time replay is happening and it is blocked until the replay is done (testing replayLock)

31/03/2026:
uncovered a bug - burst of data for kraken book partition led to a lot of buffered records being dropped. all the records for Kraken BTC-USD were mapped to single partition and the websocket got in a burst of data
 
worker channel got full and since i had buffered 5000 records as per config, too many records got dropped from dispatch channel
backpressure high threshold was at 0.8 so it took too long to kick in. 
to address it, i'll change backpressure high threshold and low threshold to a lower values to lessen these drops occuring due to an overloaded partition -> made it high=0.4, low=0.2. increased worker queue size=2000