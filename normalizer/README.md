proto command for my reference: protoc --go_out=. .\proto\*.proto

test plan:

wal tests:
1. first need to somehow make the breaker in open state. then need to test that append works and only append happens. 
2. need to toggle the breaker to open. then make it closed and then the replay triggers. so i need to ensure the downstream produce happened. and the file is cleared. 
3. mock a error for the 3rd or 4th record processing out of 10 records. then need to ensure the new file contains the 4th to 10th record. 
4. test for new message to enter the pipeline at the same time replay is happening and it is blocked until the replay is done (testing replayLock)