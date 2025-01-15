# MboxTurbo
A Python package for indexing and efficiently reading large mbox files with parallel chunked processing. MboxTurbo provides high performance
indexing of messages in an mbox file by computing the byte offsets of each message of the mbox file once. Once the offset map is computed once,
it can be exported to a binary file which can be imported skipping the overhead of computing the message offsets. This allows the library to perform O(1) look ups on Mbox messages given an index. Both multi-processing and multi-threading are seemless in MboxTurbo and easily customizable with the MboxReader class which allows you to specify how each EmailMessage read from the mbox file should be processed by passing in a callback to the object. 


  
