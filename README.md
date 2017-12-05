# SimpleDynamo
This project implements a simplified version of Dynamo. There are three main pieces in this implementation:
1) Partitioning
2) Replication
3) Failure handling

The main goal is to provide both availability and linearizability at the same time. In other words, the implementation should always perform read and write operations successfully even under failures. At the same time, a read operation should always return the most recent value.

<a href = https://docs.google.com/document/d/1VpTvRTb7TETtN59ovdfb1FMQDRXfq6H5Toh7L7Dq1P4/edit>Project Spec</a>
