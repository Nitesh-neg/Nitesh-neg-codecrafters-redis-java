A lightweight Redis-like in-memory data store implementation in Java, supporting key-value pairs, lists, streams, transactions, and replication.
Features

    Data Types:

        Strings (SET/GET)

        Lists (RPUSH/LRANGE/LPOP)

        Streams (XADD/XRANGE/XREAD)

    Advanced Features:

        Transactions (MULTI/EXEC/DISCARD)

        Blocking operations (BLPOP)

        Replication (PSYNC, master-replica)

    Protocol:

        Full RESP (Redis Serialization Protocol) implementation

        Command pipelining support

Getting Started
Prerequisites

    Java 17 or higher

    Maven 3.6+

Installation
bash

git clone https://github.com/yourusername/redis-java.git
cd redis-java
mvn clean package

Running the Server
bash

# As standalone server
java -jar target/redis-java.jar --port 6379

# As replica
java -jar target/redis-java.jar --port 6380 --replicaof localhost 6379
