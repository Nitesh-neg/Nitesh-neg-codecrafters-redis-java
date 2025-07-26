# Nitesh Redis Java

![Java](https://img.shields.io/badge/Java-17%2B-blue)
![License](https://img.shields.io/badge/License-MIT-green)
![Build](https://img.shields.io/badge/Build-Passing-brightgreen)

A lightweight, Redis-inspired in-memory data store implemented in Java. Supports key-value pairs, lists, streams, transactions, replication, and more. Ideal for learning, prototyping, or embedding Redis-like functionality into Java projects.

## ✨ Features

### Data Types
- **Strings**: SET, GET, INCR
- **Lists**: RPUSH, LRANGE, LPOP, BLPOP
- **Streams**: XADD, XRANGE, XREAD with blocking support

### Advanced Features
- 🧮 Transactions: MULTI, EXEC, DISCARD
- ⏳ Blocking operations: BLPOP
- 🔄 Replication: PSYNC, master-replica with offset tracking

### Protocol Support
- 📡 Full RESP (Redis Serialization Protocol) implementation
- 🚀 Command pipelining support
- 🔄 Replication protocol compatibility

## 🚀 Getting Started

### Prerequisites
- Java 17 or higher
- Maven 3.6+

### Installation
```bash
git clone https://github.com/Nitesh-neg/Nitesh-redis-java.git
cd Nitesh-redis-java
mvn clean package

Running the Server

Standalone mode:
bash

java -jar target/redis-java.jar --port 6379

Replica mode:
bash

java -jar target/redis-java.jar --port 6380 --replicaof localhost 6379


