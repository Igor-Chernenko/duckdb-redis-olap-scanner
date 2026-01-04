<img width="1280" height="320" alt="RedDuck" src="https://github.com/user-attachments/assets/24fc1401-2cbd-4c20-a7cb-aa2c4a83ee91" />

### **A high-performance, zero-dependency Redis connector for DuckDB.**

RedDuck is a custom C++ extension that allows DuckDB to query live Redis data directly.

Unlike standard wrappers that depend on heavy client libraries (e.g., hiredis), RedDuck features a custom-built RESP (Redis Serialization Protocol) parser and a raw TCP socket implementation. This architecture eliminates external dependencies and provides granular control over memory allocation minimizing memory overhead by parsing RESP directly into DuckDB Vectors, bypassing standard client library object allocation.

## Usage Guide

### 1. Connection & Setup
Initialize the extension and establish a TCP connection to the Redis host.

```sql
LOAD redduck;

-- Defaults to localhost:6379 if no argument is provided
SELECT redis_connect('redis://192.168.1.50:6379');
```
### 2. Key Discovery
```sql
-- Retrieve a list of keys matching a pattern using batches
SELECT * FROM redis_scan('pattern');

```
### 3. Data Retrieval
Fetch values efficiently using vectorized execution.
```sql
-- Optimized batch retrieval of Key-Value pairs
SELECT * FROM redis_kv('pattern');

-- Retrieve simple string values for specific keys
SELECT key, redis_get(key) FROM redis_scan('pattern');

-- Retrieve and expand Redis Hashes into DuckDB STRUCTs
SELECT key, redis_hgetall(key) as user_data 
FROM redis_scan('pattern');
```

--- 
## Primary Learning Goals:
1. **Networking from Scratch:** Instead of using hiredis, I wanted to write my own driver. I learned how to manage raw TCP sockets, handle packet fragmentation, and implement the RESP wire protocol manually.
2. **DuckDB Internals:** I chose DuckDB because of its modern vectorized execution engine. Writing this extension taught me how analytical engines handle memory (Buffer Pools), execute table functions, and optimize query plans.
3. **Performance Engineering:** My goal was to build a connector that is potentially faster than generic wrappers by stripping away unused client library overhead and optimizing specifically for OLAP read patterns.
