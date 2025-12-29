<img width="1280" height="320" alt="RedDuck" src="https://github.com/user-attachments/assets/24fc1401-2cbd-4c20-a7cb-aa2c4a83ee91" />

### **A high-performance, zero-dependency Redis connector for DuckDB.**

RedDuck is a custom C++ extension that allows DuckDB to query live Redis data directly.

Unlike standard connectors that rely on heavy client libraries (like `hiredis`), RedDuck implements a **custom-built RESP (Redis Serialization Protocol) parser** and low-level TCP socket implementation from scratch. This was designed to maximize control over memory allocation and network buffer management, with the goal of outperforming standard connector latency.

```sql
LOAD redduck;

-- (Optional) Configure the connection. 
-- If skipped, defaults to localhost:6379
SELECT redis_connect('redis://192.168.1.50:6379');

SELECT * FROM redis_scan('user:*');
```
--- 
## Primary Learning Goals:
1. **Networking from Scratch:** Instead of using hiredis, I wanted to write my own driver. I learned how to manage raw TCP sockets, handle packet fragmentation, and implement the RESP wire protocol manually.
2. **DuckDB Internals:** I chose DuckDB because of its modern vectorized execution engine. Writing this extension taught me how analytical engines handle memory (Buffer Pools), execute table functions, and optimize query plans.
3. **Performance Engineering:** My goal was to build a connector that is potentially faster than generic wrappers by stripping away unused client library overhead and optimizing specifically for OLAP read patterns.
