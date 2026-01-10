# RaftKV — Final (Raft + JNI storage)


### What this does
- 3-node cluster with **Raft election + replication**
- Writes are accepted only by the **leader** and **commit on majority**
- Followers learn the leader’s `commitIndex` and **apply to durable** C WAL
- GET works from **any** node after commit


### Quick start
```bash
docker compose build
docker compose up -d
docker compose ps


# Find the leader (check all 3)
curl http://localhost:8081/status || true
curl http://localhost:8082/status || true
curl http://localhost:8083/status || true


# Try PUT to a follower (shows redirect)
curl -i -X POST "http://localhost:8082/put?key=hello&val=world"


# PUT on the leader (replace 808X)
curl -i -X POST "http://localhost:808X/put?key=hello&val=world"


# Read from any node
curl "http://localhost:8081/get?key=hello"
curl "http://localhost:8082/get?key=hello"
curl "http://localhost:8083/get?key=hello"


# (Optional) Kill the leader; watch a new one take over
# docker stop raftkv-node1