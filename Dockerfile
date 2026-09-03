# syntax=docker/dockerfile:1

FROM eclipse-temurin:17-jdk-jammy AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY c/ c/
RUN cmake -S c -B build/native -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build/native --config Release --parallel

COPY java/src/main/java/ java/src/main/java/
RUN find java/src/main/java -name '*.java' -print > build/java-sources.txt \
    && mkdir -p build/classes \
    && javac --release 17 -Xlint:all -Werror -d build/classes @build/java-sources.txt \
    && jar --create --file build/raftkv.jar \
        --main-class com.example.raftkv.Main -C build/classes .

FROM eclipse-temurin:17-jre-jammy AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system raftkv \
    && useradd --system --gid raftkv --home-dir /opt/raftkv raftkv \
    && mkdir -p /opt/raftkv/native /data \
    && chown -R raftkv:raftkv /opt/raftkv /data

WORKDIR /opt/raftkv
COPY --from=builder --chown=raftkv:raftkv /src/build/raftkv.jar ./raftkv.jar
COPY --from=builder --chown=raftkv:raftkv /src/build/native/libkvstore.so ./native/libkvstore.so

ENV PORT=8080 \
    NODE_ID=node1 \
    DATA_DIR=/data \
    PEERS=node1=http://127.0.0.1:8080 \
    RAFT_SNAPSHOT_THRESHOLD=256

USER raftkv
VOLUME ["/data"]
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=2s --retries=10 \
    CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["java", "-Djava.library.path=/opt/raftkv/native", "-jar", "/opt/raftkv/raftkv.jar"]
