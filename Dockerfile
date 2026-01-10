# syntax=docker/dockerfile:1

# ---- Stage 1: Build native C/JNI library ------------------------------------
FROM ubuntu:24.04 AS c-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    openjdk-17-jdk \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app/c
COPY c/ /app/c/

# Build shared library: libkvstore.(so|dylib|dll)
RUN mkdir -p build \
 && cd build \
 && cmake .. -DCMAKE_BUILD_TYPE=Release \
 && cmake --build . --config Release

# ---- Stage 2: Build Java fat JAR --------------------------------------------
FROM gradle:8.7.0-jdk17 AS java-builder
WORKDIR /app/java
COPY java/ /app/java/

# Build shadow JAR at build/libs/raftkv-all.jar
RUN gradle --no-daemon clean shadowJar

# ---- Stage 3: Runtime image -------------------------------------------------
FROM eclipse-temurin:17-jre-jammy AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/raftkv
RUN mkdir -p /opt/raftkv/native

# Native library + application JAR
COPY --from=c-builder    /app/c/build/libkvstore.so          /opt/raftkv/native/
COPY --from=java-builder /app/java/build/libs/raftkv-all.jar /opt/raftkv/

# Defaults (overridable by docker-compose)
ENV PORT=8080 \
    NODE_ID=node \
    DATA_DIR=/data \
    PEERS=node=http://localhost:8080

VOLUME ["/data"]
EXPOSE 8080

# <<< single-line exec-form CMD (fix) >>>
CMD ["java","-Djava.library.path=/opt/raftkv/native","-cp","/opt/raftkv/raftkv-all.jar","com.example.raftkv.Main"]