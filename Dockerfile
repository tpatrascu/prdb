# Client image for connecting to a separately configured PRDB gateway.
FROM docker.io/library/rust:1.95.0-bookworm AS build

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        build-essential \
        binutils \
        clang \
        patchelf \
        python3 \
        tcc=0.9.27+git20200814.62c30a4a-1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY engine ./engine

ARG PRDB_CPU_BASELINE=x86_64-v1
ARG PRDB_RUNTIME_PROFILE=debian-bookworm-glibc-2.36
ARG PRDB_SDK_RELEASE=1
ARG PRDB_SDK_ABIS=1,2,3,4,5

RUN cargo build --release --locked -p prdb-engine --bins \
    && make -f engine/sdk/Makefile sdk-shared-release \
        BUILD_DIR=target/docker-sdk \
        SDK_RELEASE="${PRDB_SDK_RELEASE}" \
        SDK_NATIVE_ABIS="${PRDB_SDK_ABIS}" \
        SDK_CPU_BASELINE="${PRDB_CPU_BASELINE}" \
        SDK_RUNTIME_PROFILE="${PRDB_RUNTIME_PROFILE}" \
        SHARED_LINKER=cc

FROM docker.io/library/debian:bookworm-slim AS client

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        ca-certificates \
        gcc \
        patchelf \
        tcc=0.9.27+git20200814.62c30a4a-1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 prdb \
    && useradd --system --uid 10001 --gid prdb --home-dir /home/prdb --create-home \
        --shell /usr/sbin/nologin prdb \
    && install -d -o prdb -g prdb -m 0700 /work /home/prdb/.config/prdb

ARG PRDB_CPU_BASELINE=x86_64-v1
ARG PRDB_RUNTIME_PROFILE=debian-bookworm-glibc-2.36
ARG PRDB_SDK_RELEASE=1
ARG PRDB_SDK_ABIS=1,2,3,4,5
ENV PRDB_CPU_BASELINE=${PRDB_CPU_BASELINE} \
    PRDB_RUNTIME_PROFILE=${PRDB_RUNTIME_PROFILE} \
    PRDB_SDK_RELEASE=${PRDB_SDK_RELEASE} \
    PRDB_SDK_ABIS=${PRDB_SDK_ABIS} \
    PRDB_SDK_LIBRARY=/opt/prdb/sdk/releases/linux-x86_64/${PRDB_CPU_BASELINE}/${PRDB_RUNTIME_PROFILE}/sdk-${PRDB_SDK_RELEASE}-abis${PRDB_SDK_ABIS}/libprdb_sql.so.${PRDB_SDK_RELEASE} \
    PRDB_INCLUDE_DIR=/opt/prdb/include

COPY --from=build /src/target/release/prdb-client /usr/local/bin/prdb-client
COPY --from=build /src/engine/include /opt/prdb/include
COPY --from=build /src/target/docker-sdk/releases/linux-x86_64/ /opt/prdb/sdk/releases/linux-x86_64/

WORKDIR /work
USER prdb
ENTRYPOINT ["/usr/local/bin/prdb-client"]
CMD ["--help"]

# Full server image. It includes the client as well as the supervisor and
# gateway, but requires an explicitly delegated writable cgroup v2 subtree.
FROM client AS server

USER root
RUN install -d -o prdb -g prdb -m 0700 \
      /var/lib/prdb /var/lib/prdb/scratch /var/lib/prdb/instance/sdk-libraries \
      /run/prdb

COPY --from=build /src/target/release/prdb-executor /usr/local/bin/prdb-executor
COPY --from=build /src/target/release/prdb-gateway /usr/local/bin/prdb-gateway
COPY --from=build /src/target/release/prdb-init /usr/local/bin/prdb-init
COPY --from=build /src/target/release/prdb-landlock-run /usr/local/bin/prdb-landlock-run
COPY --from=build /src/target/release/prdb-maintain /usr/local/bin/prdb-maintain
COPY --from=build /src/target/release/prdb-optimizer /usr/local/bin/prdb-optimizer
COPY --from=build /src/target/release/prdb-packager /usr/local/bin/prdb-packager
COPY --from=build /src/target/release/prdb-supervisor /usr/local/bin/prdb-supervisor
COPY engine/tools/with_delegated_cgroup.sh /usr/local/bin/with_delegated_cgroup.sh
COPY docker/server-entrypoint.sh /usr/local/bin/prdb-server-entrypoint
RUN chmod 0755 /usr/local/bin/prdb-server-entrypoint \
    && chmod 0755 /usr/local/bin/with_delegated_cgroup.sh

USER prdb
EXPOSE 8765 8766
ENTRYPOINT ["/usr/local/bin/prdb-server-entrypoint"]
CMD ["serve"]
