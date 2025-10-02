# syntax=docker/dockerfile:1
FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive
ARG PGBRANCH=REL_18_STABLE

# Base setup & build deps (not including emacs yet for caching)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git \
    build-essential pkg-config \
    bison flex \
    libreadline-dev zlib1g-dev libssl-dev \
    libxml2-dev libxslt1-dev \
    libzstd-dev liblz4-dev \
    libicu-dev \
    llvm-dev clang \
    tcl python3 python3-dev python3-pip \
    locales jq emacs gcovr \
  && pip3 install slack_sdk matplotlib --break-system-packages \
  && rm -rf /var/lib/apt/lists/*

# Locale
RUN sed -i 's/# *en_US.UTF-8/en_US.UTF-8/' /etc/locale.gen && locale-gen
ENV LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8

# Build PostgreSQL 18 from source
WORKDIR /opt
RUN git clone --depth=1 --branch ${PGBRANCH} https://git.postgresql.org/git/postgresql.git

WORKDIR /opt/postgresql
ENV CFLAGS="-O3 -g -fno-omit-frame-pointer" \
    LDFLAGS="-Wl,-O1 -Wl,--as-needed"
RUN ./configure \
      --prefix=/usr/local/pgsql \
      --with-openssl \
      --with-icu \
      --with-libxml \
      --with-libxslt \
      --with-zstd \
      --with-lz4 \
      --with-python \
      --with-llvm \
      --disable-cassert \
  && make -j $(nproc) \
  && make install \
  && (cd contrib/pg_buffercache && make install)

RUN /usr/local/pgsql/bin/pg_config

# provide codex for inside-docker use
RUN apt update && apt install curl nodejs npm -y \
  && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
  && apt-get install -y nodejs \
  && npm install -g @openai/codex

# provide claude code for inside-docker use
RUN npm install -g @anthropic-ai/claude-code

# PATH
ENV PATH="/usr/local/pgsql/bin:${PATH}"

# Data dir
RUN useradd -m postgres \
  && mkdir -p /var/lib/postgresql/data /var/run/postgresql \
  && chown -R postgres:postgres /var/lib/postgresql /usr/local/pgsql \
  && chsh -s /bin/bash postgres \
  && echo "PATH=$PATH:/opt/postgresql/src/bin:/usr/local/pgsql/bin" >> /home/postgres/.bashrc

EXPOSE 5432
#ENTRYPOINT ["/bin/bash"]

# make stop && make install && make start && su - postgres bash -c '/usr/local/pgsql/bin/psql -f bench/thrash.sql' | tee results/thrash.out
# git config user.email "140002+asah@users.noreply.github.com" && git config user.name "Adam Sah"
