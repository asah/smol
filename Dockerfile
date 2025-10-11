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
    libicu-dev liburing-dev \
    llvm-dev clang \
    tcl python3 python3-dev python3-pip \
    locales sudo less jq emacs gcovr vmtouch \
  && pip3 install slack_sdk matplotlib --break-system-packages \
  && rm -rf /var/lib/apt/lists/*

# Locale
RUN sed -i 's/# *en_US.UTF-8/en_US.UTF-8/' /etc/locale.gen && locale-gen
ENV LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8

# provide codex for inside-docker use
RUN apt update && apt install curl nodejs npm -y \
  && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
  && apt-get install -y nodejs \
  && npm install -g @openai/codex

# provide claude code for inside-docker use
RUN npm install -g @anthropic-ai/claude-code

# setup postgres user
RUN useradd -m -s /bin/bash -d /home/postgres postgres \
  && mkdir -p /home/postgres/pgdata /var/run/postgresql /usr/local/pgsql \
  && chown -R postgres:postgres /var/run/postgresql /usr/local/pgsql \
  && echo "PATH=$PATH:/usr/local/pgsql/bin" >> /home/postgres/.bashrc \
  && echo "postgres ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/90-postgres-nopasswd \
  && chmod 0440 /etc/sudoers.d/90-postgres-nopasswd

USER postgres
WORKDIR /home/postgresql

# Build PostgreSQL 18 from source
RUN git clone --depth=1 --branch ${PGBRANCH} https://git.postgresql.org/git/postgresql.git pg18src
WORKDIR /home/postgresql/pg18src
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
      --with-liburing \
      --disable-cassert \
  && make -j $(( $(nproc) * 2 )) \
  && make install \
  && (cd contrib/pg_buffercache && make install)

#RUN /usr/local/pgsql/bin/pg_config

# PATH
WORKDIR /home/postgresql
ENV PATH="/usr/local/pgsql/bin:${PATH}"

EXPOSE 5432

# make stop && make install && make start && su - postgres bash -c '/usr/local/pgsql/bin/psql -f bench/thrash.sql' | tee results/thrash.out
# git config user.email "140002+asah@users.noreply.github.com" && git config user.name "Adam Sah"


