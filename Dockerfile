FROM ubuntu:24.04

# Install PostgreSQL development tools and build essentials
RUN apt update && \
    apt install -y postgresql-server-dev-16 postgresql-16 build-essential && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Set environment to avoid SDK issues
ENV SDKROOT=
