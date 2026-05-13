ARG PYTHON_VERSION=3.14-slim-trixie
ARG UV_VERSION=0.11.12
ARG NATS_VERSION=0.4.0


# Specifies the uv version to use in stages
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv


# =============================================================================
# BASE
# Slim Trixie with a non-root user inherited by all stages.
# =============================================================================
FROM docker.io/python:${PYTHON_VERSION} AS base

# Set environment variables for Python to improve behavior in containers.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

# Create non-root user and group with UID and GID 1000 (matches typical host user)
RUN groupadd --gid 1000 appuser \
    && useradd --uid 1000 --gid 1000 --create-home appuser

# Switch to non-root user
USER appuser


# =============================================================================
# DEV
# Adds system tools and Claude Code needed during development.
# =============================================================================
FROM base AS dev

# Switch back to root to install system packages.
USER root

# Install system tools only for the development environment
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl gpg openssh-client fish starship eza locales \
    && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen \
    && rm -rf /var/lib/apt/lists/*

ENV LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

# Install uv
COPY --from=uv /uv /uvx /bin/

# Install psql 18 client from PGDG (Trixie repos only ship up to pg17)
RUN curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        | gpg --dearmor -o /usr/share/keyrings/pgdg.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] https://apt.postgresql.org/pub/repos/apt trixie-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends postgresql-client-18 \
    && rm -rf /var/lib/apt/lists/*

# Install nats CLI from GitHub release
ARG NATS_VERSION
RUN curl -fsSL https://github.com/nats-io/natscli/releases/download/v${NATS_VERSION}/nats-${NATS_VERSION}-amd64.deb -o /tmp/nats.deb \
    && dpkg -i /tmp/nats.deb \
    && rm /tmp/nats.deb

# Switch to non-root user
USER appuser

# Configure fish and starship for the rootless user
RUN mkdir -p ~/.config && \
    starship preset no-runtime-versions -o ~/.config/starship.toml
COPY .devcontainer/config/config.fish /etc/fish/conf.d/config.fish
RUN mkdir -p ~/.local/share/fish
ENV SHELL=/usr/bin/fish


# =============================================================================
# BUILDER
# Installs uv and resolves dependencies into a virtualenv.
# Sibling of dev (not derived from it) so CI builds only what prod needs.
# =============================================================================
FROM base AS builder

# Install uv
COPY --from=uv /uv /uvx /bin/

WORKDIR /home/appuser/app

# Install only production dependencies, locked versions
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev


# =============================================================================
# RUNNER
# Minimal production image — only the app and its virtualenv, no uv or build tools.
# =============================================================================
FROM base AS runner

WORKDIR /home/appuser/app

COPY --chown=appuser:appuser --from=builder /home/appuser/app/.venv .venv
COPY --chown=appuser:appuser . .

ENV PATH="/home/appuser/app/.venv/bin:$PATH"

CMD ["data-pipeline"]
