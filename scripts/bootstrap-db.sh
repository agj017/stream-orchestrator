#!/usr/bin/env bash
set -euo pipefail

# Local development helper:
# 1) create role if missing
# 2) create database if missing
# 3) run migrations up

DB_URL="${DB_URL:-postgres://postgres:postgres@localhost:5432/stream_orchestrator?sslmode=disable}"
MIGRATE_BIN="${MIGRATE_BIN:-migrate}"
MIGRATIONS_PATH="${MIGRATIONS_PATH:-migrations}"
DB_ADMIN_URL="${DB_ADMIN_URL:-${DB_URL}}"
APP_DB_USER="${APP_DB_USER:-postgres}"
APP_DB_PASSWORD="${APP_DB_PASSWORD:-postgres}"

if ! command -v psql >/dev/null 2>&1; then
  echo "psql is required but not found in PATH."
  exit 1
fi

if ! command -v "${MIGRATE_BIN}" >/dev/null 2>&1; then
  echo "migrate binary '${MIGRATE_BIN}' is required but not found in PATH."
  exit 1
fi

db_name="$(echo "${DB_URL}" | sed -E 's#^.*/([^/?]+).*$#\1#')"

echo "Ensuring role '${APP_DB_USER}' exists..."
role_exists="$(
  psql "${DB_ADMIN_URL}" -tAc "SELECT 1 FROM pg_roles WHERE rolname='${APP_DB_USER}'" | tr -d '[:space:]'
)"
if [[ "${role_exists}" != "1" ]]; then
  psql "${DB_ADMIN_URL}" -c "CREATE ROLE ${APP_DB_USER} WITH LOGIN PASSWORD '${APP_DB_PASSWORD}';"
  echo "Role '${APP_DB_USER}' created."
else
  echo "Role '${APP_DB_USER}' already exists."
fi

echo "Ensuring database '${db_name}' exists..."
exists="$(
  psql "${DB_ADMIN_URL}" -tAc "SELECT 1 FROM pg_database WHERE datname='${db_name}'" | tr -d '[:space:]'
)"
if [[ "${exists}" != "1" ]]; then
  psql "${DB_ADMIN_URL}" -c "CREATE DATABASE ${db_name} OWNER ${APP_DB_USER};"
  echo "Database '${db_name}' created."
else
  echo "Database '${db_name}' already exists."
fi

echo "Running migrations..."
"${MIGRATE_BIN}" -path "${MIGRATIONS_PATH}" -database "${DB_URL}" up
echo "Bootstrap completed."
