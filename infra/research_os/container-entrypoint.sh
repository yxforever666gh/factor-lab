#!/bin/sh
set -eu

read_secret() {
  secret_path="$1"
  secret_label="$2"
  [ -f "$secret_path" ] && [ ! -L "$secret_path" ] || {
    echo "$secret_label secret file is missing or is a symlink" >&2
    exit 64
  }
  secret_value=$(sed -e 's/\r$//' "$secret_path")
  [ -n "$secret_value" ] && [ "$(printf '%s' "$secret_value" | wc -l)" -eq 0 ] || {
    echo "$secret_label secret file must contain one non-empty line" >&2
    exit 64
  }
  printf '%s' "$secret_value"
}

pgpass_escape() {
  # libpq's password-file grammar reserves both backslash and colon.  Secret
  # generation is intentionally unconstrained, so escape rather than silently
  # making a strong password unusable.
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/:/\\:/g'
}

# The WebUI settings file is deliberately not imported into process
# environment.  Application-service/resource construction reads its reviewed
# allow-list on every invocation, so a subsequent resource sees an atomic WebUI
# update instead of an entrypoint-time value shadowing it forever.  In
# particular, this script never exports profile JSON or credential references.

runtime_secret_dir=$(mktemp -d /tmp/factor-lab-runtime-secrets.XXXXXX)
chmod 700 "$runtime_secret_dir"

if [ -n "${FACTOR_LAB_POSTGRES_PASSWORD_FILE:-}" ]; then
  postgres_password=$(read_secret "$FACTOR_LAB_POSTGRES_PASSWORD_FILE" postgres)
  postgres_password_escaped=$(pgpass_escape "$postgres_password")
  printf '%s:%s:%s:%s:%s\n' \
    "${RESEARCH_OS_POSTGRES_HOST:-postgres}" \
    "${RESEARCH_OS_POSTGRES_PORT_INTERNAL:-5432}" \
    "${RESEARCH_OS_POSTGRES_DB:-factor_lab}" \
    "${RESEARCH_OS_POSTGRES_USER:?missing RESEARCH_OS_POSTGRES_USER}" \
    "$postgres_password_escaped" > "$runtime_secret_dir/pgpass"
  chmod 600 "$runtime_secret_dir/pgpass"
  unset postgres_password postgres_password_escaped
  export PGPASSFILE="$runtime_secret_dir/pgpass"
fi

if [ -n "${FACTOR_LAB_OBJECT_STORE_ACCESS_KEY_FILE:-}" ] && \
   [ -n "${FACTOR_LAB_OBJECT_STORE_SECRET_KEY_FILE:-}" ]; then
  object_access_key=$(read_secret "$FACTOR_LAB_OBJECT_STORE_ACCESS_KEY_FILE" object-store-access)
  object_secret_key=$(read_secret "$FACTOR_LAB_OBJECT_STORE_SECRET_KEY_FILE" object-store-secret)
  {
    printf '[default]\n'
    printf 'aws_access_key_id=%s\n' "$object_access_key"
    printf 'aws_secret_access_key=%s\n' "$object_secret_key"
  } > "$runtime_secret_dir/aws-credentials"
  chmod 600 "$runtime_secret_dir/aws-credentials"
  unset object_access_key object_secret_key
  export AWS_SHARED_CREDENTIALS_FILE="$runtime_secret_dir/aws-credentials"
fi

if [ "${FACTOR_LAB_ENVIRONMENT:-}" = production ]; then
  if [ "${FACTOR_LAB_PRODUCTION_ROLE:-worker}" = webui ]; then
    python -m factor_lab.webui.runtime_guard >/dev/null
  else
    python -c "from factor_lab.research_os.production_config import _main; raise SystemExit(_main())" \
      --config "${FACTOR_LAB_ORCHESTRATION_CONFIG:?missing production config}" \
      >/dev/null
  fi
fi

exec "$@"
