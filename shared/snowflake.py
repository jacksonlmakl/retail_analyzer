"""Shared Snowflake helpers — connection, env check, DDL execution, and migrations."""

import base64
import os
import sys
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

DATABASE = "RETAIL_ANALYZER"

_SHARED_SQL_DIR = Path(__file__).resolve().parent / "sql"
SETUP_DIR = _SHARED_SQL_DIR / "setup"
MIGRATIONS_DIR = _SHARED_SQL_DIR / "migrations"

ALL_SCHEMAS = [
    "GOOGLE_SHOPPING",
    "GRAILED",
    "VESTIAIRE",
    "REBAG",
    "FARFETCH",
    "FASHIONPHILE",
    "SECOND_STREET",
]

_ALWAYS_REQUIRED = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_WAREHOUSE",
]


def check_env():
    """Validate that all required environment variables are set."""
    missing = [v for v in _ALWAYS_REQUIRED if not os.environ.get(v)]
    has_key = bool(os.environ.get("SNOWFLAKE_PRIVATE_KEY_B64") or os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"))
    if not has_key:
        missing.append("SNOWFLAKE_PRIVATE_KEY_B64 or SNOWFLAKE_PRIVATE_KEY_PATH")
    if missing:
        print("[!] Missing required environment variables:")
        for v in missing:
            print(f"      - {v}")
        sys.exit(1)


def get_connection():
    """Build a Snowflake connection using key-pair auth from env vars.

    Supports two key sources (checked in order):
      1. SNOWFLAKE_PRIVATE_KEY_B64 — base64-encoded PEM key (production / ECS / K8s)
      2. SNOWFLAKE_PRIVATE_KEY_PATH — path to PEM file on disk (local dev)
    """
    check_env()

    key_b64 = (os.environ.get("SNOWFLAKE_PRIVATE_KEY_B64") or "").strip()
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

    if key_b64:
        key_bytes = base64.b64decode(key_b64)
    else:
        key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
        with open(key_path, "rb") as f:
            key_bytes = f.read()

    private_key = serialization.load_pem_private_key(
        key_bytes,
        password=passphrase.encode() if passphrase else None,
        backend=default_backend(),
    )

    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    params = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=private_key_bytes,
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=DATABASE,
    )

    role = os.environ.get("SNOWFLAKE_ROLE")
    if role:
        params["role"] = role

    return snowflake.connector.connect(**params)


def get_setup_path(marketplace_key: str) -> Path:
    """Return the path to a marketplace's setup SQL file.

    ``marketplace_key`` is the lowercase filename stem, e.g. ``"grailed"``
    or ``"google_shopping"``.
    """
    return SETUP_DIR / f"{marketplace_key}.sql"


def _execute_sql_file(conn, sql_path: Path, *, best_effort_all: bool = False):
    """Execute semicolon-delimited SQL statements from a file.

    When *best_effort_all* is True every failing statement is logged and
    skipped.  Otherwise only ``CREATE`` / ``ALTER`` errors are tolerated
    (to handle objects that already exist).
    """
    sql_text = sql_path.read_text()
    cur = conn.cursor()
    try:
        for raw_stmt in sql_text.split(";"):
            lines = [
                ln for ln in raw_stmt.splitlines()
                if ln.strip() and not ln.strip().startswith("--")
            ]
            stmt = "\n".join(lines).strip()
            if not stmt:
                continue

            stmt_upper = stmt.strip().upper()
            tolerant = best_effort_all or (
                stmt_upper.startswith("ALTER")
                or stmt_upper.startswith("CREATE")
            )
            try:
                cur.execute(stmt)
            except Exception as exc:
                if tolerant:
                    print(f"  [~] Skipped: {exc}")
                else:
                    raise
    finally:
        cur.close()


def run_setup(conn, sql_path):
    """Execute DDL statements from an arbitrary SQL file."""
    sql_path = Path(sql_path)
    print(f"[*] Running setup from {sql_path.name} ...")
    _execute_sql_file(conn, sql_path)
    print(f"[+] Setup complete ({sql_path.name}).\n")


def run_migrations(conn, schemas=None):
    """Run all migration scripts from shared/sql/migrations/ against each schema.

    Migration files are executed in sorted order (001_..., 002_..., etc.).
    Each migration is applied per schema with best-effort semantics so that
    already-applied changes (e.g. column already exists) are skipped.
    """
    schemas = schemas or ALL_SCHEMAS

    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migration_files:
        print("[*] No migration files found.\n")
        return

    cur = conn.cursor()
    try:
        for mig_path in migration_files:
            print(f"[*] Migration: {mig_path.name}")
            for schema in schemas:
                print(f"  -> {schema}")
                cur.execute(f"USE SCHEMA {schema}")
                _execute_sql_file(conn, mig_path, best_effort_all=True)
            print()
    finally:
        cur.close()
    print("[+] All migrations complete.\n")
