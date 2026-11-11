"""Runtime settings, read from code_silver_update_status/.env (real environment variables take precedence)."""
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

CODE_DIR = Path(__file__).resolve().parents[1]


def _path(value):
    """Resolve a path from .env; relative paths are relative to the code_silver_update_status/ folder."""
    p = Path(value)
    return p if p.is_absolute() else (CODE_DIR / p).resolve()


def _list(value):
    return [v.strip() for v in (value or "").split(",") if v.strip()]


def _bool(value):
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass(frozen=True)
class DbSettings:
    host: str
    port: int
    name: str
    user: str
    password: str = None
    schema: str = "public"

    def __repr__(self):  # keep the password out of logs / tracebacks
        return f"DbSettings(host={self.host!r}, port={self.port}, name={self.name!r}, user={self.user!r})"


@dataclass(frozen=True)
class Settings:
    input_dir: Path
    fetched_dir: Path
    output_dir: Path
    keep_upcs: list          # '/' form (N/02015/03002/GJ); empty -> keep all UPCs
    refresh_db_tables: bool  # true -> always re-fetch; false -> reuse CSVs in fetched_dir
    db_tables: list
    db: DbSettings
    log_level: str


def load_settings(env_file=None):
    load_dotenv(env_file or CODE_DIR / ".env")
    env = os.environ.get

    return Settings(
        input_dir=_path(env("INPUT_DIR", "../input_silver_data")),
        fetched_dir=_path(env("FETCHED_DIR", "../fetched_db_tables")),
        output_dir=_path(env("OUTPUT_DIR", "../silver_data_with_status")),
        keep_upcs=_list(env("KEEP_UPCS")),
        refresh_db_tables=_bool(env("REFRESH_DB_TABLES", "false")),
        db_tables=_list(env("DB_TABLES", "udm_defect_reference,dlm_defect_case")),
        db=DbSettings(
            host=env("DB_HOST"),
            port=int(env("DB_PORT", "5432")),
            name=env("DB_NAME"),
            user=env("DB_USER"),
            password=env("DB_PASSWORD"),
            schema=env("DB_SCHEMA", "public"),
        ),
        log_level=env("LOG_LEVEL", "INFO"),
    )
