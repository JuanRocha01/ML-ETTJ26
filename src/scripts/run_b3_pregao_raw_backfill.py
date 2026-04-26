from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, timedelta, datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

from ml_ettj26.utils.io.http import HTTPConfig, RequestsTransport
from ml_ettj26.utils.io.storage import LocalFileStorage
from ml_ettj26.utils.io.storage import ByteStorage  # ou LocalFileStorage, conforme seu projeto
from ml_ettj26.utils.io.paths import project_root

from ml_ettj26.extractors.b3_pregao_specs import PRICE_REPORT, SWAP_MARKET_RATES
from ml_ettj26.extractors.b3_pregao_raw import (
    B3PregaoDailyRawExtractor,
    B3PregaoHttpConfig,
    DownloadError,
)


# ----------------------------
# Logging estruturado (JSON)
# ----------------------------

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base: Dict[str, Any] = {
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # campos extras (via logger.info(..., extra={...}))
        for k, v in record.__dict__.items():
            if k in ("msg", "args", "levelname", "levelno", "name", "pathname", "filename",
                     "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
                     "created", "msecs", "relativeCreated", "thread", "threadName",
                     "processName", "process"):
                continue
            # só serializa tipos simples (evita crash)
            try:
                json.dumps(v)
                base[k] = v
            except Exception:
                base[k] = str(v)

        if record.exc_info:
            base["exc"] = self.formatException(record.exc_info)

        return json.dumps(base, ensure_ascii=False)


def build_logger(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("b3_pregao_backfill")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(JsonFormatter())
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(JsonFormatter())
    logger.addHandler(ch)

    return logger


# ----------------------------
# Config / helpers
# ----------------------------

@dataclass(frozen=True)
class BackfillConfig:
    start_day: date = date(2020, 1, 1)
    end_day: Optional[date] = None  # default: today
    strict: bool = False            # False = pula dias sem dados/zip vazio
    timeout_sec: int = 30
    max_retries: int = 2
    backoff_sec: float = 0.5

    # Onde salvar logs e manifest
    log_relpath: str = "data/99_logs/b3/pregao_backfill.jsonl"
    manifest_relpath: str = "data/99_logs/b3/pregao_manifest.jsonl"


def daterange(d0: date, d1: date):
    cur = d0
    while cur <= d1:
        yield cur
        cur += timedelta(days=1)


def write_manifest_line(manifest_path: Path, payload: Dict[str, Any]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _summarize_counts(counts: Dict[str, int]) -> Dict[str, int]:
    # garante chaves estáveis
    keys = [
        "ok_price_report", "skip_price_report", "err_price_report",
        "ok_swap", "skip_swap", "err_swap",
        "days_total",
    ]
    return {k: int(counts.get(k, 0)) for k in keys}


# ----------------------------
# Runner principal
# ----------------------------

def main() -> None:
    cfg = BackfillConfig()
    root = project_root()

    end_day = cfg.end_day or date.today()

    log_path = root / cfg.log_relpath
    manifest_path = root / cfg.manifest_relpath
    logger = build_logger(log_path)

    logger.info(
        "starting backfill",
        extra={
            "start_day": cfg.start_day.isoformat(),
            "end_day": end_day.isoformat(),
            "strict": cfg.strict,
            "project_root": str(root),
        },
    )

    # Infra (transport/storage)
    transport = RequestsTransport(
        HTTPConfig(
            timeout_sec=cfg.timeout_sec,
            max_retries=cfg.max_retries,
            backoff_sec=cfg.backoff_sec,
        )
    )
    storage = LocalFileStorage(Path("data/01_raw/b3"))  # se você usa LocalFileStorage, troque aqui

    # Extractors (um por dataset)
    e_cfg = B3PregaoHttpConfig(project_root=root, strict=cfg.strict)

    pr_ex = B3PregaoDailyRawExtractor(transport, storage, PRICE_REPORT, e_cfg)
    sw_ex = B3PregaoDailyRawExtractor(transport, storage, SWAP_MARKET_RATES, e_cfg)

    counts: Dict[str, int] = {"days_total": 0}

    t0 = datetime.utcnow()

    for d in daterange(cfg.start_day, end_day):
        counts["days_total"] += 1

        # ---- PriceReport ----
        pr_status = "skip"
        pr_path: Optional[str] = None
        pr_err: Optional[str] = None

        try:
            paths = pr_ex.fetch_and_store(day=d)
            if paths:
                pr_status = "ok"
                pr_path = paths[0]
                counts["ok_price_report"] = counts.get("ok_price_report", 0) + 1
            else:
                counts["skip_price_report"] = counts.get("skip_price_report", 0) + 1

        except DownloadError as exc:
            pr_status = "error"
            pr_err = str(exc)
            counts["err_price_report"] = counts.get("err_price_report", 0) + 1

        # ---- Swap ----
        sw_status = "skip"
        sw_path: Optional[str] = None
        sw_err: Optional[str] = None

        try:
            paths = sw_ex.fetch_and_store(day=d)
            if paths:
                sw_status = "ok"
                sw_path = paths[0]
                counts["ok_swap"] = counts.get("ok_swap", 0) + 1
            else:
                counts["skip_swap"] = counts.get("skip_swap", 0) + 1

        except DownloadError as exc:
            sw_status = "error"
            sw_err = str(exc)
            counts["err_swap"] = counts.get("err_swap", 0) + 1

        # Log por dia (uma linha JSON)
        logger.info(
            "day processed",
            extra={
                "day": d.isoformat(),
                "price_report": {"status": pr_status, "path": pr_path, "error": pr_err},
                "swap": {"status": sw_status, "path": sw_path, "error": sw_err},
            },
        )

        # Manifest (também JSONL, útil para reprocessamento)
        write_manifest_line(
            manifest_path,
            {
                "day": d.isoformat(),
                "price_report": {"status": pr_status, "path": pr_path, "error": pr_err},
                "swap": {"status": sw_status, "path": sw_path, "error": sw_err},
            },
        )

    elapsed = (datetime.utcnow() - t0).total_seconds()

    logger.info(
        "backfill finished",
        extra={
            "elapsed_sec": elapsed,
            "counts": _summarize_counts(counts),
            "log_path": str(log_path),
            "manifest_path": str(manifest_path),
        },
    )


if __name__ == "__main__":
    main()
