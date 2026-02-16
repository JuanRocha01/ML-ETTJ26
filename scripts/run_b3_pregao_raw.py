from __future__ import annotations

from datetime import date

from ml_ettj26.utils.io.http import HttpTransport
from ml_ettj26.utils.io.storage import ByteStorage

from ml_ettj26.extractors.b3_pregao_specs import PRICE_REPORT, SWAP_MARKET_RATES
from ml_ettj26.extractors.b3_pregao_raw import (
    B3PregaoDailyRawExtractor,
    B3PregaoHttpConfig,
)


def main() -> None:
    # Deixe isso explícito para evitar surpresa com cwd:
    # execute: python -m ml_ettj26.extractors.run_b3_pregao
    start_day = date(2020, 1, 1)
    end_day = date.today()

    transport = HttpTransport(timeout_sec=30, max_retries=4, backoff_sec=1.0)
    storage = ByteStorage()  # seu storage real

    cfg = B3PregaoHttpConfig(
        project_root=None,   # usa Path.cwd() -> raiz do projeto (recomendado)
        strict=False,        # pula dias sem arquivo
    )

    pr = B3PregaoDailyRawExtractor(transport, storage, PRICE_REPORT, cfg)
    ts = B3PregaoDailyRawExtractor(transport, storage, SWAP_MARKET_RATES, cfg)

    pr_paths = pr.fetch_and_store(start_day=start_day, end_day=end_day)
    ts_paths = ts.fetch_and_store(start_day=start_day, end_day=end_day)

    print(f"PriceReport: {len(pr_paths)} arquivos")
    print(f"SwapMarketRates: {len(ts_paths)} arquivos")
    print(f"Total: {len(pr_paths) + len(ts_paths)} arquivos")


if __name__ == "__main__":
    main()
