from __future__ import annotations

from datetime import date
from typing import Dict, Any
from dateutil.relativedelta import relativedelta

from ml_ettj26.utils.io.http import HTTPConfig, RequestsTransport
from ml_ettj26.utils.io.storage import LocalFileStorage
from ml_ettj26.extractors.bcb_sgs_raw import BcbSgsRawExtractor, BcbDemabNegociacoesRawExtractor


def extract_bcb_raw(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Baixa e salva RAW do BCB:
      - SGS (json) para séries (selic, ipca, ...)
      - DEMAB negociações (zip) por mês
    Retorna manifest com paths salvos.
    """
    http_cfg = HTTPConfig(
        timeout_sec=params.get("timeout_sec", 30),
        max_retries=params.get("max_retries", 4),
        backoff_sec=params.get("backoff_sec", 1.0),
        headers=params.get("headers"),
    )
    transport = RequestsTransport(http_cfg)

    raw_base_dir = params.get("raw_base_dir", "data/01_raw")
    storage = LocalFileStorage(raw_base_dir)

    sgs = BcbSgsRawExtractor(transport, storage)
    demab = BcbDemabNegociacoesRawExtractor(transport, storage)

    manifest: Dict[str, Any] = {"sgs": {}, "demab_negociacoes": []}

    # SGS
    sgs_params = params["sgs"]
    start = sgs_params.get("start")
    end = sgs_params.get("end")
    for name, sid in sgs_params["series"].items():
        path = sgs.fetch_and_store(series_id=int(sid), start=start, end=end)
        manifest["sgs"][name] = path

    # DEMAB - meses (opcional)
    demab_params = params.get("demab_negociacoes")
    if demab_params:
        tipo = demab_params.get("tipo", "T")
        start_month = demab_params["start_month"]  # "YYYY-MM"
        end_month = demab_params["end_month"]      # "YYYY-MM"

        y1, m1 = map(int, start_month.split("-"))
        y2, m2 = map(int, end_month.split("-"))
        cur = date(y1, m1, 1)
        endd = date(y2, m2, 1)

        while cur <= endd:
            manifest["demab_negociacoes"].append(
                demab.fetch_and_store_month(month=cur, tipo=tipo)
            )
            cur = cur + relativedelta(months=1)

    return manifest
