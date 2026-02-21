from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional

@dataclass(frozen=True)
class PriceReportDerivative:
    # PK
    ticker: str

    contract_code: str

    type: str
    exchange: str
    month_code: str
    year_code: str
    maturity_month: date
    maturity_date: date
    source: str = "B3_PRICE_REPORT"

@dataclass(frozen=True)
class ContractSpecification:
    # PK
    contract_code: str

    currency: str = 'BRL'
    day_count_convention: str = 'BUS/252'
    roll_rule: str = '1st BUSINESS_DAY'
    price_quote_convention: str = 'PU'
    contract_size: Optional[int] = None

@dataclass(frozen=True)
class PriceReportQuoteDaily:
    # PK
    trade_date: date
    ticker: str

    adjstd_val_ctrct: Optional[Decimal]
    min_price: Optional[float]
    max_price: Optional[float]
    last_price: Optional[float]
    avg_price: Optional[float]

    trade_qnty: Optional[int]
    opn_intrst: Optional[int]
    oscn_pctg: Optional[float]
    vartn_pts: Optional[float]

    file_hash: str
    ingestion_ts_utc: str
