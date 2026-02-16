from datetime import date
from pathlib import Path

from ml_ettj26.extractors.b3_raw import B3RawExtractor

class DiFuturoRawExtractor:
    def __init__(self, b3raw: B3RawExtractor):
        self.b3raw = b3raw

    def fetch_daily_zip(self, ref_date: date) -> Path:
        return self.b3raw.fetch_and_store_zip("settlement_adjustments", ref_date)
