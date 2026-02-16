from ml_ettj26.utils.io.http import HTTPConfig, RequestsTransport
from ml_ettj26.utils.io.storage import LocalFileStorage
from ml_ettj26.extractors.bcb_sgs_raw import BcbSgsRawExtractor

def main():
    http = RequestsTransport(HTTPConfig(timeout_sec=60, max_retries=4, backoff_sec=1.0))
    storage = LocalFileStorage("data/01_raw")
    sgs = BcbSgsRawExtractor(http, storage)

    start = "01/01/2018"
    end = "13/02/2026"

    selic_paths = sgs.fetch_and_store(432, start=start, end=end, out_dir="bcb/sgs")
    ipca_paths = sgs.fetch_and_store(433, start=start, end=end, out_dir="bcb/sgs")

    print("SELIC:", len(selic_paths), "arquivos")
    for p in selic_paths:
        print(" -", p)

    
    print("IPCA:", len(ipca_paths), "arquivos")
    for p in ipca_paths:
        print(" -", p)

if __name__ == "__main__":
    main()
