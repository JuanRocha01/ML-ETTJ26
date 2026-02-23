import re
from datetime import datetime, timezone

RE_ISO = re.compile(rb"(20\d{2}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?)")

def parse_snapshot_ts_from_head(head: bytes) -> datetime | None:
    """
    Busca timestamps ISO no header; pega o maior que encontrar.
    """
    hits = RE_ISO.findall(head)
    if not hits:
        return None

    dts = []
    for h in hits:
        s = h.decode("utf-8")
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            # assume UTC se não tiver timezone (melhor do que naive)
            dt = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        dts.append(dt)

    return max(dts)
