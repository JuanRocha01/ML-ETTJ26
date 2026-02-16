from __future__ import annotations

import logging
from typing import Any, Dict

import pandas as pd
from kedro.framework.hooks import hook_impl

log = logging.getLogger(__name__)


class DataObservabilityHooks:
    """Hooks leves de observabilidade para ajudar debugging e governança."""

    @hook_impl
    def before_node_run(self, node, inputs: Dict[str, Any], is_async: bool, **kwargs):
        log.info("Starting node: %s", node.name)

    @hook_impl
    def after_node_run(self, node, outputs: Dict[str, Any], inputs: Dict[str, Any], **kwargs):
        log.info("Finished node: %s", node.name)

        # Loga estatísticas simples de outputs
        for name, out in outputs.items():
            if isinstance(out, pd.DataFrame):
                log.info(
                    "Output %s: DataFrame shape=%s cols=%s",
                    name, out.shape, list(out.columns)
                )
                if out.empty:
                    log.warning("Output %s is EMPTY (node=%s)", name, node.name)
