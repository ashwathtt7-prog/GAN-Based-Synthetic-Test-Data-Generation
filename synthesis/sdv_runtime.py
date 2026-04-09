"""
Runtime guards for SDV/CTGAN on local Windows execution.

The default CTGAN transformer parallelizes preprocessing with joblib once the
row count reaches 500 rows. On this machine that repeatedly caused pickling
and subprocess cleanup failures, so we force synchronous transforms and keep
BLAS thread counts low to reduce memory spikes.
"""

from __future__ import annotations

import os


for env_var in (
    "OPENBLAS_NUM_THREADS",
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ.setdefault(env_var, "1")


_RUNTIME_CONFIGURED = False


def configure_sdv_runtime() -> None:
    """Apply one-time runtime patches for CTGAN/TVAE stability."""
    global _RUNTIME_CONFIGURED
    if _RUNTIME_CONFIGURED:
        return

    import numpy as np
    import pandas as pd
    from ctgan.data_transformer import DataTransformer

    if getattr(DataTransformer.transform, "__name__", "") != "_codex_synchronous_transform":
        def _codex_synchronous_transform(self, raw_data):
            if not isinstance(raw_data, pd.DataFrame):
                column_names = [str(num) for num in range(raw_data.shape[1])]
                raw_data = pd.DataFrame(raw_data, columns=column_names)

            column_data_list = self._synchronous_transform(raw_data, self._column_transform_info_list)
            return np.concatenate(column_data_list, axis=1).astype(float)

        DataTransformer.transform = _codex_synchronous_transform

    _RUNTIME_CONFIGURED = True
