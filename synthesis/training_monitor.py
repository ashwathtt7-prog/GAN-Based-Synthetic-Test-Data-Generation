"""
Helpers for streaming model training metrics while SDV models are fitting.
"""

from __future__ import annotations

import threading
import time

import pandas as pd


class LossPollingMonitor:
    """Poll a synthesizer's underlying loss dataframe and emit unseen epochs."""

    def __init__(self, synthesizer, model_type: str, emit_metric, poll_interval: float = 0.5):
        self.synthesizer = synthesizer
        self.model_type = model_type
        self.emit_metric = emit_metric
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._seen_keys = set()

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._thread.join(timeout=2)
        self._flush()

    def _run(self):
        while not self._stop_event.is_set():
            self._flush()

            time.sleep(self.poll_interval)

    def _flush(self):
        model = getattr(self.synthesizer, "_model", None)
        loss_values = getattr(model, "loss_values", None)
        if isinstance(loss_values, pd.DataFrame) and not loss_values.empty:
            self._emit_unseen_rows(loss_values.copy())

    def _emit_unseen_rows(self, loss_values: pd.DataFrame):
        if self.model_type == "ctgan":
            grouped = loss_values.groupby("Epoch", as_index=False).last()
            for _, row in grouped.iterrows():
                key = int(row["Epoch"])
                if key in self._seen_keys:
                    continue
                self._seen_keys.add(key)
                discriminator_loss = row.get("Discriminator Loss", row.get("Distriminator Loss"))
                self.emit_metric({
                    "epoch": key + 1,
                    "model_type": "ctgan",
                    "generator_loss": float(row["Generator Loss"]),
                    "discriminator_loss": float(discriminator_loss),
                })
            return

        grouped = loss_values.groupby("Epoch", as_index=False)["Loss"].mean()
        for _, row in grouped.iterrows():
            key = int(row["Epoch"])
            if key in self._seen_keys:
                continue
            self._seen_keys.add(key)
            self.emit_metric({
                "epoch": key + 1,
                "model_type": "tvae",
                "loss": float(row["Loss"]),
            })
