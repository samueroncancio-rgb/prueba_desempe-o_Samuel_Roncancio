"""Airflow Models Param Shim for Local IDE Resolution"""
from typing import Any

class Param:
    def __init__(self, default: Any, *args: Any, **kwargs: Any):
        self.default = default
