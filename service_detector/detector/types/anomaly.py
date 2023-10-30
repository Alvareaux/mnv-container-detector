#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base
from dataclasses import dataclass, asdict


@dataclass
class Anomaly:
    """
    Anomaly data class

    """

    metric_name: str
    metric_value: float | int
    expected_value: float | int | None

    score: float

    _weight: int = 100

    def to_dict(self) -> dict:
        """
        Convert anomaly to dict

        :return: Anomaly dict
        """

        return {k: str(v) for k, v in asdict(self).items()}
