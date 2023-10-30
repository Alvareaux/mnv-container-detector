#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base
from dataclasses import dataclass, asdict, field
from typing import Any
from datetime import datetime


@dataclass
class Alert:
    """
    Alert data class

    """

    id: str  # Alert UUID

    date: datetime  # Message loading date == anomaly date
    score: float  # Total anomaly score, from 0 to 1.0

    domain: str  # Domain of anomaly (e.g. Web, Telegram)  # TODO: Map index - domain in database
    source: str  # Source of anomaly (e.g. Telegram channel, news website)

    description: str  # Alert description
    url: str  # Kibana URL to data  # TODO: Map index - kibana uuid in database
    # https://discuss.elastic.co/t/discover-url-without-uuid/169379

    field_name: str
    anomaly_value: Any
    expected_value: Any

    _article_id: str  # Article UUID
    _all_anomalies: field(default_factory=list)  # List of all anomalies

    def to_dict(self) -> dict:
        """
        Convert anomaly to dict

        :return: Anomaly dict
        """

        return {k: str(v) for k, v in asdict(self).items()}
