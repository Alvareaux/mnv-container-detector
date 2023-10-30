#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base

# Internal
from service_detector.detector.types.anomaly import Anomaly


class Detector:
    type = None

    def run(self, article: dict, anomalies: list[Anomaly]):
        ...
