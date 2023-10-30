#!/usr/bin/env python
# -*- coding: utf-8 -*-

from service_detector.detector.detectors.telegram_metrics import TelegramMetricAnomalyDetector
from service_detector.detector.detectors.telegram_repost import TelegramRepostAnomalyDetector

__all__ = [
    'TelegramMetricAnomalyDetector',
    'TelegramRepostAnomalyDetector',
]
