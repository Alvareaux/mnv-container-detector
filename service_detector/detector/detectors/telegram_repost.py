#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base

# Internal
from service_detector.detector.detectors._detector import Detector
from service_detector.detector.utils import TelegramSourceCache
from service_detector.detector.types.anomaly import Anomaly

# External
from redis import Redis


class TelegramRepostAnomalyDetector(Detector):
    _datetime_format = '%Y-%m-%dT%H:%M:%S'
    __must_have_fields = ['country', 'forward_from_chat_id']
    __none_country = ('xx', None)

    def __init__(self, redis_connection: Redis):
        """
        Telegram metric_name anomaly detector

        :param redis_connection: Redis connection pool object
        """

        self._redis_connection = redis_connection

    def run(self, article: dict, anomalies: list[Anomaly]):
        """
        Detect anomalies in Telegram metrics for an article based on metrics

        :param article: Input article
        :param anomalies: Anomalies list
        :return:
        """

        # If article is not valid, we can't detect anomalies for this article
        if not self._check_article_fields(article):
            return

        # Check forward country
        if self._check_original_chat_country(article):
            anomalies.append(
                Anomaly(
                    metric_name='forward',
                    metric_value=1,
                    expected_value=0,
                    score=1.0,
                )
            )

    def _check_original_chat_country(self, article: dict) -> bool:
        """
        Check if original chat country and forward chat country are different

        :param article: Input article
        :return: True if countries are different
        """

        original_country = article['country']
        forward_country = self._redis_connection.hget('tg_countries', article['forward_from_chat_id'])

        if original_country in self.__none_country or forward_country in self.__none_country:
            return False

        return not (original_country != forward_country)

    def _check_article_fields(self, article: dict) -> bool:
        """
        Check if detector should check an article based on field presence

        :param article: Input article
        :return: True if detector should check an article
        """

        return all(article.get(key) for key in self.__must_have_fields)
