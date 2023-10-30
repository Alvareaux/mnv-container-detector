#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base
from datetime import datetime

# Internal
from service_detector.detector.detectors._detector import Detector
from service_detector.detector.utils import TelegramPredictionCache
from service_detector.detector.types.anomaly import Anomaly

# External
import numpy as np


class TelegramMetricAnomalyDetector(Detector):
    _datetime_format = '%Y-%m-%dT%H:%M:%S'
    __must_have_fields = ['chat_id', 'delta', 'loading_date', 'views', 'forwards', 'reaction_count']

    anomaly_weights = {
        'predicted_metrics': 200,
        'static_metrics': 100
    }

    def __init__(self, telegram_prediction_cache: TelegramPredictionCache):
        """
        Telegram metric_name anomaly detector

        :param telegram_prediction_cache: Telegram prediction cache object
        """

        self._telegram_prediction_cache = telegram_prediction_cache

    def run(self, article: dict, anomalies: list[Anomaly]):
        """
        Detect anomalies in Telegram metrics for a article based on metrics

        :param article: Input article
        :param anomalies: Anomalies list
        :return:
        """

        # If article is not valid, we can't detect anomalies for this article
        if not self._check_article_fields(article):
            return

        # If article has minimal threshold for anomaly detection
        if not self._check_article_minimal_threshold(article):
            return

        # Check static metrics
        self._check_static_metrics(article, anomalies)

        # Check predicted metrics
        self._check_predicted_metrics(article, anomalies)

    def _check_static_metrics(self, article: dict, anomalies: list[Anomaly]):
        """
        Check static metrics in article

        :param article: Input article
        :param anomalies: Anomalies list
        :return:
        """

        # Get coefficients for the chat
        coefficients = self._telegram_prediction_cache.get_coefficients(
            chat_id=article['chat_id'],
        )

        # If no coefficients found, we can't detect anomalies for this article
        if coefficients is None:
            return

        # Calculate metrics
        forwards_by_views = article['forwards'] / article['views']
        reaction_count_by_views = article['reaction_count'] / article['views']

        if forwards_by_views > coefficients['forwards_by_views']:
            anomalies.append(
                Anomaly(
                    metric_name='forwards_by_views',
                    metric_value=forwards_by_views,
                    expected_value=coefficients['forwards_by_views'],
                    score=self._score_static_metrics(
                        metric_value=forwards_by_views,
                        expected_value=coefficients['forwards_by_views']
                    ),

                    _weight=self.anomaly_weights['static_metrics']
                )
            )

        if reaction_count_by_views > coefficients['reaction_count_by_views']:
            anomalies.append(
                Anomaly(
                    metric_name='reaction_count_by_views',
                    metric_value=reaction_count_by_views,
                    expected_value=coefficients['reaction_count_by_views'],
                    score=self._score_static_metrics(
                        metric_value=reaction_count_by_views,
                        expected_value=coefficients['reaction_count_by_views']
                    ),

                    _weight=self.anomaly_weights['static_metrics']
                )
            )

    def _check_predicted_metrics(self, article: dict, anomalies: list[Anomaly]):
        """
        Check predicted metrics in article

        :param article: Input article
        :param anomalies: Anomalies list
        :return:
        """

        # Get nearest prediction for the article
        prediction = self._telegram_prediction_cache.get_prediction(
            date=datetime.strptime(article['loading_date'], self._datetime_format),
            chat_id=article['chat_id'],
            delta=article['delta']
        )

        statistics = self._telegram_prediction_cache.get_statistics(
            date=datetime.strptime(article['loading_date'], self._datetime_format),
            chat_id=article['chat_id'],
            delta=article['delta'],
            metric='views'
        )

        # If no prediction found, we can't detect anomalies for this article
        if prediction is None:
            return

        if article['views'] > prediction['views_upper']:
            anomalies.append(
                Anomaly(
                    metric_name='views',
                    metric_value=article['views'],
                    expected_value=prediction['views'],
                    score=self._score_predicted_metrics(
                        metric_value=article['views'],
                        expected_value=prediction['views'],
                        statistics=statistics
                    ),

                    _weight=self.anomaly_weights['predicted_metrics']
                )
            )

    def _score_predicted_metrics(self, metric_value: float, expected_value: float, statistics: dict) -> float:
        """
        Calculate anomaly score for predicted metrics

        :param metric_value: Metric value
        :param expected_value: Expected value
        :param statistics: Statistics dict
        :return: Anomaly score
        """

        # TODO: Rework?

        x_offset = 2
        x_stretch = 1.5

        deviation = metric_value / expected_value

        if statistics:
            z_score = (deviation - statistics['mean']) / statistics['std']
        else:
            z_score = deviation

        return 1 / (1 + np.exp(-z_score / x_stretch + x_offset))

    def _score_static_metrics(self, metric_value: float, expected_value: float) -> float:
        """
        Calculate anomaly score

        :param metric_value: Metric value
        :param expected_value: Expected value
        :return: Anomaly score
        """

        # TODO: Rework

        deviation = metric_value / expected_value
        return 1 / (1 + np.exp((-deviation + expected_value) * 10))

    def _check_article_minimal_threshold(self, article: dict) -> bool:
        """
        Check if article has minimal threshold for anomaly detection

        :param article: Input article
        :return: True if article has minimal threshold for anomaly detection
        """

        # Get coefficients for the chat
        coefficients = self._telegram_prediction_cache.get_coefficients(
            chat_id=article['chat_id'],
        )

        # If no coefficients found, we can't detect anomalies for this article
        if coefficients is None:
            return False

        # Check if article views are too low
        if article['views'] < coefficients.get('minimal_views_threshold', 0):
            return False

        return True

    def _check_article_fields(self, article: dict) -> bool:
        """
        Check if detector should check an article based on field presence

        :param article: Input article
        :return: True if detector should check an article
        """

        return all(article.get(key) for key in self.__must_have_fields)
