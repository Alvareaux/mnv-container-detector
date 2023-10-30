#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base
from uuid import uuid4
from datetime import datetime

import numpy as np

# Internal
from service_detector.detector.detectors._detector import Detector
from service_detector.detector.types import Anomaly, Alert
from service_detector.detector.utils import KibanaLinkGenerator

# External


class AnomalyDetector:
    _datetime_format = '%Y-%m-%dT%H:%M:%S'

    _anomaly_colors_by_score = {
        'red': 0.75,
        'yellow': 0.25,
        'green': 0.0
    }

    # TODO: Rework method field in article metadata
    _domain = {
        'TelegramEngagementExecutor': 'Telegram',
        'TelegramListener': 'Telegram',
        'opoint': 'Web'
    }

    def __init__(self, detectors: list[Detector], kibana_link_generator: KibanaLinkGenerator):
        """
        Anomaly detector

        :param detectors: List of detectors to run
        :param kibana_link_generator: KibanaLinkGenerator object
        """

        self._detectors = detectors
        self._kibana_link_generator = kibana_link_generator

    def run(self, message: dict) -> Alert | None:
        """
        Run anomaly detection on message

        :param message: Message dict (metadata + article)
        :return:
        """

        anomalies = self._run_detectors(message)
        if len(anomalies) == 0:
            return None

        alert = self._create_alert(message, anomalies)
        return alert

    def _run_detectors(self, message: dict) -> list[Anomaly]:
        """
        Run detectors on message

        :param message: Message dict (metadata + article)
        :return: List of anomalies
        """

        anomalies = []

        article = message['payload']

        for detector in self._detectors:
            detector.run(article, anomalies)

        return anomalies

    def _create_alert(self, message: dict, anomalies: list[Anomaly]) -> Alert:
        """
        Sum up anomalies to create alert

        :param anomalies: List of anomalies
        :return: Alert object
        """

        anomalies = sorted(anomalies, key=lambda x: x.score, reverse=True)
        total_score = self._calculate_total_anomaly_score(anomalies)

        destination = message['metadata']['destination']
        if isinstance(destination, str):
            destination = [destination]

        query_mapping = {'source': f'"{message["payload"]["source"]}"'}
        if message['payload'].get('delta'):
            query_mapping['delta'] = message['payload']['delta']

        kibana_urls = self._kibana_link_generator.generate(
            destination=destination[0],
            mapping=query_mapping,
            date=datetime.strptime(
                message['payload'].get(
                    'loading_date', message['payload']['date']
                ), self._datetime_format)
        )
        kibana_url = kibana_urls[0] if len(kibana_urls) > 0 else None

        alert = Alert(
            id=str(uuid4()),
            _article_id=message['metadata']['id'],

            date=datetime.strptime(
                message['payload'].get(
                    'loading_date', message['payload']['date']
                ), self._datetime_format),
            score=total_score,

            domain=self._domain.get(message['metadata']['method'], 'Unknown'),
            source=message['payload']['source'],

            description=self._generate_description(anomalies, message, total_score),
            url=kibana_url,

            field_name=anomalies[0].metric_name,
            anomaly_value=anomalies[0].metric_value,
            expected_value=anomalies[0].expected_value,

            _all_anomalies=[anomaly.to_dict() for anomaly in anomalies]
        )

        return alert

    def _calculate_total_anomaly_score(self, anomalies: list[Anomaly]) -> float:
        """
        Calculate total anomaly score

        :param anomalies: List of anomalies
        :return: Total anomaly score
        """

        # TODO: Implement scoring
        return sum([anomaly.score for anomaly in anomalies])

    def _generate_description(self, anomalies: list[Anomaly], message: dict, total_score: float) -> str:
        """
        Generate alert description

        :param anomalies: List of anomalies (better to sort by score)
        :return: Alert description
        """

        # TODO: Maybe some LLM magic here?
        # TODO: Collect feedback from users
        # TODO: Move text generation to detection classes

        additional_info = None
        if message['payload'].get('delta'):
            additional_info = f"{message['payload']['delta']} seconds after publication"

        if total_score > self._anomaly_colors_by_score['red']:
            prefix = 'Critical anomaly'
        elif total_score > self._anomaly_colors_by_score['yellow']:
            prefix = 'Major anomaly'
        elif total_score > self._anomaly_colors_by_score['green']:
            prefix = 'Minor anomaly'
        else:
            prefix = 'Warning anomaly'

        description_points = []
        for anomaly in anomalies:
            if type(anomaly.metric_value) in (float, int):
                if anomaly.expected_value == 0:
                    description_points.append(f"{anomaly.metric_name} ({anomaly.expected_value:.2f}x expected)")
                    continue

                difference = anomaly.metric_value / anomaly.expected_value
                description_points.append(f"{anomaly.metric_name} ({difference:.1f}x higher)")

            if isinstance(anomaly.metric_value, str):
                description_points.append(f"{anomaly.metric_name} is {anomaly.metric_value}")

        if len(description_points) == 1:
            anomalies = description_points[0]
        else:
            anomalies = ', '.join(description_points[:-1]) + f' and {description_points[-1]}'

        complete_text = f"{prefix} in {anomalies} found for {message['payload']['source']}"
        if additional_info  is not None:
            complete_text += f" {additional_info}"

        return complete_text
