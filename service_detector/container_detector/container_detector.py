#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base
import json
import gc

# Import from project
from pathlib import Path
import sys
working_dir = Path(__file__).parents[2].resolve()
sys.path.append(str(working_dir))

# Internal
from service_detector.detector.detector import AnomalyDetector
from service_detector.detector.detectors import (
    TelegramMetricAnomalyDetector,
    TelegramRepostAnomalyDetector,
)

from service_detector.detector.utils import (
    TelegramPredictionCache,
    KibanaObjectCache,
    KibanaLinkGenerator
)

# External
from mnv_data_container_template.container_template import Container
from mnv_queue_adapter.types import ACK, QueueMessage
from redis import Redis, ConnectionPool


class ContainerDetector(Container):
    __version__ = '1.0.0'

    _service_name = 'detector'

    def __init__(self):
        """
        Unifier container object

        """

        # Required config groups for container
        self._config_services_groups = ['elasticsearch', 'redis']
        self._config_routing_groups = ['detector', 'versioning']

        # Setup params
        self._setup_pubsub = True
        self._setup_rabbitmq = False
        self._setup_versioning = True
        self._versioning_target = 'pubsub'

        super().__init__()

    def _setup_container(self):
        """
        Additional setup for service_unifier container

        :return:
        """

        # Setup routing keys
        # self.input_subscription = self._routing_values['detector_pubsub_subscription']
        self.input_subscription = 'data-pipeline-detector-attached'

        # Setup Redis connection
        self._setup_redis_connection()

        # Setup additional db connection
        self._setup_connection_db_metrics()

        # Setup anomaly detector
        self._setup_anomaly_detector()

    def _setup_redis_connection(self):
        """
        Setup Redis connection

        :return:
        """

        hostname = self._service_values['redis_hostname']
        port = self._service_values['redis_port']
        db = self._service_values['redis_database']

        redis_connection_pool = ConnectionPool(host=hostname, port=port, db=db)
        self.connection_redis = Redis(connection_pool=redis_connection_pool)

    def _setup_connection_db_metrics(self):
        """
        Setup connection to metrics database

        """
        import os
        from mnv_data_package_database.connections.mysql import ConnectionDB

        assert 'METRICS_MYSQL_HOSTNAME' in os.environ, 'METRICS_MYSQL_HOSTNAME is not set'
        assert 'METRICS_MYSQL_DATABASE' in os.environ, 'METRICS_MYSQL_DATABASE is not set'
        assert 'METRICS_MYSQL_LOGIN' in os.environ, 'METRICS_MYSQL_LOGIN is not set'

        db_hostname = os.environ['METRICS_MYSQL_HOSTNAME']

        if 'METRICS_MYSQL_PORT' in os.environ:
            port = os.environ['METRICS_MYSQL_PORT']
            db_hostname = f'{db_hostname}:{port}'

        db_database = os.environ['METRICS_MYSQL_DATABASE']
        db_login = os.environ['METRICS_MYSQL_LOGIN']
        db_password = self._gcloud_secrets.get_secret('pipeline-data-password-sql-art')

        self.connection_db_metrics = ConnectionDB(db_login, db_password, db_hostname, db_database)

    def _setup_anomaly_detector(self):
        """
        Setup anomaly detector

        :return:
        """

        telegram_prediction_cache = TelegramPredictionCache(self.connection_db_metrics)
        kibana_object_cache = KibanaObjectCache(
            elastic_api_key=self._gcloud_secrets.get_secret('pipeline-data-key-elasticsearch')
        )

        detectors = [
            TelegramMetricAnomalyDetector(telegram_prediction_cache=telegram_prediction_cache),
            TelegramRepostAnomalyDetector(redis_connection=self.connection_redis),
        ]

        kibana_link_generator = KibanaLinkGenerator(kibana_object_cache=kibana_object_cache)
        self.anomaly_detector = AnomalyDetector(detectors=detectors, kibana_link_generator=kibana_link_generator)

    def _run_anomaly_detection(self, message: QueueMessage):
        """
        Run anomaly detection on article

        :param message: Input message
        :return:
        """

        message = json.loads(message.message, strict=False)
        alert = self.anomaly_detector.run(message)
        if not alert:
            print(f"Message {message['metadata']['id']} has no alerts")
            return

        # TODO: remove this
        # print(f"Alert {alert.id} created")
        self._publish(json.dumps(alert.to_dict()), 'data-pipeline-alerts')

    def run(self):
        """
        Start consumer

        """

        with self._pubsub_client.publisher() as self._queue_pub:
            with self._pubsub_client.subscriber(self.input_subscription) as (queue_sub, queue_ack):
                while True:
                    message = queue_sub.get()
                    self._run_anomaly_detection(message)
                    queue_ack.put((ACK.ACK, message))

    def _setup_gcloud_credentials(self):
        import google
        self._gcloud_credentials, _ = google.auth.default()


if __name__ == '__main__':
    processor = ContainerDetector()
    processor.run()
