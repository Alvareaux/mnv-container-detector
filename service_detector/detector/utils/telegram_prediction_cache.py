#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base
from cachetools import TTLCache
from datetime import datetime, timedelta

# Internal
from mnv_data_package_database.connections.mysql import ConnectionDB

# External


class TelegramPredictionCache:
    telegram_prediction_table = 'metrics_telegram_prediction'
    telegram_statistic_table = 'metrics_telegram_statistic'

    telegram_coefficient_table = 'metrics_telegram_coefficient'

    def __init__(self,
                 connection_db_metrics: ConnectionDB,
                 prediction_step: int = 5,

                 cache_big_size: int = 500000,
                 cache_big_update_window: timedelta = timedelta(hours=1),
                 cache_big_retention_window: timedelta = timedelta(hours=1),

                 cache_small_size: int = 10000,
                 cache_small_update_window: timedelta = timedelta(hours=1),
                 cache_small_retention_window: timedelta = timedelta(hours=1),
                 ):
        """
        Prediction cache object

        :param connection_db_metrics: ConnectionDB object to ART database
        :param prediction_step:
        """

        self._connection_db_metrics = connection_db_metrics
        self._prediction_step = prediction_step

        self.cache_big_update_window = cache_big_update_window
        self.cache_big_retention_window = cache_big_retention_window.total_seconds()
        self.cache_small_update_window = cache_small_update_window
        self.cache_small_retention_window = cache_small_retention_window.total_seconds()

        self._predictions = TTLCache(maxsize=cache_big_size, ttl=self.cache_big_retention_window)
        self._statistics = TTLCache(maxsize=cache_small_size,  ttl=self.cache_small_retention_window)
        self._coefficients = TTLCache(maxsize=cache_small_size, ttl=self.cache_small_retention_window)

        now = datetime.now()

        self._get_predictions_from_db(now - self.cache_big_update_window, now + self.cache_big_update_window)
        self._get_statistics_from_db(now)
        self._get_coefficients_from_db()

    def get_prediction(self, date: datetime, chat_id: int, delta: int) -> dict | None:
        """
        Get prediction for date, chat_id and delta combination from cache or None if not found

        :param date: Date to predict on
        :param chat_id: Chat ID to predict on
        :param delta: Delta to predict on
        :return: Prediction dict
        """

        date = self._get_nearest_date(date)

        # Check if prediction is in cache
        prediction = self._predictions.get((date, chat_id, delta))
        if prediction is not None:
            return prediction

        # If prediction is not in cache, try to get it from database with cache update window
        self._get_predictions_from_db(date - self.cache_big_update_window, date + self.cache_big_update_window)

        # Try to get prediction from cache again
        # If prediction is not in cache, return None
        prediction = self._predictions.get((date, chat_id, delta))
        return prediction

    def get_statistics(self, date: datetime, chat_id: int, delta: int, metric: str) -> dict | None:
        """
        Get statistic for date, chat_id, delta and metric combination from cache or None if not found

        :param date: Date for statistic
        :param chat_id: Chat ID for statistic
        :param delta: Delta for statistic
        :param metric: Metric name
        :return:
        """

        def get_latest_statistics():
            # Check if statistic is in cache
            rows_for_date = {}
            for row in self._statistics:
                if row[0] < date < row[1] and row[2] == chat_id and row[3] == delta and row[4] == metric:
                    rows_for_date[row[1]] = self._statistics[row]

            if len(rows_for_date) > 0:
                return sorted(rows_for_date.items(), key=lambda x: x[0])[-1][1]

            else:
                return None

        # Check if statistic is in cache
        statistic = get_latest_statistics()
        if statistic is not None:
            return statistic

        # If statistic is not in cache, try to get it from database with cache update window
        self._get_statistics_from_db(date)

        # Try to get statistic from cache again
        # If statistic is not in cache, return None
        statistic = get_latest_statistics()
        return statistic

    def get_coefficients(self, chat_id: int) -> dict | None:
        """
        Get coefficients for chat_id from cache or None if not found

        :param chat_id: Chat ID to get coefficients for
        :return:
        """

        # Check if coefficients are in cache
        coefficients = self._coefficients.get(chat_id)
        if coefficients is not None:
            return coefficients

        # If coefficients are not in cache, try to get them from database with cache update window
        self._get_coefficients_from_db()

        # Try to get coefficients from cache again
        # If coefficients are not in cache, return None
        coefficients = self._coefficients.get(chat_id)
        return coefficients

    def _get_predictions_from_db(self, from_date: datetime = None, to_date: datetime = None):
        """
        Get all predictions from database

        :return:
        """

        table = self._connection_db_metrics.metadata.tables[self.telegram_prediction_table]
        for row in self._connection_db_metrics.session.query(table).filter(
                table.c.date >= from_date,
                table.c.date <= to_date
        ).all():
            self._predictions[(row.date.replace(second=0, microsecond=0), row.chat_id, row.delta)] = {
                column_name: value for column_name, value in row._asdict().items()
                if column_name not in ['date', 'chat_id', 'delta']
            }

    def _get_statistics_from_db(self, date: datetime):
        """
        Get statistics from the database for given date

        :param date: Date to get statistics for
        :return:
        """

        table = self._connection_db_metrics.metadata.tables[self.telegram_statistic_table]
        for row in self._connection_db_metrics.session.query(table).filter(
                table.c.date_from <= date,
                table.c.date_to >= date,
        ).all():
            self._statistics[(row.date_from, row.date_to, row.chat_id, row.delta, row.metric)] = {
                column_name: value for column_name, value in row._asdict().items()
                if column_name not in ['chat_id', 'delta', 'metric', 'date_from', 'date_to']
            }

    def _get_coefficients_from_db(self):
        """
        Get all coefficients from database

        :return:
        """

        table = self._connection_db_metrics.metadata.tables[self.telegram_coefficient_table]
        for row in self._connection_db_metrics.session.query(table).all():
            self._coefficients[row.id] = {
                column_name: value for column_name, value in row._asdict().items()
                if column_name not in ['id']
            }

    def _get_nearest_date(self, dt: datetime) -> datetime:
        """
        Get the nearest date to predict on, rounded to prediction step minutes

        :param dt: Date to predict on
        :return:
        """

        minutes = dt.minute
        subtract_minutes = minutes % self._prediction_step

        return dt.replace(minute=minutes - subtract_minutes, second=0)
