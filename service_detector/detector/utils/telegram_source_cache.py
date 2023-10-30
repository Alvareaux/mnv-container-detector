#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base
from cachetools import TTLCache
from datetime import timedelta

# Internal
from mnv_data_package_database.connections.mysql import ConnectionDB

# External


class TelegramSourceCache:
    telegram_source_table = 'telegram_source'

    def __init__(self,
                 connection_db_metrics: ConnectionDB,

                 cache_size: int = 100000,
                 cache_retention_window: timedelta = timedelta(hours=1),
                 ):
        """
        Source cache object

        :param connection_db_metrics: ConnectionDB object to ART database
        """

        self._connection_db_metrics = connection_db_metrics
        self._sources = TTLCache(maxsize=cache_size, ttl=cache_retention_window.total_seconds())

        self._get_sources_from_db()

    def get_source(self, chat_id: int) -> dict | None:
        """
        Get a Telegram source for chat_id from cache or None if not found

        :param chat_id: Chat ID to predict on
        :return: Source info or None
        """

        # Check if a source is in cache
        source = self._sources.get(chat_id)
        if source is not None:
            return source

        # If source is not in cache, try to get it from database
        self._get_sources_from_db()

        # Try to get a source from cache again
        # If a source is not in cache, return None
        prediction = self._sources.get(chat_id)
        return prediction

    def _get_sources_from_db(self):
        """
        Get all predictions from database

        :return:
        """

        table = self._connection_db_metrics.metadata.tables[self.telegram_source_table]
        for row in self._connection_db_metrics.session.query(table).all():
            self._sources[row.id] = {
                column_name: value for column_name, value in row._asdict().items()
                if column_name not in ['id']
            }
