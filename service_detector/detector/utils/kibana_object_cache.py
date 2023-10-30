#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base
from datetime import timedelta
from cachetools import TTLCache
from collections import defaultdict

# Internal

# External
import requests


class KibanaObjectCache:
    _base_kibana_url = 'https://elastic-4.kb.europe-west3.gcp.cloud.es.io:9243'

    def __init__(self,
                 elastic_api_key: str,

                 cache_size: int = 1000,
                 cache_retention_window: timedelta = timedelta(days=1)):
        """
        Kibana cache object

        :param elastic_api_key: Elastic API key with access to Kibana
        """

        self.__elastic_api_key = elastic_api_key

        self._objects = TTLCache(maxsize=cache_size, ttl=cache_retention_window.total_seconds())

        self._setup()

    def get_dataviews_by_index(self, index: str) -> dict:
        """
        Get dataviews and spaces info by index name

        :param index: Index name
        :return: Space dict
        """

        space = self._objects.get(index)

        if space:
            return space

        self._setup()

        return self._objects.get(index)

    def _setup(self):
        """
        Setup indexes caches

        """

        objects = defaultdict(dict)

        spaces = self._get_all_spaces()
        for space in spaces:
            dataviews = self._get_all_dataviews(space)
            for index, dataview_id in dataviews.items():
                objects[index][dataview_id] = space

        for index, dataviews_spaces in objects.items():
            self._objects[index] = dataviews_spaces

    def _get_all_spaces(self) -> list[str]:
        """
        Get all spaces from Kibana

        :return: List of spaces names
        """

        spaces = requests.get(
            f'{self._base_kibana_url}/api/spaces/space',
            headers=self._kibana_headers()).json()

        return [space['id'] for space in spaces]

    def _get_all_dataviews(self, space_id: str) -> {str: str}:
        """
        Get all dataviews from Kibana space

        :param space_id: Space id from Kibana
        :return: Dict of {index_title: dataview_id}
        """

        dataviews = requests.get(
            f'{self._base_kibana_url}/s/{space_id}/api/data_views',
            headers=self._kibana_headers()).json()

        return {dataview['title']: dataview['id'] for dataview in dataviews['data_view']}

    def _kibana_headers(self) -> dict:
        """
        Generate Kibana headers

        :return: Kibana headers dict
        """

        return {
            'Authorization': f'ApiKey {self.__elastic_api_key}'
        }


if __name__ == '__main__':
    a = KibanaObjectCache('VFpIaE1JZ0JlVGZhS1NZYnF2QUU6ZDNORVJRSXVSUUNpelFPSkM3ZU9RZw==')

