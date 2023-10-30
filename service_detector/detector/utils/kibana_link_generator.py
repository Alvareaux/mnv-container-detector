#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base
from datetime import datetime, timedelta
import urllib.parse

# Internal
from service_detector.detector.utils import KibanaObjectCache

# External


class KibanaLinkGenerator:
    time_window = timedelta(minutes=5)

    _time_format = '%Y-%m-%dT%H:%M:%S.000Z'
    _base_kibana_url = 'https://elastic-4.kb.europe-west3.gcp.cloud.es.io:9243'

    def __init__(self, kibana_object_cache: KibanaObjectCache):
        """
        Kibana link generator

        :param kibana_object_cache: KibanaObjectCache object
        """

        self._kibana_object_cache = kibana_object_cache

    def generate(self, destination: str, mapping: dict, date: datetime) -> list[str]:
        """
        Generate Kibana links

        :param destination: Destination index name
        :param mapping: Query mapping (only for 'and' queries
        :param date: Date for Discover time window
        :return: List of Kibana links for every dataview
        """

        dataviews = self._kibana_object_cache.get_dataviews_by_index(destination)
        query = self._query_builder(mapping)

        return [
            self._url_builder(
                query=query,

                space_id=space_id,
                dataview_id=dataview_id,

                from_date=date - self.time_window,
                to_date=date + self.time_window
            ) for dataview_id, space_id in dataviews.items()
        ]

    def _query_builder(self, mapping: dict):
        """
        Build simple query string for K1bana Query Language.
        Add your own quoting if needed!

        :param mapping: Mapping dict
        :return: Query string (not URL encoded)
        """

        return ' and '.join([f'{key} : {value}' for key, value in mapping.items()])

    def _url_builder(self, query: str, space_id: str, dataview_id: str, from_date: datetime, to_date: datetime):
        """
        Build Kibana URL for given query, space, dataview and time window

        :param query: Query string
        :param space_id: Kibana space id
        :param dataview_id: Kibana dataview UUID (must be in the same space as space_name)
        :param from_date: From date (datetime object)
        :param to_date: To date (datetime object)
        :return: Kibana URL
        """

        from_str = from_date.strftime(self._time_format)
        to_str = to_date.strftime(self._time_format)

        query_str = urllib.parse.quote_plus(query)

        return (f"{self._base_kibana_url}/s/{space_id}/app/discover#/?_g=(time:(from:'{from_str}',to:'{to_str}'))"
                f"&_a=(index:{dataview_id},query:(language:kuery,query:'{query_str}'))")


if __name__ == '__main__':
    a = KibanaObjectCache('VFpIaE1JZ0JlVGZhS1NZYnF2QUU6ZDNORVJRSXVSUUNpelFPSkM3ZU9RZw==')
    b = KibanaLinkGenerator(a)
    print(b.generate('dm_8_countries_tg', '#BEZNAHUBKU', datetime.strptime('2023-10-25T14:56:37', '%Y-%m-%dT%H:%M:%S')))