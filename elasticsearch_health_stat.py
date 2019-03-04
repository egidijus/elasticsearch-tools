#!/bin/env python
"""
When retiring a node from ES gracefully, you should exclude that node from shard/index allocation.
you should do this via the api:

```
curl -XPUT http://elk-elasticsearch.service.prod.consul:9200/_cluster/settings -H 'Content-Type: application/json' -d '{  "transient" :{
      "cluster.routing.allocation.exclude._ip" : "10.10.10.10"
   }
}';echo
```

During the de-allocation of shards, ES will move indexes and shards about to ensure it complies with your outlined polices of shards + replicas.
This scrip will let you watch the process, so you know when the node is ready(SAFE) to be decommissioned/terminated.

After you terminate the NODE, you should remove the `exclude_ip` filter, so that if a new node comes up, with a matching IP, the new node will allocate shards and indexes.

curl -XPUT http://elk-elasticsearch.service.prod.consul:9200/_cluster/settings -H 'Content-Type: application/json' -d '{  "transient" :{
      "cluster.routing.allocation.exclude._ip" : ""
   }
}';echo

when the output changes from:
retiring node: 10.10.10.10 relocating: 35 initializing: 0 unassigned: 0 retiring_node_count: 519406211

to
retiring node: 10.10.10.10 relocating: 0 initializing: 0 unassigned: 0 retiring_node_count: 0

you can kill/terminate the `10.10.10.10` retiring node

"""
import argparse
import requests
import json
import sys
import time
from datetime import datetime

HEALTH = "/_cluster/health"
SHARDS = "/_cat/shards?format=json"
STATS = "/_cluster/stats?human&pretty"
SETTINGS = "/_cluster/settings"


class ES(object):
    def __init__(self, es_host='127.0.0.1', es_port=9200):
        self.host = es_host
        self.port = es_port

    def get_api_host(self, host, port):
        try:
            API_HOST = "http://" + self.host + ":" + self.port
            return API_HOST
        except Exception as e:
            print(
                "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                type(e).__name__,
                e,
            )

    def prettyfy_json(json_object):
        try:
            return json.dumps(json_object, indent=4, sort_keys=True)
        except Exception as e:
            print(
                "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                type(e).__name__,
                e,
            )

    def client(self, PATH, API_HOST):
        """
        accepts a path for API and an API_HOST
        returns a json document from elasticsearch.
        """
        API_HOST = self.get_api_host(self.host, self.port)
        try:
            URL = API_HOST + PATH
            get_state = requests.get(
                URL,
                verify=False,
            )
            state = get_state.json()
            # print(state)
            return state
        except Exception as e:
            print(
                "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                type(e).__name__,
                e,
            )

    def get_relocating_shards(self):
        API_HOST = self.get_api_host(self.host, self.port)
        try:
            relocating_shards = self.client(SHARDS, API_HOST)
            return relocating_shards['relocating_shards']
        except Exception as e:
            print(
                "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                type(e).__name__,
                e,
            )

    def relocating_index(self):
        API_HOST = self.get_api_host(self.host, self.port)
        shards_json = self.client(SHARDS, self.get_api_host())
        try:
            shard_index = []
            for shard in shards_json:
                if shard['state'] == "RELOCATING":
                    shard_index.append(shard['index'])
                    return shard_index
                else:
                    pass
        except Exception as e:
            print(
                "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                type(e).__name__,
                e,
            )

    def get_health(self, key_name):
        API_HOST = self.get_api_host(self.host, self.port)
        try:
            health_value = self.client(HEALTH, API_HOST)
            return health_value[key_name]
        except Exception as e:
            print(
                "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                type(e).__name__,
                e,
            )

    def get_excluded_ip(self):
        API_HOST = self.get_api_host(self.host, self.port)
        try:
            excluded_ip = self.client(SETTINGS, API_HOST)
            exclude_ip = excluded_ip['transient']['cluster']['routing'][
                'allocation']['exclude']['_ip']
            return exclude_ip
        except Exception as e:
            print(
                "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                type(e).__name__,
                e,
            )

    def get_arb_value(self, dictionary):
        """
        because "/_nodes/" + RETIRING_NODE + "/stats/indices?pretty‌"
        or get_stats(RET_NODE_STAT)['nodes']
        or "/_nodes/" + RETIRING_NODE + "/stats/indices?pretty‌"['nodes']
        will return a dictionary with mystery key for the node ID, we want to
        get to the values, we simply iterate.
        """
        try:
            return next(iter(dictionary.values()))
        except StopIteration:
            print("NOTHING here captain")
            return []

    def get_doc_count(self):
        API_HOST = self.get_api_host(self.host, self.port)
        """
        this should return just a count of documents from the key:
        ['indices']['docs']['count']

        """
        PATH = "/_nodes/" + self.get_excluded_ip() + "/stats/indices"
        print(API_HOST + PATH)
        try:
            doc_count = self.client(PATH, API_HOST)['nodes']
            doc_count = self.get_arb_value(
                doc_count)['indices']['docs']['count']
            if not doc_count:
                return 0
            else:
                return doc_count
        except Exception as e:
            print(
                "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                type(e).__name__,
                e,
            )

    def health_average(self, start_time):
        """
        build a list of important ES stats while decommissioning a node.
        then we will check each value if it is 0, because 0 means all unsafe
        operations have completed.
        then we loop, with a two second wait.
        example output:
        retiring node: 10.10.10.10 relocating: 28 initializing: 0 unassigned: 0 retiring_node_count: 878941271
        retiring node: 10.10.10.10 relocating: 28 initializing: 0 unassigned: 0 retiring_node_count: 878941468
        once all reaches 0, you are ready to kill that node.
        """
        try:
            loop_start = time.time()
            health = [
                self.get_health("relocating_shards"),
                self.get_health("initializing_shards"),
                self.get_health("unassigned_shards"),
                self.get_doc_count()
            ]
            for stat in health:
                while stat in health != 0:
                    time.sleep(2.0 - ((time.time() - loop_start) % 2.0))
                    relocating = self.get_health("relocating_shards")
                    initializing = self.get_health("initializing_shards")
                    unassigned = self.get_health("unassigned_shards")
                    doc_count = self.get_doc_count()
                    print("retiring node:", self.get_excluded_ip(),
                          "relocating:", relocating, "initializing:",
                          initializing, "unassigned:", unassigned,
                          "retiring_node_count:", doc_count)
        except Exception as e:
            print(
                "Error on line {}".format(sys.exc_info()[-1].tb_lineno),
                type(e).__name__,
                e,
            )


if __name__ == '__main__':
    start_time = datetime.now()
    parser = argparse.ArgumentParser(description='es-tools params')
    parser.add_argument(
        '--host', dest='host', default='127.0.0.1', help='elasticsearch host')
    parser.add_argument(
        '--port', dest='port', default='9200', help='elasticsearch port')
    args = parser.parse_args()
    es = ES(args.host, args.port)
    es.health_average(start_time)
