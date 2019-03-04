# tools to make ElasticSearch node management more intuitive.

This script will tell you the status of a node you are retiring.
You probably start with making a node excluded from shard allocation:

```
curl -XPUT http://elasticsearch:9200/_cluster/settings -H 'Content-Type: application/json' -d '{  "transient" :{
      "cluster.routing.allocation.exclude._ip" : "10.10.10.10"
   }
}';echo
````

You would then run this script like this:
```
pip install -r requirements.py
```

Then :
```
python elasticsearch_health_stat.py --host elasticsearch
```

You will see lovely moving output of how many documents are left on the node to be retired, and how many things are "in flight".
```
http://elasticsearch:9200/_nodes/10.128.234.112/stats/indices
retiring node: 10.10.10.10 relocating: 5 initializing: 0 unassigned: 0 retiring_node_count: 11
```

After `retiring_node_count` reaches 0, you should be safe to terminate that elasticsearch node, because it will not make the cluster red or amber.

After you terminate the node, you want to remove the exclusion.

```
curl -XPUT http://elasticsearch:9200/_cluster/settings -H 'Content-Type: application/json' -d '{  "transient" :{
      "cluster.routing.allocation.exclude._ip" : ""
   }
}';echo
```



