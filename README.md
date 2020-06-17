# tools to make ElasticSearch node management more intuitive.

## What
This script will tell you the status of a node you are retiring.

## Why
I wrote this tool to gracefully rotate elastic search instances/nodes. Before, I used to watch kopf and cerebro, if there are any indexes or shards in-flight, if there aren't, I would simply terminate the node/instance.
Then I would let ES sort itself out and rebalance when a new node joins the cluster.

This was not great.
Because, if I lost another node, I could experience dataloss. 
During this chaotic rebalance, the cluster would be in a degraded state, and the performance would be terrible.

Instead, I recommend you use this tool along with `exclude.ip` to gracefully retire a node.

You will know when it's safe to kill a node.
You will not lose data.
Your performance should be much better than chaotic node termination.
Your cluster will remain healthy. 

## How


You probably start with making a node excluded from shard allocation:


```
curl -XPUT http://elasticsearch:9200/_cluster/settings -H 'Content-Type: application/json' -d '{  "transient" :{
      "cluster.routing.allocation.exclude._ip" : "10.10.10.10"
   }
}';echo
````

You would then run this script like this:
```
virtualenv --system-site-packages -p python3 venv
. ./venv/bin/activate
pip install -r requirements.txt
```

Then :
```
python elasticsearch_health_stat.py --host elasticsearch
```

You will see lovely moving output of how many documents are left on the node to be retired, and how many things are "in flight".
```
http://elasticsearch:9200/_nodes/10.10.10.10/stats/indices
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



