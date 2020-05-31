## This file contains a project on Spark Streaming

#### Time: 120 minutes


## Project

```
- In your aws machine, download kafka and set it up
 There are many samples in web, you can take reference from my github project - https://github.com/shekhar2010us/kafka_t/blob/master/labs/01_install_zk_kafka_single_broker.md
 Cautious: The reference use Ubuntu, but we have centos machines
 
- From terminal, create a kafka topic and start sending words
- In Jupyter notebook, write a spark streaming notebook that reads data from Kafka topic
    - Count words and output the frequency of each word in the given micro-batch timeframe
    Reference: https://github.com/cloudxlab/bigdata/tree/master/spark/examples/streaming/word_count_kafka
```
