# kafkatools

Kafka's console tool to manage topics, query offsets, etc.

 - Validated on kafka version 2.11

## Usage command
```
Usage: java -jar kafkatools.jar [options]
  Options:
  * --broker
      broker url, for example --broker=192.168.19.61:9092
    --topic-list
      list all topics
    --topic-list-offset
      list all topics with offset
    --topic-create
      create a topic, for example --topic-create=topicName -partitions=3 
      -replication=2 
    --topic-delete
      delete a topic, for example --topic-delete=topicName
    --group
      list consumer group information, for example --group=group1 
      [-topic=topicName] 
    --help
```
