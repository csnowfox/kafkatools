# kafkatools

Kafka's console tool to manage topics, query offsets, etc.
You need jre1.8 or above.

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
    --group-list
      list consumer group information, for example --group-list
    --group
      list consumer group information, for example --group=group1 
      [-topic=topicName [-reset-offset-datetime=yyyyMMddHHmmss]] 
    --help
```
