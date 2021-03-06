package org.csnowfox.kafkatools;

import kafka.admin.ConsumerGroupCommand;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.csnowfox.kafkatools.entity.GroupPartitionOffset;
import org.csnowfox.kafkatools.entity.TopicPartitionOffset;
import org.csnowfox.kafkatools.utils.DateUtils;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @ClassName: Tools
 * @Description Kafka console tool business
 * @Author Csnowfox
 **/
public class Tools {

    private static String clientID = "__monitor" ;
    private String brokerUrl;

    public Tools(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    /**
     * Get all the topics
     *
     * @return
     */
    public Set<String> getTopicList() throws ExecutionException, InterruptedException {
        Properties pro = new Properties();
        pro.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        org.apache.kafka.clients.admin.AdminClient adminClient = org.apache.kafka.clients.admin.AdminClient.create(pro);
        ListTopicsResult listTopics = adminClient.listTopics();
        Set<String> topics = listTopics.names().get();
        return topics;

    }

    /**
     * Get the groupids
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public List<String> consumerGroupListing () throws ExecutionException, InterruptedException {
        Properties pro = new Properties();
        pro.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        org.apache.kafka.clients.admin.AdminClient adminClient = org.apache.kafka.clients.admin.AdminClient.create(pro);
        ListConsumerGroupsOptions opts = new ListConsumerGroupsOptions();
        ListConsumerGroupsResult listConsumerGroupsResult =  adminClient.listConsumerGroups();
        Collection<ConsumerGroupListing> rs = listConsumerGroupsResult.all().get();
        List<String> result = new LinkedList<>();
        for (ConsumerGroupListing c : rs) {
            result.add(c.groupId());
        }
        return result;
    }

    /**
     * Get the offset of the specified groupid
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public List<GroupPartitionOffset> consumerGroupListing (String groupId, String topic) throws ExecutionException, InterruptedException {
        Properties pro = new Properties();
        pro.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        org.apache.kafka.clients.admin.AdminClient adminClient = org.apache.kafka.clients.admin.AdminClient.create(pro);
        ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult =  adminClient.listConsumerGroupOffsets(groupId);
        Map<TopicPartition, OffsetAndMetadata> offsets = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
        List<GroupPartitionOffset> poffset = new LinkedList<>();
        for (TopicPartition p : offsets.keySet()) {
            if (topic != null && !topic.trim().equals("")) {
                if (topic.equals(p.topic())) {
                    poffset.add(new GroupPartitionOffset(p.topic(), p.partition(), offsets.get(p).offset(), getLogEndOffset(p)));
                }
            } else {
                poffset.add(new GroupPartitionOffset(p.topic(), p.partition(), offsets.get(p).offset(), getLogEndOffset(p)));
            }
        }
        poffset.sort((o1, o2) -> o1.compareTo(o2));
        return poffset;
    }

    /**
     * Get the offset of the specified topic
     * @return
     */
    public List<TopicPartitionOffset> getTopicOffset (String topic) {
        KafkaConsumer<String, String> consumer= getNewConsumer();
        List<PartitionInfo> partitionInfos = getNewConsumer().partitionsFor(topic);
        List<TopicPartition> topicPartitions = new LinkedList<>();
        for (PartitionInfo p : partitionInfos) {
            topicPartitions.add(new TopicPartition(topic, p.partition()));
        }
        consumer.assign(topicPartitions);
        consumer.seekToEnd(topicPartitions);

        List<TopicPartitionOffset> list = new LinkedList<>();
        for (PartitionInfo p : partitionInfos) {
            list.add(new TopicPartitionOffset(p.partition(), consumer.position(new TopicPartition(topic, p.partition()))));
        }
        return list;
    }

    /**
     * Create topic
     * @param topic
     * @param numPartitions
     * @param replicationFactor
     */
    public void createTopic (String topic, int numPartitions, short replicationFactor) {
        Properties pro = new Properties();
        pro.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        org.apache.kafka.clients.admin.AdminClient adminClient = org.apache.kafka.clients.admin.AdminClient.create(pro);

        NewTopic newTopic = new NewTopic(topic,numPartitions, (short) replicationFactor);
        Collection<NewTopic> newTopicList = new ArrayList<>();
        newTopicList.add(newTopic);
        CreateTopicsResult result =  adminClient.createTopics(newTopicList);
        try {
            result.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Delete topic
     * @param topic
     */
    public void deleteTopic (String topic) {
        Properties pro = new Properties();
        pro.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        org.apache.kafka.clients.admin.AdminClient adminClient = org.apache.kafka.clients.admin.AdminClient.create(pro);
        Collection<String> deleteTopicList = new ArrayList<>();
        deleteTopicList.add(topic);
        DeleteTopicsResult result =  adminClient.deleteTopics(deleteTopicList);
        try {
            result.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                System.out.println("The deleted topic does not exist");
            } else {
                e.printStackTrace();
            }
        }
    }

    /**
     * Set offset according to time
     * @param groupid
     * @param topic
     * @param datetime
     * @throws ParseException
     */
    public void resetOffsetByDatetime(String groupid, String topic, String datetime) throws ParseException {
        String[] arg = new String[10];
        arg[0] = "--bootstrap-server";
        arg[1] = brokerUrl;
        arg[2] = "--group";
        arg[3] = groupid;
        arg[4] = "--topic";
        arg[5] = topic;
        arg[6] = "--reset-offsets";
        arg[7] = "--to-datetime";
        arg[8] = DateUtils.getISO8601Timestamp(DateUtils.convertToGMT(DateUtils.getDateClass(datetime)));
        arg[9] = "--execute";

        ConsumerGroupCommand.ConsumerGroupCommandOptions checkArgs = new ConsumerGroupCommand.ConsumerGroupCommandOptions(
                arg);
        checkArgs.checkArgs();
        scala.collection.immutable.Map<TopicPartition, OffsetAndMetadata> resultMap = new ConsumerGroupCommand.ConsumerGroupService(checkArgs).resetOffsets();
    }

    private KafkaConsumer getNewConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerUrl);
        props.put("group.id", clientID);
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    private long getLogEndOffset(TopicPartition topicPartition){
        KafkaConsumer<String, String> consumer= getNewConsumer();
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seekToEnd(Arrays.asList(topicPartition));
        long endOffset = consumer.position(topicPartition);
        return endOffset;
    }

}
