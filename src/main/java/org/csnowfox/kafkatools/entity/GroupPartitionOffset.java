package org.csnowfox.kafkatools.entity;

public class GroupPartitionOffset implements Comparable {

    private String topic;
    private int partition;
    private long offset;
    private long endOffset;

    public GroupPartitionOffset(String topic, int partition, long offset, long endOffset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.endOffset = endOffset;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    @Override
    public int compareTo(Object o) {
        return this.topic.compareTo(((GroupPartitionOffset)o).topic);
    }

}
