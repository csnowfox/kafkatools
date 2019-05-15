package org.csnowfox.kafkatools.entity;

public class TopicPartitionOffset {

    private int partition;
    private long endOffset;

    public TopicPartitionOffset(int partition, long endOffset) {
        this.partition = partition;
        this.endOffset = endOffset;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }
}
