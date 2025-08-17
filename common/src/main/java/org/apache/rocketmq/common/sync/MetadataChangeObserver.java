package org.apache.rocketmq.common.sync;

public interface MetadataChangeObserver {

    void onCreated(String targetTopic,String metadataKey, Object newMetadata);

    void onUpdated(String targetTopic,String metadataKey, Object newMetadata);

    void onDeleted(String targetTopic,String metadataKey, Object oldMetadata);
}
