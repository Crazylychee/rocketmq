package org.apache.rocketmq.broker.sync;

import org.apache.rocketmq.common.sync.MetadataChangeObserver;

public class NoopMetadataChangeObserver implements MetadataChangeObserver {


    @Override
    public void onCreated(String targetTopic, String metadataKey, Object newMetadata) {

    }

    @Override
    public void onUpdated(String targetTopic, String metadataKey, Object newMetadata) {

    }

    @Override
    public void onDeleted(String targetTopic, String metadataKey, Object oldMetadata) {

    }
}
