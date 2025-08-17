package org.apache.rocketmq.broker.sync;

import com.alibaba.fastjson2.JSON;
import org.apache.rocketmq.common.sync.MetadataChangeInfo;
import org.apache.rocketmq.common.sync.MetadataChangeObserver;

public class SyncMetadataChangeObserver implements MetadataChangeObserver {


    private final SyncMessageProducer producer;

    public SyncMetadataChangeObserver(SyncMessageProducer producer) {
        this.producer = producer;
    }

    //第一个参数是目标主题
    @Override
    public void onCreated(String targetTopic,String metadataKey, Object newMetadata) {
        this.producer.sendMetadataChange(targetTopic, MetadataChangeInfo.created(
            metadataKey,
            JSON.toJSONString(newMetadata)
        ));
    }

    @Override
    public void onUpdated(String targetTopic, String metadataKey, Object newMetadata) {
        MetadataChangeInfo changeInfo = MetadataChangeInfo.updated(
                metadataKey,
            JSON.toJSONString(newMetadata)
        );
        this.producer.sendMetadataChange(targetTopic, changeInfo);
    }

    @Override
    public void onDeleted(String targetTopic,String metadataKey, Object oldMetadata) {
        this.producer.sendMetadataChange(targetTopic, MetadataChangeInfo.deleted(JSON.toJSONString(oldMetadata)));
    }
}
