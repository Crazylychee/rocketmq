package org.apache.rocketmq.broker.sync;

import com.alibaba.fastjson2.JSON;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sync.MetadataChangeInfo;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

public class SyncMessageProducer {

    private final BrokerController brokerController;
    private final int syncQueueId;

    public SyncMessageProducer(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.syncQueueId = 0;
    }

    public boolean sendMetadataChange(final String targetTopic, MetadataChangeInfo changeInfo) {
        try {
            byte[] body = JSON.toJSONBytes(changeInfo);

            MessageExtBrokerInner msg = new MessageExtBrokerInner();
            msg.setBrokerName(brokerController.getBrokerConfig().getBrokerName());
            msg.setTopic(targetTopic);
            msg.setBody(body);
            msg.setKeys(changeInfo.getMetadataKey());
            msg.setTags(changeInfo.getMetadataKey());
            msg.setBornTimestamp(System.currentTimeMillis());
            msg.setStoreHost(brokerController.getStoreHost());
            msg.setBornHost(brokerController.getStoreHost());
            msg.setQueueId(this.syncQueueId);
            msg.setMsgId(MessageClientIDSetter.createUniqID());

            PutMessageResult result = this.brokerController.getMessageStore().putMessage(msg);
            System.out.println("发送了消息");
            System.out.println(msg);

            if (result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                return true;
            } else {
                System.out.println("Failed to put message: " + result.getPutMessageStatus());
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
