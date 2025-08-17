package org.apache.rocketmq.common.sync;

public class MetadataChangeInfo {

    private String metadataKey;

    private String metadataValue;

    private long timestamp;

    private ChangeType changeType;


    public enum ChangeType {
        CREATED,
        UPDATED,
        DELETED
    }

    //这里元数据的键是配置类中哈希表的键
    public static MetadataChangeInfo created(String metadataKey, String metadataValue) {
        if (metadataKey == null || metadataValue == null) {
            throw new IllegalArgumentException("Metadata key and value cannot be null");
        }
        MetadataChangeInfo metadataChangeInfo = new MetadataChangeInfo();
        metadataChangeInfo.setMetadataKey(metadataKey);
        metadataChangeInfo.setMetadataValue(metadataValue);
        metadataChangeInfo.setChangeType(ChangeType.CREATED);
        metadataChangeInfo.setTimestamp(System.currentTimeMillis());
        return metadataChangeInfo;
    }


    public static MetadataChangeInfo updated(String metadataKey, String metadataValue) {
        if (metadataKey == null || metadataValue == null) {
            throw new IllegalArgumentException("Metadata key and value cannot be null");
        }
        MetadataChangeInfo metadataChangeInfo = new MetadataChangeInfo();
        metadataChangeInfo.setMetadataKey(metadataKey);
        metadataChangeInfo.setMetadataValue(metadataValue);
        metadataChangeInfo.setChangeType(ChangeType.UPDATED);
        metadataChangeInfo.setTimestamp(System.currentTimeMillis());
        return metadataChangeInfo;
    }

    public static MetadataChangeInfo deleted(String metadataKey) {
        if (metadataKey == null) {
            throw new IllegalArgumentException("Metadata key cannot be null");
        }
        MetadataChangeInfo metadataChangeInfo = new MetadataChangeInfo();
        metadataChangeInfo.setMetadataKey(metadataKey);
        metadataChangeInfo.setChangeType(ChangeType.DELETED);
        metadataChangeInfo.setTimestamp(System.currentTimeMillis());
        return metadataChangeInfo;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    public void setChangeType(ChangeType changeType) {
        this.changeType = changeType;
    }

    public String getMetadataKey() {
        return metadataKey;
    }

    public void setMetadataKey(String metadataKey) {
        this.metadataKey = metadataKey;
    }

    public String getMetadataValue() {
        return metadataValue;
    }

    public void setMetadataValue(String metadataValue) {
        this.metadataValue = metadataValue;
    }

}
