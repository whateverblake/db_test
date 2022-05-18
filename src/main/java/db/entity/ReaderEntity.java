package db.entity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReaderEntity {

    private long startTime;
    private long endTime;
    private String sensorId;
    private String aggregation;
    private String tagsStr;
    private List<String> tags;

    public ReaderEntity(long startTime, long endTime, String aggregation, String tagsStr, List<String> tags) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.sensorId = "sensor1";
        this.aggregation = aggregation;
        this.tagsStr = tagsStr;
        this.tags = tags;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public Map<String,String> getTags() {
        Map<String,String> tagsMap = new HashMap<>();
        if(tags != null){
            for(String tag: tags){
                tagsMap.put(tag,tag);
            }
        }
        return tagsMap;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public String getAggregation() {
        return aggregation;
    }

    public void setAggregation(String aggregation) {
        this.aggregation = aggregation;
    }

    public String getTagsStr() {
        return tagsStr;
    }

    public void setTagsStr(String tagsStr) {
        this.tagsStr = tagsStr;
    }

    @Override
    public String toString() {
        return "ReaderEntity{" +
                "startTime=" + startTime +
                ", endTime=" + endTime +
                ", sensorId='" + sensorId + '\'' +
                ", aggregation='" + aggregation + '\'' +
                ", tagsStr='" + tagsStr + '\'' +
                '}';
    }
}
