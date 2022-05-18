package db.entity;

import java.util.List;

public class DataPoint {

    private String sensorId;
    private long time;
    private double value;
    private List<String> tags;

    public DataPoint() {

    }

    public DataPoint(String sensorId, long time, double value,List<String> tags) {
        this.sensorId = sensorId;
        this.time = time;
        this.value = value;
        this.tags = tags;
    }

    public DataPoint(long time, double value,List<String> tags) {
        this.time = time;
        this.value = value;
        this.tags = tags;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }
}
