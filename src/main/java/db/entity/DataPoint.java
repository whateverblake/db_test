package db.entity;

public class DataPoint {

    private String sensorId;
    private long time;
    private double value;
    private String tag;

    public DataPoint() {

    }

    public DataPoint(String sensorId, long time, double value, String tag) {
        this.sensorId = sensorId;
        this.time = time;
        this.value = value;
        this.tag = tag;
    }

    public DataPoint(long time, double value, String tag) {
        this.time = time;
        this.value = value;
        this.tag = tag;
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

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }
}
