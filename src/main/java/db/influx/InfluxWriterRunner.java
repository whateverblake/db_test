package db.influx;

import db.entity.DataPoint;
import db.runner.BaseRunner;
import db.sqlite.SqliteWriter;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class InfluxWriterRunner extends BaseRunner {

    InfluxDataWriter influxDataWriter;
    public InfluxWriterRunner(long startTime, int total, int batch, List<String> tags, AtomicInteger counter) {
        super(startTime, total, batch, tags,counter);
        influxDataWriter = new InfluxDataWriter();
    }



    public void doSaveData(List<DataPoint> dataPointList){
        influxDataWriter.saveDataPoint(dataPointList);
    }

}
