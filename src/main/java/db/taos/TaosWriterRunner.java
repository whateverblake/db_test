package db.taos;

import db.entity.DataPoint;
import db.influx.InfluxDataWriter;
import db.runner.BaseRunner;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TaosWriterRunner extends BaseRunner {

    TDengineWriter tDengineWriter;
    public TaosWriterRunner(long startTime, int total, int batch, List<String> tags, AtomicInteger counter) {
        super(startTime, total, batch, tags,counter);
        tDengineWriter = new TDengineWriter(tags);
    }



    public void doSaveData(List<DataPoint> dataPointList){
        tDengineWriter.saveDataPoint(dataPointList);
    }

}
