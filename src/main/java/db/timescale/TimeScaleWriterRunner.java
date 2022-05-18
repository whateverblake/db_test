package db.timescale;

import db.entity.DataPoint;
import db.runner.BaseRunner;
import db.taos.TDengineWriter;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeScaleWriterRunner extends BaseRunner {

    TimeScaleDBWriter timeScaleDBWriter;
    public TimeScaleWriterRunner(long startTime, int total, int batch, List<String> tags, AtomicInteger counter) {
        super(startTime, total, batch, tags,counter);
        timeScaleDBWriter = new TimeScaleDBWriter(tags);
    }



    public void doSaveData(List<DataPoint> dataPointList){
        timeScaleDBWriter.saveDataPoint(dataPointList);
    }

}
