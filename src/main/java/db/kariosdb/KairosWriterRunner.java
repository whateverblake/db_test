package db.kariosdb;

import db.entity.DataPoint;
import db.influx.InfluxDataWriter;
import db.runner.BaseRunner;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class KairosWriterRunner extends BaseRunner {

    KariosDBWriter kariosDBWriter;
    public KairosWriterRunner(long startTime, int total, int batch, List<String> tags, AtomicInteger counter) {
        super(startTime, total, batch, tags,counter);
        kariosDBWriter = new KariosDBWriter();
    }



    public void doSaveData(List<DataPoint> dataPointList){
        kariosDBWriter.saveDataPoint(dataPointList);
    }



}
