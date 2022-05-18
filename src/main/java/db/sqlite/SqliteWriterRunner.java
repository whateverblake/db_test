package db.sqlite;

import db.entity.DataPoint;
import db.runner.BaseRunner;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SqliteWriterRunner extends BaseRunner {

    SqliteWriter sqliteWriter;
    public SqliteWriterRunner(long startTime, int total, int batch, List<String> tags, AtomicInteger counter) {
        super(startTime, total, batch, tags,counter);
        sqliteWriter = new SqliteWriter(tags);
        sqliteWriter.doPreJob();
    }



    public void doSaveData(List<DataPoint> dataPointList){
        sqliteWriter.saveDataPoint(dataPointList);
    }

}
