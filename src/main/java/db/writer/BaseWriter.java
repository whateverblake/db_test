package db.writer;

import db.entity.DataPoint;

import java.util.List;

public abstract class BaseWriter {
    public static Object lock = new Object();
    public static boolean init = false;

    public void doPreJob() {
        synchronized (lock) {
            if (!init) {
                dropTable();
                createTable();
                init = true;
            }
        }
    }

    public  void dropTable(){}
    public  void createTable(){};

    public abstract void saveDataPoint(List<DataPoint> dataPointList);

}
