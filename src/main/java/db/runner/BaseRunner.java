package db.runner;

import db.Main;
import db.entity.DataPoint;
import db.util.GenerateDataPointBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BaseRunner extends RunnerTask {

    private static Logger logger = LoggerFactory.getLogger(BaseRunner.class);
    private long startTime;
    private int total;
    private int batch;
    private int iteration;
    private String sensorId = "sensor1";
    private List<String> tags;
    private AtomicInteger counter;

    public BaseRunner(long startTime, int total, int batch, List<String> tags,AtomicInteger counter) {
        this.startTime = startTime;
        this.total = total;
        this.batch = batch;
        if(total % batch == 0){
            iteration = total/batch;
        }else {
            iteration =  total/batch +1 ;
        }
        this.tags = tags;
        this.counter = counter;
    }

    @Override
    public void run() {

        long subTimeStart = System.currentTimeMillis();
        for (int i = 0; i < iteration; i++) {
            if(i!=0){
                startTime = startTime + batch ;
            }
            List<DataPoint> dataPointList = GenerateDataPointBatch.generateDataPoint(batch, sensorId, startTime, tags);
            long timeStart = System.currentTimeMillis();
            doSaveData(dataPointList);
            long timeEnd = System.currentTimeMillis();
            logger.info("???? {} iteration = {}  batchSize = {} consume time = {} ????", Thread.currentThread().getName(), i,batch, (timeEnd - timeStart));
        }
        long subTimeEnd = System.currentTimeMillis();
        counter.decrementAndGet();
        logger.info("################ {} write {} on batch size {} need time = {} ms #############", Thread.currentThread().getName(), total, batch, (subTimeEnd - subTimeStart));
    }


    public void doSaveData(List<DataPoint> dataPointList) {


    }
}
