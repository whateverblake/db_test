package db;

import db.constant.Constants;
import db.entity.ReaderEntity;
import db.influx.InfluxWriterRunner;
import db.kariosdb.KairosWriterRunner;
import db.runner.BaseRunner;
import db.sqlite.SqliteWriterRunner;
import db.taos.TaosWriterRunner;
import db.timescale.TimeScaleWriterRunner;
import db.writer.BaseWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    private static Logger logger = LoggerFactory.getLogger(Main.class);
    static int totalSize = 0;
    static int batchSize = 0;
    static int threadNum = 0;
    static String dbName = "";
    static String operation = "";
    static long startTime;
    static long readStartTime;
    static long readEndTime;
    static List<String> tags;
    static List<String> readTags;
    static String readTagsStr;
    static String readAggregation;



    static {
        try {

            operation = System.getProperty("operation");
            if(operation.equals("write")){
                totalSize = Integer.parseInt(System.getProperty("totalSize"));
                batchSize = Integer.parseInt(System.getProperty("batchSize"));
                threadNum = Integer.parseInt(System.getProperty("threadNum"));
                dbName = System.getProperty("dbName");
                String tagsStr = System.getProperty("tags");
                tags = parseTags(tagsStr);
                startTime = Long.parseLong(System.getProperty("startTime"));
                logger.info("totalSize = {}", totalSize);
                logger.info("batchSize = {}", batchSize);
                logger.info("threadNum = {}", threadNum);
                logger.info("dbName = {}", dbName);
            }else{
                readStartTime = Long.parseLong(System.getProperty("readStartTime"));
                readEndTime = Long.parseLong(System.getProperty("readEndTime"));
                readTagsStr = System.getProperty("readTags");
                readAggregation = System.getProperty("readAggregation");
                readTags = parseTags(readTagsStr);
            }

        } catch (Exception e) {
            logger.error("please set all required variables");
            System.exit(0);
        }

    }


    public static  List<String> parseTags(String tagsStr){
        if (tagsStr != null) {
            String[] tagarr = tagsStr.split(",");
           List<String> tags = new ArrayList<>(tagarr.length);
            for (String tag : tagarr) {
                tags.add(tag);
            }
            return tags;
        }
        return null ;
    }

    private static ExecutorService service;

    private static AtomicInteger counter;



    public static void main(String[] args) {

        if(operation.equals("write")){
            doWrite();
        }else{
            doRead();
        }


    }

    public static void doRead(){

        ReaderEntity readerEntity = new ReaderEntity(readStartTime,readEndTime,readAggregation,readTagsStr,readTags);



    }

    public static void doWrite(){
        service = Executors.newFixedThreadPool(threadNum);

        int subTotal = (int) Math.round(totalSize / threadNum);

        counter = new AtomicInteger(threadNum);

        List<BaseRunner> baseRunners = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            startTime += i * subTotal;
            BaseRunner baseRunner = null;

            if (dbName.equals(Constants.TAOS)) {
                baseRunner = new TaosWriterRunner(startTime, subTotal, batchSize, tags, counter);

            } else if (dbName.equals(Constants.INFLUXDB)) {
                baseRunner = new InfluxWriterRunner(startTime, subTotal, batchSize, tags, counter);

            } else if (dbName.equals(Constants.KAIROSDB)) {
                baseRunner = new KairosWriterRunner(startTime, subTotal, batchSize, tags, counter);


            } else if (dbName.equals(Constants.TIMESCALE)) {
                baseRunner = new TimeScaleWriterRunner(startTime, subTotal, batchSize, tags, counter);
            } else if (dbName.equals(Constants.SQLITE)) {
                baseRunner = new SqliteWriterRunner(startTime, subTotal, batchSize, tags, counter);
            } else {
                logger.info("..................specific db type does not exist....................");
                System.exit(0);
            }

            baseRunners.add(baseRunner);

        }

        long consumeStartTime = System.currentTimeMillis();

        for(BaseRunner baseRunner: baseRunners){
            service.submit(baseRunner);
        }


        while (counter.get() != 0) {
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        long consumeEndTime = System.currentTimeMillis();

        logger.info("************************ dbType = {} , threadNum = {}, batchSize = {},totalSize = {} ,need time = {} ************************ ",

                dbName, threadNum, batchSize, totalSize, (consumeEndTime - consumeStartTime));

        System.exit(0);
    }


}
