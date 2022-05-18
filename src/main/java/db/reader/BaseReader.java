package db.reader;

import db.entity.ReaderEntity;
import db.runner.BaseRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseReader {
    private static Logger logger = LoggerFactory.getLogger(BaseReader.class);


    public void queryData(ReaderEntity readerEntity){
        long timeStart = System.currentTimeMillis();
        double res = doQuery(readerEntity);
        long timeEnd = System.currentTimeMillis();
        logger.info("---------query result = {},, consume time = {} ------ for query entity ",res,(timeEnd - timeStart),readerEntity);
    }


    public abstract double doQuery(ReaderEntity readerEntity);

}
