package db.taos;

import db.Main;
import db.entity.ReaderEntity;
import db.reader.BaseReader;
import db.util.SQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;

public class TDengineReader  extends BaseReader {

    private static Logger logger = LoggerFactory.getLogger(TDengineReader.class);

    @Override
    public double doQuery(ReaderEntity readerEntity) {
        String querySQL = SQLUtil.sqlGenerateForTDB(readerEntity,
                "sensor_data");
        logger.info("sql = {}",querySQL);

        return SQLUtil.sqlExecuteWithResult(querySQL);
    }

//    public static void main(String[] args) {
//        Connection conn = null;
//        try {
//            conn = SQLUtil.getConn(jdbcUrl);
//            long start = System.currentTimeMillis();
//            doSearch();
//            long end = System.currentTimeMillis();
//            System.out.println("time consume: " + (end - start));
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//           SQLUtil.closeConnection(conn);
//        }
//    }

//
//    private static void doSearch(){
//        long start = 111;
//        long end = System.currentTimeMillis();
//        String agg = "count";
//        String tableName = "t_test";
//        Map<String,String> tags = new HashMap<>();
//        tags.put("sensor","'sensor3'");
//        String sql = generateSql(start,end,agg,tableName,tags);
//        sql = String.format(sql);
//
//        System.out.println(sql);
//        try {
//            double res = SQLUtil.sqlExecuteWithResult(SQLUtil.getConn(jdbcUrl),sql);
//            System.out.println(res);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }












}
