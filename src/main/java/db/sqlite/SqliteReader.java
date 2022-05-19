package db.sqlite;

import db.Main;
import db.entity.ReaderEntity;
import db.reader.BaseReader;
import db.util.SQLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SqliteReader extends BaseReader {

    private static Logger logger = LoggerFactory.getLogger(SqliteReader.class);

    @Override
    public double doQuery(ReaderEntity readerEntity) {

        String querySQL = SQLUtil.sqlGenerateForTDB(readerEntity, "sensor_data");

        logger.info("======= sql = {}",querySQL);
        return SQLUtil.sqlExecuteWithResult(querySQL);
    }

//
//    public static String generateSql(long startTime, long endTime, String aggregation, String tableName, Map<String, String> tags) {
//
//        StringBuilder tagSb = new StringBuilder();
//        if (tags != null && tags.size() > 0) {
//            for (Map.Entry<String, String> entry : tags.entrySet()) {
//                tagSb.append(" and ");
//                String tag = entry.getKey();
//                String value = entry.getValue();
//                tagSb.append(tag).append("=").append(value);
//            }
//        }
//        StringBuilder sqlSB = new StringBuilder();
//        sqlSB.append("select ").append(aggregation).append("(").append("value").append(")").append(" from ").append(tableName)
//                .append(" where time >=").append(startTime).append(" and time<= ").append(endTime)
//                .append(tagSb.toString());
//
//        return sqlSB.toString();
//    }

//    private static String path = "/Users/blake/intellij_workspace/db_test";
//    private static String url = "jdbc:sqlite:" + path + "/test.db";

//    public static void main(String[] args) {
//
//        long start = System.currentTimeMillis();
//        doSearch();
//        long end = System.currentTimeMillis();
//
//        System.out.println("time consume: "+(end-start));
//    }


//    private static void doSearch(){
//        long start = 111;
//        long end = System.currentTimeMillis();
//        String agg = "avg";
//        String tableName = "sensor_data";
//        Map<String,String> tags = new HashMap<>();
//        tags.put("tag1","'tag1'");
//        String sensorId = "sensor1";
//        String sql = generateSql(sensorId,start,end,agg,tableName,tags);
//        sql = String.format(sql);
//        System.out.println(sql);
//        try {
//            double res = SQLUtil.sqlExecuteWithResult(SQLUtil.getConn(url),sql);
//            System.out.println(res);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }


}
