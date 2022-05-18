package db.timescale;

import db.util.SQLUtil;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

public class TimeScaleDBReader {

//    private static String jdbcUrl = "jdbc:postgresql://127.0.0.1:5432/db_test?user=postgres&password=123456";
//
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
//            SQLUtil.closeConnection(conn);
//        }
//    }
//
//
//    private static void doSearch(){
//        long start = 111;
//        long end = System.currentTimeMillis();
//        String agg = "avg";
//        String tableName = "sensor_data";
//        Map<String,String> tags = new HashMap<>();
//        tags.put("tag1","'tag1'");
//        String sql = generateSql(start,end,agg,tableName,tags);
//        sql = String.format(sql);
//        System.out.println(sql);
//        try {
//            double res = SQLUtil.sqlExecuteWithResult(SQLUtil.getConn(jdbcUrl),sql);
//            System.out.println(res);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static String generateSql(long startTime, long endTime, String aggregation, String tableName, Map<String,String> tags){
//
//        StringBuilder tagSb = new StringBuilder() ;
//        if(tags != null && tags.size()>0){
//            for(Map.Entry<String,String> entry : tags.entrySet() ){
//                tagSb.append(" and ");
//                String tag = entry.getKey();
//                String value = entry.getValue();
//                tagSb.append(tag).append("=").append(value);
//            }
//        }
//        StringBuilder sqlSB  = new StringBuilder();
//        sqlSB.append("select ").append(aggregation).append("(").append("dt").append(")").append(" from ").append(tableName)
//                .append(" where time >=").append(startTime).append(" and time<= ").append(endTime).append(tagSb.toString());
//
//        return sqlSB.toString() ;
//
//    }

}
