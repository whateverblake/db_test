package db.sqlite;

import db.util.ConnectionUtil;

import java.util.HashMap;
import java.util.Map;

public class SqliteReader {

    private static String path = "/Users/blake/intellij_workspace/db_test";
    private static String url = "jdbc:sqlite:" + path + "/test.db";

    public static void main(String[] args) {

        long start = System.currentTimeMillis();
        doSearch();
        long end = System.currentTimeMillis();

        System.out.println("time consume: "+(end-start));
    }


    private static void doSearch(){
        long start = 111;
        long end = System.currentTimeMillis();
        String agg = "avg";
        String tableName = "sensor_data";
        Map<String,String> tags = new HashMap<>();
        tags.put("tag1","'tag1'");
        String sensorId = "sensor1";
        String sql = generateSql(sensorId,start,end,agg,tableName,tags);
        sql = String.format(sql);
        System.out.println(sql);
        try {
            double res = ConnectionUtil.sqlExecuteWithResult(ConnectionUtil.getConn(url),sql);
            System.out.println(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static String generateSql(String sensorId,long startTime, long endTime, String aggregation, String tableName, Map<String,String> tags){

        StringBuilder tagSb = new StringBuilder() ;
        if(tags != null && tags.size()>0){
            for(Map.Entry<String,String> entry : tags.entrySet() ){
                tagSb.append(" and ");
                String tag = entry.getKey();
                String value = entry.getValue();
                tagSb.append(tag).append("=").append(value);
            }
        }
        StringBuilder sqlSB  = new StringBuilder();
        sqlSB.append("select ").append(aggregation).append("(").append("value").append(")").append(" from ").append(tableName)
                .append(" where time >=").append(startTime).append(" and time<= ").append(endTime)
                .append(" and sensor_id='").append(sensorId).append("' ")
                .append(tagSb.toString());

        return sqlSB.toString() ;

    }



}
