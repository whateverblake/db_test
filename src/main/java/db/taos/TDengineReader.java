package db.taos;

import com.taosdata.jdbc.TSDBDriver;
import db.util.ConnectionUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class TDengineReader {


    private static String jdbcUrl = "jdbc:TAOS-RS://127.0.0.1:6041/test?user=root&password=taosdata";

    public static void main(String[] args) {
        Connection conn = null;
        try {
            conn = ConnectionUtil.getConn(jdbcUrl);
            long start = System.currentTimeMillis();
            doSearch();
            long end = System.currentTimeMillis();
            System.out.println("time consume: " + (end - start));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
           ConnectionUtil.closeConnection(conn);
        }
    }


    private static void doSearch(){
        long start = 111;
        long end = System.currentTimeMillis();
        String agg = "count";
        String tableName = "t_test";
        Map<String,String> tags = new HashMap<>();
        tags.put("sensor","'sensor3'");
        String sql = generateSql(start,end,agg,tableName,tags);
        sql = String.format(sql);

        System.out.println(sql);
        try {
            double res = ConnectionUtil.sqlExecuteWithResult(ConnectionUtil.getConn(jdbcUrl),sql);
            System.out.println(res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String generateSql(long startTime, long endTime, String aggregation, String tableName, Map<String,String> tags){


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
        sqlSB.append("select ").append(aggregation).append("(").append("dt").append(")").append(" from ").append(tableName)
                .append(" where ts >=").append(startTime).append(" and ts<= ").append(endTime).append(tagSb.toString());

        return sqlSB.toString() ;

    }











}
