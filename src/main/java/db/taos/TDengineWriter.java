package db.taos;

import db.entity.DataPoint;
import db.util.SQLUtil;
import db.writer.BaseWriter;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TDengineWriter extends BaseWriter {

//    private static Random random = new Random();

//    private static String jdbcUrl = "jdbc:TAOS-RS://127.0.0.1:6041/test?user=root&password=taosdata";
//    public static void main(String[] args) {
//        int batch = 1;
//        int size = 50;
//        Connection conn = null;
//        try {
//            conn = SQLUtil.getConn(jdbcUrl);
//            createTable(conn);
//            long start = System.currentTimeMillis();
////            truncateTable("device1",conn);
//            writeBatch(batch, size, conn);
//            long end = System.currentTimeMillis();
//            System.out.println("time consume: " + (end - start));
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            SQLUtil.closeConnection(conn);
//        }
//    }


    private List<String> tags;


    public TDengineWriter(List<String> tags) {
        this.tags = tags;
        doPreJob();
    }


//    private static List<String> generateSql(int batch) {
//        List<String> sqlList = new ArrayList<>();
//        for (int i = 0; i < batch; i++) {
//            StringBuilder sqlSb = new StringBuilder();
//            sqlSb.append("insert into device1 using t_test TAGS('sensor3') values(").append(System.currentTimeMillis()).append(",")
//                    .append(generateValue())
////                    .append(",'sensor02'")
//                    .append(")");
//            sqlList.add(sqlSb.toString());
//        }
//
//        return sqlList;
//    }

//    private static void createTable(Connection conn) {
//        try {
//            Statement stmt = conn.createStatement();
//            stmt.executeUpdate("create table if not exists t_test (ts timestamp, dt double) tags(sensor NCHAR(10))");
//            stmt.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally {
//
//        }
//    }


    public void createTable() {

        StringBuilder baseSql = new
                StringBuilder("create table sensor_data(time timestamp ,value double)");
        if (tags != null && tags.size() > 0) {
            baseSql.append(" tags(");
            for (String tag : tags) {
                baseSql.append(tag).append(" NCHAR(50),");
            }
        }

        String createSql = baseSql.replace(baseSql.length() - 1, baseSql.length(), ")").toString();

        try {
            SQLUtil.sqlExecute(createSql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void dropTable() {
        String sql = "DROP TABLE sensor_data";
        List<String> sqls = new ArrayList<>();
        sqls.add(sql);
        SQLUtil.sqlExecute(sqls);
    }


    @Override
    public void saveDataPoint(List<DataPoint> dataPointList) {

        List<String> sqls = dataPointToInsertSQL(dataPointList);
        SQLUtil.sqlExecute(sqls);

    }


    public List<String> dataPointToInsertSQL(List<DataPoint> dataPointList) {
        List<String> sqls = new ArrayList<>();
        for (DataPoint dataPoint : dataPointList) {


            String tagSQL = null;
            if (tags != null && tags.size() > 0) {
                StringBuilder tagSB = new StringBuilder(" tags(");
                for (String tag : tags) {
                    tagSB.append("'").append(tag).append("',");
                }
                tagSQL = tagSB.replace(tagSB.length() - 1, tagSB.length(), ")").toString();
            }

            String sensorId = dataPoint.getSensorId();
            StringBuilder sqlSb = new StringBuilder();
            sqlSb = sqlSb.append("insert into ")
                    .append(sensorId).append(" using sensor_data ");

            if (tagSQL != null) {
                sqlSb = sqlSb.append(tagSQL);
            }
            sqlSb.append(" values(")
                    .append(dataPoint.getTime())
                    .append(", ").append(dataPoint.getValue()).append(" )");

            sqls.add(sqlSb.toString());
        }

        return sqls;
    }
}
