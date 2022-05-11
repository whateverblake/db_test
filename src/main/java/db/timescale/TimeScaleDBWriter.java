package db.timescale;

import db.util.ConnectionUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TimeScaleDBWriter {
    private static Random random = new Random();

    private static String jdbcUrl = "jdbc:postgresql://127.0.0.1:5432/db_test?user=postgres&password=123456";

    public static void main(String[] args) {
        Connection conn = null;
        try {
            conn = ConnectionUtil.getConn(jdbcUrl);
//            System.out.println(conn);
//            createTable(conn);
            insertData(conn);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConnection(conn);
        }


    }


    private static void createTable(Connection conn) {
//        String sql1 = "create table sensor_data(time bigint,dt DOUBLE PRECISION, tag1 text)";
//        String sql2 = "SELECT create_hypertable('sensor_data','time',chunk_time_interval => 1000)";
        String sql3 = "create index on sensor_data(tag1,time desc)";
        List<String> sqls = new ArrayList<>();
//        sqls.add(sql1);
//        sqls.add(sql2);
        sqls.add(sql3);
        ConnectionUtil.sqlExecute(conn, sqls);
    }


    public static void insertData(Connection connection) {
        String sql = "insert into sensor_data values(?,?,?) ";
        int size = 50;
        int batch = 1;
        int iteration = (int) Math.ceil(size / batch);
        PreparedStatement pstm = null;
        for (int i = 0; i < iteration; i++) {
            List<DataPoint> dataPoints = generateDataPoint(batch);
            try {
                pstm = connection.prepareStatement(sql);
                for (DataPoint dataPoint : dataPoints) {
                    pstm.setLong(1, dataPoint.time);
                    pstm.setDouble(2, dataPoint.value);
                    pstm.setString(3, dataPoint.tag);
                    pstm.addBatch();
                }
                pstm.executeBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (pstm != null) {
                    ConnectionUtil.closeStatement(pstm);
                }
            }

        }


    }


    public static List<DataPoint> generateDataPoint(int batch) {
        String tag = "tag1";
        List<DataPoint> dataPointList = new ArrayList<>();
        for (int i = 0; i < batch; i++) {
            dataPointList.add(new DataPoint(System.currentTimeMillis(), generateValue(), tag));
        }

        return dataPointList;
    }

    static class DataPoint {

        private long time;
        private double value;
        private String tag;

        public DataPoint() {
        }

        public DataPoint(long time, double value, String tag) {
            this.time = time;
            this.value = value;
            this.tag = tag;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }
    }


    private static Double generateValue() {
        return random.nextDouble() * 100;
    }
}
