package db.sqlite;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import db.entity.DataPoint;
import db.timescale.TimeScaleDBWriter;
import db.util.ConnectionUtil;
import org.omg.CORBA.DATA_CONVERSION;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SqliteWrite {

    private static Random random = new Random();

    private static String path = "/Users/blake/intellij_workspace/db_test";
    private static String url = "jdbc:sqlite:" + path + "/test.db";

    public static void main(String[] args) {
        createTable();
        try {
            String sql = "insert into sensor_data(sensor_id,value,tag1,time) values(?,?,?,?)";
            Connection connection = ConnectionUtil.getConn(url);
            PreparedStatement pstm = connection.prepareStatement(sql);

            int size = 50;
            int batch = 1;
            int iteration = (int) Math.ceil(size / batch);
            for (int i = 0; i < iteration; i++) {
                    List<DataPoint> dataPoints = generateDataPoint(batch,"sensor1");
                    try {
                        pstm = connection.prepareStatement(sql);
                        for (DataPoint dataPoint : dataPoints) {
                            pstm.setString(1, dataPoint.getSensorId());
                            pstm.setDouble(2, dataPoint.getValue());
                            pstm.setString(3, dataPoint.getTag());
                            pstm.setLong(4, dataPoint.getTime());
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

        } catch (Exception e) {
            e.printStackTrace();
        }

    }







    public static void createTable() {
        String createSQL = "create table sensor_data(sensor_id CHAR(50),value double,tag1 CHAR(50),time)";
        String index1 = "CREATE INDEX sensor_id ON sensor_data(sensor_id)";
        String index2 = "CREATE INDEX tag1 ON sensor_data(tag1)";
        String index3 = "CREATE INDEX time ON sensor_data(time)";
        List<String> sqls = new ArrayList<>();
        sqls.add(createSQL);
        sqls.add(index1);
        sqls.add(index2);
        sqls.add(index3);
        Connection connection = null;
        try {
            connection = ConnectionUtil.getConn(url);
            ConnectionUtil.sqlExecute(connection,sqls);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }







    public static List<DataPoint> generateDataPoint(int batch, String sensorId) {
        String tag = "tag1";
        List<DataPoint> dataPointList = new ArrayList<>();
        for (int i = 0; i < batch; i++) {
            dataPointList.add(new DataPoint(sensorId, System.currentTimeMillis(), generateValue(), tag));
        }

        return dataPointList;
    }


    private static Double generateValue() {
        return random.nextDouble() * 100;
    }


}
