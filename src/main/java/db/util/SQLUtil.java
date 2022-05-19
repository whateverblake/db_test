package db.util;

import com.taosdata.jdbc.TSDBDriver;
import db.entity.DataPoint;
import db.entity.ReaderEntity;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SQLUtil {


    public static Connection getConn(String jdbcUrl, String driverName) throws Exception {
        Class.forName(driverName);
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
        return conn;
    }


    public static void preparedStatementExecute(String sql, List<DataPoint> dataPointList) {
        Connection connection = Druid.getConnection();
        PreparedStatement pstm = null;

        try {
            pstm = connection.prepareStatement(sql);
            for (DataPoint dataPoint : dataPointList) {
                int index = 1;
//                pstm.setString(index,dataPoint.getSensorId());
//                index++;
                pstm.setLong(index, dataPoint.getTime());
                index++;
                pstm.setDouble(index, dataPoint.getValue());
                index++;
                List<String> tags = dataPoint.getTags();
                for (String tag : tags) {
                    pstm.setString(index, tag);
                    index++;
                }

                pstm.addBatch();
            }
            pstm.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            Druid.releaseConnection(connection, pstm, null);
        }
    }


    public static void sqlExecute(List<String> sqls) {
        Connection connection = Druid.getConnection();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            for (String sql : sqls) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            Druid.releaseConnection(connection, statement, null);
        }
    }


    public static void sqlExecute(String sql) {
        Connection connection = Druid.getConnection();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            Druid.releaseConnection(connection, statement, null);
        }
    }

    public static double sqlExecuteWithResult(String sql) {
        Connection connection = Druid.getConnection();
        Statement statement = null;
        ResultSet rs = null;
        double result = 0;
        try {
            statement = connection.createStatement();
            rs = statement.executeQuery(sql);
            if (rs != null) {
                while (rs.next()) {
                    result = rs.getDouble(1);
                    break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            Druid.releaseConnection(connection, statement, rs);
        }
        return result;
    }


    public static String sqlGenerateForTDB(ReaderEntity readerEntity, String tableName) {
        long startTime = readerEntity.getStartTime();
        long endTime = readerEntity.getEndTime();
        String aggregation = readerEntity.getAggregation();
        Map<String, String> tags = readerEntity.getTags();

        StringBuilder tagSb = new StringBuilder();
        if (tags != null && tags.size() > 0) {
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                tagSb.append(" and ");
                String tag = entry.getKey();
                String value = entry.getValue();
                tagSb.append(tag).append("= '").append(value).append("'");
            }
        }
        StringBuilder sqlSB = new StringBuilder();
        sqlSB.append("select ").append(aggregation).append("(").append("value").append(")").append(" from ").append(tableName)
                .append(" where time >=").append(startTime).append(" and time<= ").append(endTime).append(tagSb.toString());

        return sqlSB.toString();

    }


}
