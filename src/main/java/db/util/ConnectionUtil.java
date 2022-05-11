package db.util;

import com.taosdata.jdbc.TSDBDriver;

import java.sql.*;
import java.util.List;
import java.util.Properties;

public class ConnectionUtil {


    public static Connection getConn(String jdbcUrl) throws Exception {
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
        return conn;
    }



    public static void sqlExecute(Connection connection, List<String> sqls) {
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
            closeStatement(statement);
        }
    }

    public static double sqlExecuteWithResult(Connection connection, String sql) {
        Statement statement = null;
        ResultSet rs = null ;
        double result = 0 ;
        try {
            statement = connection.createStatement();
            rs = statement.executeQuery(sql);
            if(rs != null){
                while (rs.next()){
                    result =  rs.getDouble(1);
                    break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeStatement(statement);
            closeResultSet(rs);
        }
        return result ;
    }

    public static void closeConnection(Connection conn){
        if (conn != null) {
            try {
                conn.close();
                System.exit(0);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeStatement(Statement statement){
        try {
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void closeResultSet(ResultSet resultSet){
        try {
            resultSet.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
