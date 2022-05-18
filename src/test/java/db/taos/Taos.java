package db.taos;

import com.alibaba.druid.pool.DruidDataSource;
import db.util.SQLUtil;

import java.sql.Connection;
import java.sql.Statement;

public class Taos {

    public static  String url = "jdbc:TAOS-RS://127.0.0.1:6041/test?user=root&password=taosdata";
    public static String driverClass = "com.taosdata.jdbc.TSDBDriver";

    public static void main(String[] args) {

//        localDriver();
        connectByDruid();

    }

    public static void connectByDruid(){
        try {
            DruidDataSource dataSource = new DruidDataSource();
            // jdbc properties
            dataSource.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
            dataSource.setUrl(url);
//            dataSource.setUsername("root");
//            dataSource.setPassword("taosdata");
            // pool configurations
            dataSource.setInitialSize(10);
            dataSource.setMinIdle(10);
            dataSource.setMaxActive(10);
            dataSource.setMaxWait(30000);
            dataSource.setValidationQuery("select server_status()");
            Connection connection = dataSource.getConnection(); // get connection
            Statement statement = connection.createStatement(); // get statement
            connection.close(); // put back to connection pool
        }catch (Exception e){

        }
    }

    public static void localDriver(){

        try {
           Connection connection =  SQLUtil.getConn(url,driverClass);
            System.out.println(connection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
