package db.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Druid {

    private static Logger logger = LoggerFactory.getLogger(Druid.class);



    private static String dbconfig = "dbconfig.properties";
    static DataSource dataSource;

    static {

        try{
            String directory = SystemPropertiesConfigUtil.getSystemConfigFileDirectory("db.config","configuration");

            dbconfig  = directory+dbconfig ;
            File file = new File(dbconfig);

            logger.info("......dbconfig path...... = {}",file.exists());
            FileInputStream fis = new FileInputStream(file);
            Properties properties = new Properties();
            properties.load(fis);

            dataSource = DruidDataSourceFactory.createDataSource(properties);

        }catch (Exception e){
            e.printStackTrace();
            logger.error("init db source error",e);
            System.exit(-1);
        }
    }


    public static Connection getConnection(){

        Connection connection = null;
        try {
            connection  = dataSource.getConnection() ;
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("get connection error",e);
        }

        return connection ;
    }


    public static void releaseConnection(Connection connection, Statement statement, ResultSet resultSet){

        try{
            if(resultSet != null && !resultSet.isClosed()){
                resultSet.close();
            }

        }catch (Exception e){
            e.printStackTrace();
            logger.error("close resultSet error",e);
        }

        try{
            if(statement != null && !statement.isClosed()){
                statement.close();
            }

        }catch (Exception e){
            e.printStackTrace();
            logger.error("close statement error",e);
        }

        try{
            if(connection != null && !connection.isClosed()){
                connection.close();
            }

        }catch (Exception e){
            e.printStackTrace();
            logger.error("close connection error",e);
        }

    }






}
