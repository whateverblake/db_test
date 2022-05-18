package db.timescale;

import db.entity.DataPoint;
import db.util.SQLUtil;
import db.writer.BaseWriter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TimeScaleDBWriter extends BaseWriter {

    private static String insertSQL ;
    List<String> tags ;

    public TimeScaleDBWriter(List<String> tags){
        this.tags = tags ;
        doPreJob();
    }


    public void dropTable(){
        try{
            String sql = "drop table sensor_data";
            SQLUtil.sqlExecute(sql);
        }catch (Exception e){

        }

    }

    public  void createTable() {
        String createHyperTable = "SELECT create_hypertable('sensor_data','time',chunk_time_interval=>60000);";
        StringBuilder insertSQLSB = new StringBuilder("insert into sensor_data(time,value,") ;
//        String indexSensorId = "CREATE INDEX sensor_id ON sensor_data(sensor_id,time desc)";
        String indexTime = "CREATE INDEX time ON sensor_data(time)";
        List<String> indices = new ArrayList<>();
//        indices.add(indexSensorId);
        indices.add(indexTime);

        StringBuilder baseSql = new
                StringBuilder("create table sensor_data(time bigint ,value double PRECISION,");
        for (String tag : tags){
            baseSql.append(tag).append(" char(50),");
            indices.add(new StringBuilder("CREATE INDEX ").append(tag).append(" on ")
                    .append("sensor_data(").append(tag).append(", time desc)").toString());
            insertSQLSB.append(tag).append(",");

        }

        String createSql = baseSql.replace(baseSql.length()-1,baseSql.length(),")").toString();
        insertSQLSB = insertSQLSB.replace(insertSQLSB.length()-1,insertSQLSB.length(),")");

        insertSQLSB.append(" values(?,?");
        for(int i = 0; i < tags.size(); i++){
            insertSQLSB.append(",?");
        }
        insertSQLSB.append(")");
        insertSQL = insertSQLSB.toString();

        List<String> sqls = new ArrayList<>();
//        sqls.add(createSql);
        try {
            SQLUtil.sqlExecute(createSql);
            SQLUtil.sqlExecute(createHyperTable);
        }catch (Exception e){

        }

        sqls.addAll(indices);
        try {
            SQLUtil.sqlExecute(sqls);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void saveDataPoint(List<db.entity.DataPoint> dataPointList){
        SQLUtil.preparedStatementExecute(insertSQL,dataPointList);
    }

}
