package db.sqlite;

import db.entity.DataPoint;
import db.util.SQLUtil;
import db.writer.BaseWriter;

import java.util.ArrayList;
import java.util.List;

public class SqliteWriter extends BaseWriter {


    public List<String> tags;

    public SqliteWriter(List<String> tags) {
        this.tags = tags;
    }

    private static String insertSQL;


//    public void doPreJob() {
//        synchronized (lock) {
//            if (!init) {
//                dropTable();
//                createTable();
//                init = true;
//            }
//        }
//    }


    public  void dropTable() {
        try {
            String sql = "drop table sensor_data";
            SQLUtil.sqlExecute(sql);
        }catch (Exception e){

        }

    }

    public void createTable() {
        StringBuilder insertSQLSB = new StringBuilder("insert into sensor_data(time,value,");
//        String indexSensorId = "CREATE INDEX sensor_id ON sensor_data(sensor_id)";
        String indexTime = "CREATE INDEX time ON sensor_data(time)";
        List<String> indices = new ArrayList<>();
//        indices.add(indexSensorId);
        indices.add(indexTime);

        StringBuilder baseSql = new
                StringBuilder("create table sensor_data(time bigint ,value double,");
        for (String tag : tags) {
            baseSql.append(tag).append(" CHAR(50),");
            indices.add(new StringBuilder("CREATE INDEX ").append(tag).append(" on ")
                    .append("sensor_data(").append(tag).append(")").toString());
            insertSQLSB.append(tag).append(",");

        }

        String createSql = baseSql.replace(baseSql.length() - 1, baseSql.length(), ")").toString();
        insertSQLSB = insertSQLSB.replace(insertSQLSB.length() - 1, insertSQLSB.length(), ")");
        insertSQLSB.append(" values(?,?");
        for(int i = 0; i < tags.size(); i++){
            insertSQLSB.append(",?");
        }
        insertSQLSB.append(")");
        insertSQL = insertSQLSB.toString();

        List<String> sqls = new ArrayList<>();
        sqls.add(createSql);
        sqls.addAll(indices);
        try {
            SQLUtil.sqlExecute(sqls);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void saveDataPoint(List<DataPoint> dataPointList) {
        SQLUtil.preparedStatementExecute(insertSQL, dataPointList);
    }

}
