package db.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import db.entity.DataPoint;
import db.timescale.TimeScaleDBWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CassandraWriter {


    private static Cluster cluster =
            Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();

    private static Random random = new Random();


    public static void main(String[] args) {
        String keyspace = "db_test";
        Session session = cluster.newSession();
        createKeyspace(keyspace,"SimpleStrategy",1,session);
        session = cluster.connect(keyspace);
        createTable("sensor_data",session);
        PreparedStatement preparedStatement = session.prepare("insert into sensor_data(sensor_id,time,tag1,value) values(?,?,?,?)");
        int size = 50;
        int batch = 1;
        int iteration = (int) Math.ceil(size / batch);
        for (int i = 0; i < iteration; i++) {
            List<DataPoint> dataPoints = generateDataPoint(batch, "sensor1");
            rawDataInsert(dataPoints, preparedStatement, session);
            System.out.println("iteration :"+i+" over ");

        }

    }


    private static void rawDataInsert(List<DataPoint> dataPointList, PreparedStatement preparedStatement, Session
            session) {

        try {

            BatchStatement batchStatement = new BatchStatement();


            for (DataPoint dataPoint : dataPointList) {
                BoundStatement boundStatement = preparedStatement.bind(
                        dataPoint.getSensorId(), dataPoint.getTime(),
//                        dataPoint.getTag(),
                        dataPoint.getValue());
                batchStatement.add(boundStatement);
            }
            session.execute(batchStatement);
            batchStatement.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static List<DataPoint> generateDataPoint(int batch, String sensorId) {
        String tag = "tag1";
        List<DataPoint> dataPointList = new ArrayList<>();
        for (int i = 0; i < batch; i++) {
//            dataPointList.add(new DataPoint(sensorId, System.currentTimeMillis(), generateValue(), tag));
        }

        return dataPointList;
    }


    private static Double generateValue() {
        return random.nextDouble() * 100;
    }



    public static  void createKeyspace(
            String keyspaceName, String replicationStrategy, int replicationFactor,Session session) {
        StringBuilder sb =
                new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                        .append(keyspaceName).append(" WITH replication = {")
                        .append("'class':'").append(replicationStrategy)
                        .append("','replication_factor':").append(replicationFactor)
                        .append("};");

        String query = sb.toString();
        session.execute(query);
    }

    public static void createTable(String tableName,Session session) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(tableName).append("(")
                .append("sensor_id text, ")
                .append("time bigint,")
                .append("tag1 text,")
                .append("value double,")
                .append("PRIMARY KEY(sensor_id,tag1,time));");

        String query = sb.toString();
        session.execute(query);
    }


}
