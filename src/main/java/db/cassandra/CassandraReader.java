package db.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.HashMap;
import java.util.Map;

public class CassandraReader {
    private static Cluster cluster =
            Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();

    public static void main(String[] args) {

        String keyspace = "db_test";
        Session session = cluster.connect(keyspace);

        String sensorId = "sensor1";
        long start = 111;
        long end = System.currentTimeMillis();
        String agg = "avg";
        String tableName = "sensor_data";
        Map<String,String> tags = new HashMap<>();
        tags.put("tag1","'tag1'");
        String sql = generateQuerySql(sensorId,start,end,tags,agg,tableName);
        sql = String.format(sql);
        System.out.println(sql);
        query(sql,session);


        session.close();
        cluster.close();






    }



    public static String generateQuerySql(String sensorId, long startTime, long endTime, Map<String,String> tags,String aggregation,String tableName){

        StringBuilder tagSb = new StringBuilder() ;
        if(tags != null && tags.size()>0){
            for(Map.Entry<String,String> entry : tags.entrySet() ){
                tagSb.append(" and ");
                String tag = entry.getKey();
                String value = entry.getValue();
                tagSb.append(tag).append("=").append(value);
            }
        }


        StringBuilder sqlSB  = new StringBuilder();
        sqlSB.append("select ").append(aggregation).append("(").append("value").append(")").append(" from ").append(tableName)
                .append(" where time >=").append(startTime).append(" and time<= ").append(endTime)
                .append(" and sensor_id='").append(sensorId).append("' ")
                .append(tagSb.toString())
        ;

        return sqlSB.toString() ;
    }



    public static void  query(String sql,Session session){
        long start = System.currentTimeMillis();
        ResultSet rs = session.execute(sql);
        Row row = rs.one();
        System.out.println(row);
//        Object result = row.get(0,Double.class);
//        System.out.println(result.toString());
        long end = System.currentTimeMillis();

        System.out.println("time consume :"+(end-start));
    }


}
