package db.taos;

import com.taosdata.jdbc.TSDBDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class TDengineWriter {

    private static Random random = new Random();


    public static void main(String[] args) {
        int batch = 1;
        int size = 50;
        Connection conn = null;
        try {
            conn = ConnectionUtil.getConn();
            createTable(conn);
            long start = System.currentTimeMillis();
//            truncateTable("device1",conn);
            writeBatch(batch, size, conn);
            long end = System.currentTimeMillis();
            System.out.println("time consume: " + (end - start));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ConnectionUtil.closeConnection(conn);
        }
    }


    private static List<String> generateSql(int batch) {
        List<String> sqlList = new ArrayList<>();
        for (int i = 0; i < batch; i++) {
            StringBuilder sqlSb = new StringBuilder();
            sqlSb.append("insert into device1 using t_test TAGS('sensor3') values(").append(System.currentTimeMillis()).append(",")
                    .append(generateValue())
//                    .append(",'sensor02'")
                    .append(")");
            sqlList.add(sqlSb.toString());
        }

        return sqlList;
    }

    private static void createTable(Connection conn) {
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create table if not exists t_test (ts timestamp, dt double) tags(sensor NCHAR(10))");
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {

        }
    }


    private static Double generateValue() {
        return random.nextDouble() * 100;
    }

    public static void writeBatch(int batch, int size, Connection connection) {

        int iteration = (int) Math.ceil(size / batch);

        for (int i = 0; i < iteration; i++) {
            List<String> sqls = generateSql(batch);
            ConnectionUtil.sqlExecute(connection,sqls);
        }

    }



    private static void  truncateTable(String table,Connection connection){
        String sql = "DROP TABLE '"+table +"'";
        List<String> sqls = new ArrayList<>();
        sqls.add(sql);
        ConnectionUtil.sqlExecute(connection,sqls);
    }

}
