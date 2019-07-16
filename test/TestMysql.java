
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import javax.sql.SQLDataSource;
import javax.sql.SQLDatabase;
import javax.util.DBUtils;

public class TestMysql {

  public static SQLDataSource src = new SQLDataSource()
          .setDriverClassName("com.mysql.jdbc.Driver")
          .setUrl("jdbc:mysql://192.168.1.25:3306/colegio")
          .setUsername("root")
          .setPassword("")
          .setDebuggable(true);
  
  
  public List<Map<String, Object>> get() throws SQLException {
    ResultSet rs = null;
    try {
      SQLDatabase db = src.getDatabase();
      rs = db.query("SELECT * FROM alumno WHERE id > ? ORDER BY nombre", 2);
      return DBUtils.fromList(rs);
    } finally {
      DBUtils.closeQuietly(rs);
      src.close();
    }
  }
  
  public static void main(String[] args) throws SQLException {
    TestMysql testMysql = new TestMysql();
    for (Map<String, Object> row : testMysql.get()) {
      System.out.println(row);
    }
  }
}
