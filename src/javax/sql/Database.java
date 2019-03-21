package javax.sql;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface Database {

  public ResultSet query(String toString) throws SQLException;
  
}
