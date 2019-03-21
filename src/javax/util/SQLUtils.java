package javax.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class SQLUtils {

  private SQLUtils() {
  }
   
  public static void prepareBind(PreparedStatement ps, Object... bindArgs)
          throws SQLException {
    if (bindArgs != null)
      for (int i = 0; i < bindArgs.length; i++) 
        ps.setObject(i + 1, bindArgs[i]);
  }

  public static void closeQuietly(AutoCloseable closeable) {
    if (closeable == null) return;
    try {
      closeable.close();
    } catch(Exception e) {
    }
  }
  
  public static void closeQuietly(ResultSet resultSet) {
    if (resultSet == null) return;
    try {
      resultSet.close();
    } catch(SQLException e) {
    }
  }

  public static void closeQuietly(Statement statement) {
    if (statement == null) return;
    try {
      statement.close();
    } catch(SQLException e) {
    }
  }
  
  public static void closeQuietly(PreparedStatement statement) {
    if (statement == null) return;
    try {
      statement.close();
    } catch(SQLException e) {
    }
  }
  
  public static void closeQuietly(Connection connection) {
    if (connection == null) return;
    try {
      connection.close();
    } catch(SQLException e) {
    }
  }
  
  public static void appendClause(StringBuilder s, String name, Object clause) {
    if (clause != null) appendClause(s, name, clause.toString());
  }
  
  public static void appendClause(StringBuilder s, String name, String clause) {
    if (!isEmpty(clause)) 
      s.append(name).append(clause);
  }
  
  /**
   * Add the names that are non-null in columns to s, separating them with
   * commas.
   */
  public static void appendColumns(StringBuilder s, String[] columns) {
    for (int i = 0; i < columns.length; i++) {
      String column = columns[i];
      if (column != null) {
        if (i > 0) {
          s.append(", ");
        }
        s.append(column);
      }
    }
    s.append(' ');
  }
  
  public static List<Map<String, Object>> fromList(ResultSet rs, boolean closed) 
  throws SQLException {
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    resultSetToList(list, rs, closed);
    return list;
  } 
  public static List<Map<String, Object>> fromList(ResultSet rs)
  throws SQLException {
    return fromList(rs, Boolean.FALSE);
  } 
 
  
  public static void resultSetToList(List<Map<String, Object>> list, 
          ResultSet rs, boolean closed) throws SQLException {
    try {
      ResultSetMetaData md = rs.getMetaData();
      int columns = md.getColumnCount();

      while (rs.next()) {
        Map<String, Object> row = new LinkedHashMap<String, Object>(columns);
        for (int i = 1; i <= columns; ++i) {
          row.put(md.getColumnName(i), rs.getObject(i));
        }
        list.add(row);
      }
    } finally {
      if (closed) SQLUtils.closeQuietly(rs);
    }
  }
  
  public static void resultSetToList(List<Map<String, Object>> list, 
          ResultSet rs) throws SQLException {
    resultSetToList(list, rs, Boolean.FALSE);
  }
  
  
  public static void resultSetToMap(
          Map<String, Object> mappedValues, ResultSet rs) throws SQLException {
    if (rs.next()) {
      ResultSetMetaData md = rs.getMetaData();    
      int columns = md.getColumnCount();
      for (int i = 1; i <= columns; ++i) {
        mappedValues.put(md.getColumnName(i), rs.getObject(i));
      }
    }
  }
  
   /**
   * Funcion para concatenear elementos
   *
   * @param elements
   * @return String
   */
  public static String concat(Object... elements) {
    if (isEmpty(elements)) return "";
    StringBuilder sb = new StringBuilder();
    fill(sb, elements);
    return sb.toString();
  }
  
  public static void fill(StringBuilder sb, Object... elements) {
    if (elements != null)
      for (int i = 0; i < elements.length; i++) 
        sb.append(elements[i]);
  }

  public static <T> boolean isEmpty(T... array) {
    return array == null || array.length == 0;
  }
  
  public static boolean isEmpty(CharSequence str) {
    return str == null || str.length() == 0;
  }
}
