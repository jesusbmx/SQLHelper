package javax.sqlite.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.util.DBUtils;

/**
 * CREATE UNIQUE INDEX IF NOT EXISTS 'index_picking'
 * ON 'tb_picking' ('folio')
 */
public class Index {
  final String name;
  final String tablename;
  private boolean unique = false;
  private List<String> columns = new ArrayList<String>();

  public Index(String name, String tablename) {
    this.name = name;
    this.tablename = tablename;
  }
  
  public void unique(String... columns) {
    this.unique = true;
    this.columns(columns);
  }

  public void columns(String... columns) {
    this.columns.addAll(Arrays.asList(columns));
  }

  @Override public String toString() {
    StringBuilder sql = new StringBuilder();
    sql.append("CREATE");
    if (unique) sql.append(" UNIQUE");
    sql.append(" INDEX IF NOT EXISTS ");
    DBUtils.appendEscapedSQLString(sql, name);
    sql.append(" ON ");
    DBUtils.appendEscapedSQLString(sql, tablename);
    sql.append(" (");
    sql.append(DBUtils.joinToStr(columns, new Function<Object, String>() {
      @Override public String apply(Object it) {
        return DBUtils.sqlEscapeString(String.valueOf(it));
      }
    }));
    sql.append(')');
    return sql.toString();
  }
 
}
