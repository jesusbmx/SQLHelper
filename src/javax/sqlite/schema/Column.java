package javax.sqlite.schema;

import javax.util.DBUtils;

/**
 * id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL DEFAUL '' +
 */
public class Column {
  final String name;
  final String type;
  private boolean primaryKey = false;
  private boolean autoIncrement = false;
  private boolean nullable = false;
  private String defaultVal = null;

  public Column(String name, String type) {
    this.name = name;
    this.type = type;
  }
  
  public Column primaryKey() {
    primaryKey = true;
    return this;
  }
  public Column autoIncrement() {
    autoIncrement = true;
    return this;
  }

  public Column defaultVal(String defaultVal) {
    this.defaultVal = defaultVal;
    return this;
  }

  public Column nullable() {
    nullable = true;
    return this;
  }

  @Override public String toString() {
    StringBuilder sql = new StringBuilder();
    // column-def:
    sql.append(name).append(" ").append(type);
    // column-constraint:
    if (primaryKey) sql.append(" PRIMARY KEY");
    if (autoIncrement) sql.append(" AUTOINCREMENT");

    sql.append((nullable) ? " NULL" : " NOT NULL");
    if (defaultVal != null) {
      sql.append(" DEFAULT ");
      DBUtils.appendEscapedSQLString(sql, defaultVal);
    }
    return sql.toString();
  }
}
