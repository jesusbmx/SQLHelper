
package javax.sqlite.schema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import javax.sqlite.SQLiteDatabase;
import javax.util.Debug;
import javax.util.DBUtils;

public class Schema {
  static final int FIRST_INDEX = 1;
  
  final SQLiteDatabase db;
  boolean debugger = false;

  public Schema(SQLiteDatabase db) {
    this.db = db;
  }
  
  public boolean isDebugger() {
    return debugger;
  }
  public void setDebugger(boolean debugger) {
    this.debugger = debugger;
  }
   
  /**
   * Ejecuta una sentecia sql.
   */
  public void execSQL(String sql) throws SQLException {
    if (isDebugger())
      Debug.i(Schema.class, sql);

    db.execSQL(sql);
  }
  
  /**
   * Obtiene un lista de columnas que ha un no exiten fisicamente en la tabla.
   */
  public LinkedHashSet<Column> getColumnsNotExist(Table table) throws SQLException {
    String sql = "SELECT * FROM " + table.name + " LIMIT 1";
    ResultSet r = db.query(sql);

    try {
      LinkedHashSet<Column> set = new LinkedHashSet<Column>();
      for (Column col : table) {
        // see if the column is there
        int columnIndex = r.findColumn(col.name);
        if (columnIndex < FIRST_INDEX) {
          // missing_column not there - add it
          set.add(col);
        }
      }
      return set;
    } finally {
      DBUtils.closeQuietly(r);
    }
  }

  /**
   * Valida si existe una tabla en la base de datos.
   */
  public boolean hastTable(String tablename) throws SQLException {
    String sql = "SELECT COUNT(*) FROM sqlite_master WHERE type = ? AND name = ?";
    ResultSet r = db.query(sql, "table", tablename);

    try {
      return (r.next()) ? (r.getInt(FIRST_INDEX) > 0) : (false);
    } finally {
      DBUtils.closeQuietly(r);
    }
  }
  
   /**
   * Crea o actualiza una tabla en la base de datos.
   */
  public Table table(String tablename, Closure closure) throws SQLException {
    Table table = new Table(tablename);
    closure.call(table);

    if (hastTable(tablename))
      addColumnsIfNotExists(table);
    else
      execSQL(table.toString());

    for (Index index : table.indexs)
      execSQL(index.toString());

    return table;
  }
  
  /**
   * Agrega las columnas nuevas que no existen fisicamente en la tabla.
   */
  public void addColumnsIfNotExists(Table table) throws SQLException {
    LinkedHashSet<Column> set = getColumnsNotExist(table);
    for (Column col : set)
      execSQL("ALTER TABLE " + table.name + " ADD COLUMN " + col);
  }
}
