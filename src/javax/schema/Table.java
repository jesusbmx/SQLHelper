
package javax.schema;

import java.util.Iterator;
import java.util.LinkedHashSet;
import javax.util.DBUtils;

public class Table implements Iterable<Column> {
  public final String name;
  public final LinkedHashSet<Column> columns = new LinkedHashSet<Column>();
  public final LinkedHashSet<Constraint> constraints = new LinkedHashSet<Constraint>();
  public final LinkedHashSet<Index> indexs = new LinkedHashSet<Index>();

  public Table(String name) {
    this.name = name;
  }
  
  @Override public Iterator<Column> iterator() {
    return columns.iterator();
  }
  
  public Column addColumn(String columname, String type) {
    Column col = new Column(columname, type);
    columns.add(col);
    return col;
  }

  public Constraint addConstraint() {
    Constraint constraint = new Constraint();
    constraints.add(constraint);
    return constraint;
  }

  public Index addIndex(String indexname) {
    Index index = new Index(indexname, name);
    indexs.add(index);
    return index;
  }

  /** Incrementing ID to the table (primary key)  */
  public Column increments(String column){
    return integer(column).primaryKey().autoIncrement();
  }
  
  /** INTEGER equivalent to the table  */
  public Column integer(String column){
    return addColumn(column, "INTEGER");
  }
  
  /** DECIMAL equivalent with a precision and scale  */
  public Column real(String column) {
    return addColumn(column, "REAL");
  }
  
  /** VARCHAR equivalent column  */
  public Column text(String column) {
    return addColumn(column, "TEXT");
  }
  
  /** Binary equivalent column  */
  public Column blob(String column) {
    return addColumn(column, "BLOB");
  }

  @Override public String toString() {
    StringBuilder sql = new StringBuilder();
    sql.append("CREATE TABLE IF NOT EXISTS ").append(name).append('(');
    sql.append(DBUtils.joinToStr(DBUtils.concatList(columns, constraints) ));
    sql.append(')');
    return sql.toString();
  }

  public Constraint foreign(String columnname) {
    return addConstraint().foreign(columnname);
  }

  public Index index(String indexname) {
    return addIndex(indexname);
  }
}
