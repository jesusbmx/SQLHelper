package javax.sqlite.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.util.DBUtils;

/**
 * FOREIGN KEY(id_inventario) REFERENCES tb_inventario(id) ON DELETE CASCADE
 */
public class Constraint {
  private final List<String> foreign = new ArrayList<String>();
  private final List<String> references = new ArrayList<String>();
  private String table = null;
  private String onDelete = null;

  public Constraint foreign(String... key) {
    foreign.addAll(Arrays.asList(key));
    return this;
  }
  public Constraint references(String... key) {
    references.addAll(Arrays.asList(key));
    return this;
  }
  public Constraint on(String table) {
    this.table = table;
    return this;
  }
  public void onDelete(String on) {
    this.onDelete = on;
  }
  public void onDelete() {
    onDelete("CASCADE");
  }
  @Override public String toString() {
    StringBuilder sql = new StringBuilder();
    if (!foreign.isEmpty())
      sql.append("FOREIGN KEY (").append(DBUtils.joinToStr(foreign)).append(')');

    if (table != null && !references.isEmpty())
      sql.append(" REFERENCES ").append(table).append('(').append(DBUtils.joinToStr(references)).append(')');

    if (onDelete != null)
      sql.append(" ON DELETE ").append(onDelete);

    return sql.toString();
  }
}
