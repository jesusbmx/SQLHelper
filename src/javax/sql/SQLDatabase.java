package javax.sql;

import javax.util.SQLUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import javax.util.Debug;

/**
 *
 * @author Jesus
 */
public class SQLDatabase implements AutoCloseable, Database {

// Variables
  
  private SQLDataSource src;
  Connection conn;

// Costructor
  
  public SQLDatabase(SQLDataSource src) {
    this.src = src;
  }

// Funciones  
  
  /**
   * Establece la coneccion con la base de datos.
   *
   * @return la coneccion
   *
   * @throws SQLException
   */
  public Connection getConnection() throws SQLException {
    if (isClosed()) {
      conn = src.connect();
    }
    return conn;
  }

  @Override protected void finalize() throws Throwable {
    try {
      close();
    } finally {
      super.finalize();
    }
  }

  @Override public void close() {
    synchronized (this) {
      SQLUtils.closeQuietly(conn);
      Debug.i(getClass(), "CLOSE ", src.url);
    }
  }
  
  /**
   * @return @true si la base de datos esta cerrada.
   *
   * @throws SQLException
   */
  public boolean isClosed() throws SQLException {
    return conn == null || conn.isClosed();
  }

  /**
   * Prepara sentencias sql.
   *
   * @param sql instruccion a preparar
   *
   * @return PreparedStatement setencia preparada
   *
   * @throws SQLException
   */
  public PreparedStatement compileStatement(String sql) throws SQLException {
    return getConnection().prepareStatement(sql);
  }
  public Statement createStatement() throws SQLException {
    return getConnection().createStatement();
  }
  
  /**
   * Ejecuta consultas a la base de datos.
   *
   * @param sql query a ejecutar
   * @param bindArgs [opcional] parametros del query
   *
   * @return ResultSet con el resultado obtenido
   *
   * @throws SQLException
   */
  public ResultSet query(String sql, Object... bindArgs) throws SQLException {
    PreparedStatement statement = null;
    try {
      statement = compileStatement(sql);
      SQLUtils.prepareBind(statement, bindArgs);
      ResultSet resultSet = SQLResultSet.executeQuery(statement);
       /**/Debug.i(getClass(), SQLUtils.concat(sql, "; ", Arrays.toString(bindArgs)));
      return resultSet;
    } catch(SQLException e) {
      SQLUtils.closeQuietly(statement);
      throw e;
    }
  }
  
  @Override public ResultSet query(String sql) throws SQLException {
    Statement statement = null;
    try {
      statement = createStatement();
      ResultSet resultSet = SQLResultSet.executeQuery(statement, sql);
      /**/Debug.i(getClass(), sql);
      return resultSet;
    } catch(SQLException e) {
      SQLUtils.closeQuietly(statement);
      throw e;
    } 
  }

  /**
   * Ejecuta sentencias a la base de datos.
   *
   * @param sql sentencia a ejecutar
   * @param bindArgs [opcional] parametros del query
   *
   * @return @true resultado obtenido
   *
   * @throws SQLException
   */
  public boolean execSQL(String sql, Object... bindArgs) throws SQLException {
    PreparedStatement statement  = null;
    try {
      statement = compileStatement(sql);
      SQLUtils.prepareBind(statement, bindArgs);
      /**/Debug.i(getClass(), SQLUtils.concat(sql, "; ", Arrays.toString(bindArgs)));
      return statement.execute(sql);
    } finally {
      SQLUtils.closeQuietly(statement);
    }
  }
  public boolean execSQL(String sql) throws SQLException {
    Statement statement  = null;
    try {
      statement = createStatement();
      /**/Debug.i(getClass(), sql);
      return statement.execute(sql);
    } finally {
      SQLUtils.closeQuietly(statement);
    }
  }
  
  /**
   * Ejecuta una sentencia que modifique las filas de la base de datos.
   * 
   * @param sql sentencia update o delete.
   * @param bindArgs valores de la sentencia: <code>nombre=?</code>.
   * @return el número de filas afectadas.
   * @throws SQLException 
   */
  public int executeUpdate(String sql, Object... bindArgs) throws SQLException {
    PreparedStatement ps = null;
    try {
      ps = compileStatement(sql);
      SQLUtils.prepareBind(ps, bindArgs);
      /**/Debug.i(getClass(), SQLUtils.concat(sql, "; ", Arrays.toString(bindArgs)));
      return ps.executeUpdate();
    } finally {
      SQLUtils.closeQuietly(ps);
    }
  }
  
  /**
   * Ejecuta sentencias insert y obtiene el id del registro insertado.
   * 
   * @param sql sentencia insert
   * @param bindArgs [opcional] parametros de la sentencia
   * 
   * @return el ID de la fila recién insertada, o -1 si se produjo un error
   * 
   * @throws SQLException 
   */
  public long insertAndGetId(String sql, Object... bindArgs) throws SQLException {
    PreparedStatement ps = null;
    try {
      ps = getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
      SQLUtils.prepareBind(ps, bindArgs);
      if (ps.executeUpdate() == 1) {
        /**/Debug.i(getClass(), SQLUtils.concat(sql, "; ", Arrays.toString(bindArgs)));
        ResultSet rs = null;
        try {
          //obtengo las ultimas llaves generadas
          rs = ps.getGeneratedKeys();
          // retorna la llave.
          return rs.next() ? rs.getLong(1) : 0;
        } finally {
          SQLUtils.closeQuietly(rs);
        }
      } else {
        return -1;
      }
    } finally {
      SQLUtils.closeQuietly(ps);
    }
  }
  
  
  /**
   * Inserta un registro en la base de datos.
   *
   * @param table nombre de la tabla donde se va a insertar la fila
   * @param initialValues mapa con los valores de columna iniciales para la fila.
   *      Las claves deben ser los nombres de las columnas 
   *      y los valores valores de la columna
   *
   * @return el ID de la fila recién insertada, o -1 si se produjo un error
   *
   * @throws SQLException
   */
  public long insert(String table, Map<String, Object> initialValues) throws SQLException {
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO ");
    sql.append(table);
    sql.append('(');

    int size = initialValues.size();
    Object[] bindArgs = new Object[size];
    int i = 0;
    for (String colName : initialValues.keySet()) {
      sql.append((i > 0) ? "," : "");
      sql.append(colName);
      bindArgs[i++] = initialValues.get(colName);
    }
    sql.append(')');
    sql.append(" VALUES (");
    for (i = 0; i < size; i++) {
      sql.append((i > 0) ? ",?" : "?");
    }
    sql.append(')');

    return insertAndGetId(sql.toString(), bindArgs);
  }
  
  /**
   * Actualiza una registro en la base de datos.
   *
   * @param tabla donde se va a actualizar la fila.
   * @param datos mapa contiene los valores de columna iniciales para la fila. 
   *      Las claves deben ser los nombres de las columnas y los valores valores 
   *      de la columna.
   * @param whereClause [opcional] cláusula WHERE para aplicar al actualizar.
   *      Pasar null actualizará todas las filas.
   * @param whereArgs [opcional] Puede incluirse en la cláusula WHERE, que
   *      será reemplazado por los valores de whereArgs. Los valores
   *      se enlazará como cadenas.
   *
   * @return el número de filas afectadas.
   *
   * @throws SQLException
   */
  public int update(String tabla, Map<String, Object> datos, String whereClause, Object... whereArgs)
  throws SQLException {
    StringBuilder sql = new StringBuilder();
    sql.append("UPDATE ");
    sql.append(tabla);
    sql.append(" SET ");

    int setValuesSize = datos.size();
    int bindArgsSize = (whereArgs == null) ? setValuesSize : (setValuesSize + whereArgs.length);
    Object[] bindArgs = new Object[bindArgsSize];
    int i = 0;
    for (String colName : datos.keySet()) {
      sql.append((i > 0) ? "," : "");
      sql.append(colName);
      bindArgs[i++] = datos.get(colName);
      sql.append("=?");
    }

    if (whereArgs != null) {
      for (i = setValuesSize; i < bindArgsSize; i++) {
        bindArgs[i] = whereArgs[i - setValuesSize];
      }
    }
    if (whereClause != null && !whereClause.isEmpty()) {
      sql.append(" WHERE ");
      sql.append(whereClause);
    }

    return executeUpdate(sql.toString(), bindArgs);
  }
  
  /**
   * Elimina un registro de la base de datos.
   * 
   * @param tabla donde se eliminara
   * @param whereClause [opcional] cláusula WHERE para aplicar la eliminación.
   *      Pasar null elimina todas las filas.
   * @param whereArgs [opcional] Puede incluirse en la cláusula WHERE, que
   *      será reemplazado por los valores de whereArgs. Los valores
   *      se enlazará como cadenas.
   * 
   * @return el número de filas afectadas.
   * 
   * @throws SQLException 
   */
  public int delete(String tabla, String whereClause, Object... whereArgs) 
  throws SQLException {
    String sql = "DELETE FROM " + tabla;
    if (whereClause != null && !whereClause.isEmpty()) {
      sql += " WHERE " + whereClause;
    }
    return executeUpdate(sql, whereArgs);
  }

  /**
   * Obtiene el numero de filas.
   *
   * @param tabla donde se buscaran las existencias
   * @param whereClause condicion
   * @param whereArgs [opcional] parametros del whereClause
   *
   * @return numero de existencia
   *
   * @throws SQLException
   */
  public long count(String tabla, String whereClause, Object... whereArgs) 
  throws SQLException {
    String sql = "SELECT COUNT(*) AS COUNT FROM " + tabla;
    if (whereClause != null && !whereClause.isEmpty()) {
      sql += " WHERE " + whereClause;
    }
    ResultSet rs = null;
    try {
      rs = query(sql, whereArgs);
      return rs.next() ? rs.getLong("COUNT") : -1;
    } finally {
      SQLUtils.closeQuietly(rs);
    }
  }
  
  public ResultSet select(boolean distinct, String table, String[] columns,
            String whereClause, Object[] whereArgs, String groupBy,
            String having, String orderBy, String limit) throws SQLException {
    if (SQLUtils.isEmpty(groupBy) && !SQLUtils.isEmpty(having)) {
      throw new IllegalArgumentException(
              "HAVING clauses are only permitted when using a groupBy clause");
    }

    StringBuilder query = new StringBuilder(120);

    query.append("SELECT ");
    if (distinct) {
      query.append("DISTINCT ");
    }
    if (columns != null && columns.length != 0) {
      SQLUtils.appendColumns(query, columns);
    } else {
      query.append("* ");
    }
    query.append("FROM ");
    query.append(table);
    SQLUtils.appendClause(query, " WHERE ", whereClause);
    SQLUtils.appendClause(query, " GROUP BY ", groupBy);
    SQLUtils.appendClause(query, " HAVING ", having);
    SQLUtils.appendClause(query, " ORDER BY ", orderBy);
    SQLUtils.appendClause(query, " LIMIT ", limit);

    return query(query.toString(), whereArgs);
  }
  
  /** Obtiene un constructor de quierys. */
  public QueryBuilder table(String table) {
    return new QueryBuilder(this).from(table);
  }

}
