package javax.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.util.Debug;

public class SQLDataSource implements AutoCloseable {
  /**
   * Driver de coneccion. 
   * MySQL      : com.mysql.jdbc.Driver 
   * Oracle     : oracle.jdbc.driver.OracleDriver 
   * PostgreSQL : org.postgresql.Driver
   * SQLServer  : com.microsoft.sqlserver.jdbc.SQLServerDriver
   */
  String driverClassName = "com.mysql.jdbc.Driver";
  String url;
  String username;
  String password;
  
  private SQLDatabase mDatabase;
  private boolean mIsInitializing;
   
  public SQLDatabase getDatabase() throws SQLException {
    synchronized (this) {
      return getDatabaseLocked();
    }
  }
  
  /**
   * Connect to a sample database
   *
   * @return
   * @throws java.sql.SQLException
   */
  private SQLDatabase getDatabaseLocked() throws SQLException {
    if (mDatabase != null) {
      if (mDatabase.isClosed()) {
        // ¡Maldición! El usuario cerró la base de datos llamando a mDatabase.close ().
        mDatabase = null;
      } else {
        // La base de datos ya está abierta para los negocios.
        return mDatabase;
      }
    }

    if (mIsInitializing) {
      throw new IllegalStateException("getDatabase called recursively");
    }

    SQLDatabase db = mDatabase;
    try {
      mIsInitializing = true;

      if (db == null) {
        db = newDatabase();
        db.conn = connect();
      }
     
      mDatabase = db;
      return db;
    } finally {
      mIsInitializing = Boolean.FALSE;
      if (db != null && db != mDatabase) {
        db.close();
      }
    }
  }
  
  public SQLDatabase newDatabase() {
    //Connection conn = connect();
    return new SQLDatabase(this);
  }
  
  
  /**
   * Establece la coneccion con la base de datos.
   *
   * @return la coneccion
   *
   * @throws SQLException
   */
  public Connection connect() throws SQLException {
    try {
      Class.forName(driverClassName);
    } catch (ClassNotFoundException ex) {
      throw new SQLException(ex.getMessage(), ex);
    }
    Connection conn = DriverManager.getConnection(url, username, password);
    Debug.i(getClass(), "OPEN ", url, "; username=", username);
    return conn;
  }
  
  /**
   * Close any open database object.
   */
  @Override public synchronized void close() {
    try {
      if (mIsInitializing) throw new IllegalStateException("Closed during initialization");

      if (mDatabase != null && !mDatabase.isClosed()) {
        mDatabase.close();
        mDatabase = null;
      }
    } catch (SQLException ignore) {
      // Empty
    }
  }
  
  public String getDriverClassName() {
    return driverClassName;
  }
  public SQLDataSource setDriverClassName(String driverClassName) {
    this.driverClassName = driverClassName;
    return this;
  }

  public String getUrl() {
    return url;
  }
  public SQLDataSource setUrl(String url) {
    this.url = url;
    return this;
  }

  public String getUsername() {
    return username;
  }
  public SQLDataSource setUsername(String username) {
    this.username = username;
    return this;
  }

  public String getPassword() {
    return password;
  }
  public SQLDataSource setPassword(String password) {
    this.password = password;
    return this;
  }

  public SQLDataSource setDebuggable(boolean b) {
    Debug.setDebuggable(b);
    return this;
  }
}
