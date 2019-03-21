package javax.util;

public final class Debug {

  private static boolean debuggable;
  
  public static boolean isDebuggable() {
    return debuggable;
  }

  public static void setDebuggable(boolean debuggable) {
    Debug.debuggable = debuggable;
  }
  
  public static void i(String tag, Object... msg) {
    if (isDebuggable()) {
      System.out.printf("[%s]: %s\n", tag, SQLUtils.concat(msg));
    }
  }
  public static void i(Class<?> tag, Object... msg) {
    i(tag.getSimpleName(), msg);
  }
  
  public static void i(String tag, String msg, Throwable tr) {
    if (isDebuggable()) {
      System.out.printf("[%s]: %s => %s\n", tag, 
              tr.getClass().getCanonicalName(), msg);
    }
  }
  public static void i(Class<?> tag, String msg, Throwable tr) {
    i(tag.getSimpleName(), msg, tr);
  }

  public static void w(String tag, Object... msg) {
    if (isDebuggable()) {
      System.out.printf("[%s]: %s\n", tag, SQLUtils.concat(msg));
    }
  }
  public static void w(Class<?> tag, Object... msg) {
    i(tag.getSimpleName(), msg);
  }
  
  public static void w(String tag, String msg, Throwable tr) {
    if (isDebuggable()) {
      System.out.printf("[%s]: %s => %s\n", tag, 
              tr.getClass().getCanonicalName(), msg);
    }
  }
  public static void w(Class<?> tag, String msg, Throwable tr) {
    i(tag.getSimpleName(), msg, tr);
  }
  
  public static void e(String tag, String msg) {
    if (isDebuggable()) {
      System.err.printf("[%s]: %s\n", tag, msg);
    }
  }
  public static void e(Class<?> tag, String msg) {
    e(tag.getSimpleName(), msg);
  }
  
  public static void e(String tag, String msg, Throwable tr) {
    if (isDebuggable()) {
      System.err.printf("[%s]: %s => %s\n", tag, 
              tr.getClass().getCanonicalName(), msg);
    }
  }
  public static void e(Class<?> tag, String msg, Throwable tr) {
    e(tag.getSimpleName(), msg, tr);
  }

}
