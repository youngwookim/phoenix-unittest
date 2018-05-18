package com.exmaple.phoenix;

import static org.junit.Assert.assertEquals;
import java.io.StringReader;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.CSVCommonsLoader;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/*
 * @see http://mail-archives.apache.org/mod_mbox/phoenix-user/201607.mbox/%3CCAEF26GffG-kR=
 * dac3vxhNbZ1Rja9QJc8qF+gb2XK0gy7aScpkw@mail.gmail.com%3E
 * 
 * @see
 * https://github.com/apache/phoenix/blob/master/phoenix-core/src/it/java/org/apache/phoenix/end2end
 * /CSVCommonsLoaderIT.java
 */
public class PhoenixUnitTest extends BaseHBaseManagedTimeIT {
  private static final String STOCK_TABLE = "STOCK_SYMBOL";
  private static final String STOCK_CSV_VALUES = "AAPL,APPLE Inc.\n" + "CRM,SALESFORCE\n"
      + "GOOG,Google\n" + "HOG,Harlet-Davidson Inc.\n" + "HPQ,Hewlett Packard\n" + "INTC,Intel\n"
      + "MSFT,Microsoft\n" + "WAG,Walgreens\n" + "WMT,Walmart\n";
  private static final String[] STOCK_COLUMNS = new String[] {"SYMBOL", "COMPANY"};
  private static final String STOCK_CSV_VALUES_WITH_HEADER =
      STOCK_COLUMNS[0] + "," + STOCK_COLUMNS[1] + "\n" + STOCK_CSV_VALUES;

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void phoenixTest() throws Exception {
    PhoenixConnection conn = null;

    try {
      String statements = "CREATE TABLE IF NOT EXISTS " + STOCK_TABLE
          + "(SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);";
      conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
      PhoenixRuntime.executeStatements(conn, new StringReader(statements), null);

      CSVCommonsLoader csvUtil =
          new CSVCommonsLoader(conn, STOCK_TABLE, Collections.<String>emptyList(), true);
      csvUtil.upsert(new StringReader(STOCK_CSV_VALUES_WITH_HEADER));

      PreparedStatement statement = conn.prepareStatement("SELECT COUNT(*) FROM " + STOCK_TABLE);
      ResultSet phoenixResultSet = statement.executeQuery();

      while (phoenixResultSet.next()) {
        assertEquals(9, phoenixResultSet.getInt(1));
      }
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }

}
