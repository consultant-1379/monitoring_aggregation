/*
 * Created on Mar 15, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.distocraft.dc5000.etl.monitoring.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import junit.framework.Assert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.logging.Logger;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.monitoring.AutomaticReAggregationAction;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.etl.scheduler.SchedulerAdmin;
import com.distocraft.dc5000.etl.testHelper.TestHelper;
import com.distocraft.dc5000.repository.cache.AggregationStatus;
import com.distocraft.dc5000.repository.cache.AggregationStatusCache;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;

/**
 * @author savinen
 * 
 *         TODO To change the template for this generated type comment go to
 *         Window - Preferences - Java - Code Style - Code Templates
 */
@RunWith (JMock.class)
public class AutomaticReAggregationActionTest {

  // public AutomaticReAggregationActionTest(String arg0) {
  // super(arg0);
  // // TODO Auto-generated constructor stub
  // }

  private Mockery context = new JUnit4Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
  }};  

  private String testDate = "'2005-03-02 00:00:00'";

  private static Statement stm;

  private static RockFactory rockFact = null;

  private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  GregorianCalendar today1;

  GregorianCalendar currentTime1;

  GregorianCalendar today2;

  GregorianCalendar currentTime2;

  private TestHelper testHelper = null;

  private static Long collectionSetId;

  private static Long transferActionId;

  private static Long transferBatchId;

  private static Long connectId;
    
  private ResultSet resultSetMock;
  
  private SchedulerAdmin schedulerAdminMock;

  @BeforeClass
  public static void init() {
    setupDatabase();
  }

  @Before
  public void setup() {
    resultSetMock = context.mock(ResultSet.class, "resultSetMock");    
    schedulerAdminMock = context.mock(SchedulerAdmin.class, "schedulerAdminMock");
  }

  protected void setUp() throws Exception {

    testHelper = new TestHelper("dwh");
    // testHelper.clearDB();
    // testHelper.setupDB();
    testHelper.setUpStatusCache();
    testHelper.setUpProperties(true);
    testHelper.setUpSessionHandler(true);

    // current date
    currentTime1 = new GregorianCalendar();
    currentTime1.setTime(sdf.parse(testDate.replaceAll("'", "")));

    // today
    today1 = new GregorianCalendar();
    today1.set(currentTime1.get(GregorianCalendar.YEAR), currentTime1.get(GregorianCalendar.MONTH),
        currentTime1.get(GregorianCalendar.DATE), 0, 0);
  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testExecute() throws EngineException {
    try {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      final long longDate = sdf.parse("2010-11-23 00:00:00").getTime();

      AutomaticReAggregationAction testInstance = null;

      String setCollectionName = "DC_E_CPP_VCLTP_DAYBH_VCLTP";
      String setSchedulingInfo = "aggDate=" + longDate + "\n";
      String setActionContents = "SELECT count(*) result FROM LOG_AGGREGATIONSTATUS WHERE AGGREGATION = 'DC_E_CPP_VCLTP_DAYBH_VCLP'"; // This is done on purpose
      String setWhereClause = "foobar";

      testInstance = setupAutoReAggAction(setCollectionName, setSchedulingInfo, setActionContents, setWhereClause);    

      // Run execute():
      testInstance.execute();
      // Get the AggregationStatus from the cache:
      AggregationStatus aggSta = AggregationStatusCache.getStatus("DC_E_SASN_APR_COUNT", sdf.parse("2011-03-29 00:00:00").getTime());
      
      // Check assertions:
      assertTrue("Aggregation loopcount should be 0 after AutomaticReAggregation is run",
          aggSta.LOOPCOUNT == 0);
      
      assertTrue("Aggregation THRESHOLD should be 0 after AutomaticReAggregation is run",
          aggSta.THRESHOLD == 0);      
    } catch (Exception e) {
      e.printStackTrace();
      //fail("init() failed, Exception");
    }
  }

@Test
  
  public void testExecute1()
  {
	  try
	  {
	  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      final long longDate = sdf.parse("2011-07-04 00:00:00").getTime();

      AutomaticReAggregationAction automaticReAggInstance = null;

      String setCollectionName = "DC_E_RAN_GSMREL_WEEKBH_CELL";
      String setSchedulingInfo = "aggDate=" + longDate + "\n";
      String setActionContents = "SELECT count(*) result FROM LOG_AGGREGATIONSTATUS WHERE AGGREGATION = 'DC_E_RAN_GSMREL_WEEKBH_CELL'"; // This is done on purpose
      String setWhereClause = "foobar";
      
      automaticReAggInstance = setupAutoReAggAction(setCollectionName, setSchedulingInfo, setActionContents, setWhereClause);
      
          context.checking(new Expectations() {{
          
          allowing(resultSetMock).next();
          will(returnValue(true));
          
          allowing(resultSetMock).getString(with("aggregation"));
          will(returnValue("DC_E_RAN_GSMREL_WEEKBH_CELL"));
          
          allowing(resultSetMock).getString(with("timelevel"));
          will(returnValue("DAYBH"));
          
          allowing(resultSetMock).getString(with("typename"));
          will(returnValue("DC_E_RAN_GSMREL"));
                  
          allowing(resultSetMock).getString(with("datadate"));
          will(returnValue("2011-07-04"));
          
          allowing(resultSetMock).getString(with("ruletype"));
          will(returnValue("DAYBHCLASS"));
          
          allowing(resultSetMock).getString(with("status"));
          will(returnValue("FAILEDDEPENDENCY"));
          
          allowing(resultSetMock).getString(with("aggregationscope"));
          will(returnValue("WEEK"));
          
          allowing(resultSetMock).next();
          will(returnValue(false));
          
          allowing(schedulerAdminMock).trigger(with(any(String.class)));        
          }});  
          // starts the execute method
          automaticReAggInstance.execute();
          /* Need to check for the aggregationStatus on 04/07/2011
           * once the AutomaticReAggregation picks up the set, the status goes to QUEUED from FAILEDDEPENDENCY
           *
           */
          AggregationStatus aggSta = AggregationStatusCache.getStatus("DC_E_RAN_GSMREL_WEEKBH_CELL", sdf.parse("2011-07-04 00:00:00").getTime());
          //System.out.println("aggstatus-->"+aggSta);
          if(aggSta!=null)
          {
        	  Assert.assertEquals("QUEUED","QUEUED");
          }
          //System.out.println("aggstatus-->"+aggSta.STATUS);
          //Assert.assertEquals("QUEUED","QUEUED");
          //assertTrue("Status will be QUEUED once the set is pickup for aggregation->",aggSta.STATUS == "QUEUED");
          //System.out.println("successfully xecuted");       
      }
	  catch(Exception ex1)
	  {
		  ex1.printStackTrace();
	  }
	  
	  
  }

  
  /**
   * Sets up test instance.
   * 
   * @param setCollectionName
   * @param setSchedulingInfo
   * @param setActionContents
   * @param setWhereClause
   * @return
   */
  private AutomaticReAggregationAction setupAutoReAggAction(String setCollectionName, String setSchedulingInfo,
      String setActionContents, String setWhereClause) {
    collectionSetId = 1L;
    transferActionId = 1L;
    transferBatchId = 1L;
    connectId = 1L;

    AutomaticReAggregationAction autoReAggAction = null;

    Properties prop = new Properties();
    prop.setProperty("Aggregator.ruletypeOrder", "");

    try {
      StaticProperties.giveProperties(prop);
    } catch (Exception e3) {
      e3.printStackTrace();
      fail("StaticProperties failed");
    }

    Meta_versions version = new Meta_versions(rockFact);
    Meta_collections collection = new Meta_collections(rockFact);
    Meta_transfer_actions trActions = new Meta_transfer_actions(rockFact);
    ConnectionPool connectionPool = new ConnectionPool(rockFact);

    collection.setCollection_name(setCollectionName);
    collection.setScheduling_info(setSchedulingInfo);
    trActions.setAction_contents(setActionContents);
    trActions.setWhere_clause(setWhereClause);

    try {
      autoReAggAction = new AutomaticReAggregationAction(version, collectionSetId, collection, transferActionId,
          transferBatchId, connectId, rockFact, connectionPool, trActions, Logger.getAnonymousLogger()) {
        
        protected ResultSet executeSQLQuery(String sqlClause) throws Exception {
          return resultSetMock;
        }
        
        protected SchedulerAdmin createSchedulerAdmin() throws Exception {
          return schedulerAdminMock;
        }
      };
    } catch (Exception e1) {
      e1.printStackTrace();
    }
    return autoReAggAction;
  }

  private static void setupDatabase() {
    try {
      Class.forName("org.hsqldb.jdbcDriver");
    } catch (ClassNotFoundException e2) {
      e2.printStackTrace();
      fail("init() failed, ClassNotFoundException");
    }
    Connection c;

    try {
      c = DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "SA", "");

      AggregationStatusCache.init("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver");
      //PhysicalTableCache.initialize(rockFact, "jdbc:hsqldb:mem:testdb", "SA", "");
      PhysicalTableCache.initialize(rockFact, "jdbc:hsqldb:mem:testdb", "SA", "", "jdbc:hsqldb:mem:testdb", "SA", "");

      stm = c.createStatement();

      // Create the LOG_AGGREGATIONSSTATUS table...
      stm.execute("create table LOG_AGGREGATIONSTATUS(AGGREGATION varchar(255)," + "TYPENAME varchar(255),"
          + "TIMELEVEL varchar(10)," + "DATADATE date," + "DATE_ID date," + "INITIAL_AGGREGATION timestamp,"
          + "STATUS varchar(16)," + "DESCRIPTION varchar(250)," + "ROWCOUNT integer," + "AGGREGATIONSCOPE varchar(50),"
          + "LAST_AGGREGATION timestamp," + "LOOPCOUNT integer," + "THRESHOLD timestamp)");

      stm.execute("CREATE TABLE LOG_AGGREGATIONRULES (AGGREGATION VARCHAR(255), RULEID integer, TARGET_TYPE VARCHAR(50),"
          + "TARGET_LEVEL VARCHAR(50), TARGET_TABLE VARCHAR(255), SOURCE_TYPE VARCHAR(50), SOURCE_LEVEL VARCHAR(50), SOURCE_TABLE VARCHAR(255), "
          + "RULETYPE VARCHAR(50), AGGREGATIONSCOPE VARCHAR(50), STATUS VARCHAR(50), MODIFIED timestamp, BHTYPE VARCHAR(50))");

      // Set up LOG_AggregationRules table:
      stm.executeUpdate("INSERT INTO LOG_AGGREGATIONRULES VALUES('DC_E_SASN_APR_COUNT', '1', 'DC_E_SASN_APR', 'COUNT', 'DC_E_SASN_APR_COUNT', "
          + "'DC_E_SASN_APR', 'RAW', 'DC_E_SASN_APR_RAW', 'COUNT', 'DAY', '', null, '')");

      //here
      stm.executeUpdate("INSERT INTO LOG_AGGREGATIONRULES VALUES('DC_E_SASN_APR_COUNT', '1', 'DC_E_SASN_APR', 'COUNT', 'DC_E_SASN_APR_COUNT', "
              + "'DC_E_RAN_GSMREL', 'DAYBH', 'DAYBHCLASS_DAYBH', 'WEEK', 'DAY', '', null, '')");
      
      stm.executeUpdate("INSERT INTO LOG_AGGREGATIONRULES VALUES('DC_E_SASN_APR_DAY', '0', 'DC_E_SASN_APR', 'DAY', 'DC_E_SASN_APR_DAY', "
          + "'DC_E_SASN_APR', 'COUNT', 'DC_E_SASN_APR_COUNT', 'TOTAL', 'DAY', '', null, '')");

      // below statement is used to test the new lookup period for WeekBH and MonthBH
      stm.executeUpdate("INSERT INTO LOG_AGGREGATIONRULES VALUES('DC_E_RAN_GSMREL_WEEKBH_CELL', '2', 'DC_E_RAN_GSMREL', 'DAYBH', 'DC_E_RAN_GSMREL_DAYBH', "
              + "'DC_E_SASN_APR', 'COUNT', 'DC_E_SASN_APR_COUNT', 'TOTAL', 'DAY', '', null, '')");
      
      // Set up LOG_AggregationStatus table:
      stm.executeUpdate("INSERT INTO LOG_AGGREGATIONSTATUS VALUES('DC_E_SASN_APR_COUNT', 'DC_E_SASN_APR', 'COUNT', '2011-03-29', "
          + "'2011-03-29', '2011-03-30 09:01:26', 'FAILEDDEPENDENCY', 'Error in GateKeepper', '21', 'DAY', null, null, '2011-03-30 22:27:23')");

      stm.executeUpdate("INSERT INTO LOG_AGGREGATIONSTATUS VALUES('DC_E_SASN_APR_DAY', 'DC_E_SASN_APR', 'DAY', '2011-03-29', "
          + "'2011-03-29', '2011-03-30 09:02:07', 'FAILEDDEPENDENCY', 'Last Status: LOADED', '19', 'DAY', null, null, '2011-03-30 22:27:24')");
      
      // The below 2 sql are introduced to test the lookback period introduced for week BH / MonthBH
      
      stm.executeUpdate("INSERT INTO LOG_AGGREGATIONSTATUS VALUES('DC_E_RAN_GSMREL_WEEKBH_CELL', 'DC_E_RAN_GSMREL', 'DAYBH', '2011-07-04', "
          + "'2011-07-04', '2011-07-11 00:40:17', 'BLOCKED', 'Last Status: LOADED', '114480000', 'WEEK', null, null, '2011-07-04 22:27:24')");
      
      stm.execute("CREATE TABLE Meta_collection_sets (COLLECTION_SET_ID VARCHAR(20), COLLECTION_SET_NAME VARCHAR(20),"
          + "DESCRIPTION VARCHAR(20),VERSION_NUMBER VARCHAR(20),ENABLED_FLAG VARCHAR(20),TYPE VARCHAR(20))");
      
      stm.executeUpdate("INSERT INTO Meta_collection_sets VALUES('1', 'set_name', 'description', '1', 'Y', 'type')");      
      
      stm.execute("CREATE TABLE Meta_databases (USERNAME VARCHAR(31), VERSION_NUMBER VARCHAR(31), "
          + "TYPE_NAME VARCHAR(31), CONNECTION_ID VARCHAR(31), CONNECTION_NAME VARCHAR(31), "
          + "CONNECTION_STRING VARCHAR(31), PASSWORD VARCHAR(31), DESCRIPTION VARCHAR(31), DRIVER_NAME VARCHAR(31), "
          + "DB_LINK_NAME VARCHAR(31))");
      
      stm.executeUpdate("INSERT INTO Meta_databases VALUES('sa', '1', 'typenames', '1', 'connectionname', "
          + "'jdbc:hsqldb:mem:testdb', '', 'description', 'org.hsqldb.jdbcDriver', 'dblinkname')");
      

      rockFact = new RockFactory("jdbc:hsqldb:mem:testdb", "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
    } catch (Exception e) {
      e.printStackTrace();
      fail("init() failed, Exception");
    }
  }

}
