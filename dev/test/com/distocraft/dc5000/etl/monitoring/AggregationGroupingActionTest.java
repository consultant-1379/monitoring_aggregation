package com.distocraft.dc5000.etl.monitoring;

/*import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.ericsson.eniq.common.testutilities.DatabaseTestUtils;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.velocity.VelocityContext;
import org.jmock.integration.junit4.JMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collection_setsFactory;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_collectionsFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actionsFactory;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.etl.rock.Meta_versionsFactory;
import com.distocraft.dc5000.repository.dwhrep.Busyhour;
import com.distocraft.dc5000.repository.dwhrep.BusyhourFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
import com.distocraft.dc5000.repository.dwhrep.Measurementcounter;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;
*/


//@RunWith(JMock.class)
public class AggregationGroupingActionTest extends BaseMock {
	/*
	 * private RockFactory jUnitTestDB = null; private final String junitDbUrl =
	 * "jdbc:hsqldb:mem:test_db_abc_123"; private String testTechPackName =
	 * "DC_E_MGW"; private String testMeasType = "ATMPORT"; private String
	 * testBhTargetType = testTechPackName + "_" + testMeasType; private String
	 * realPartition = testBhTargetType + "_DAYBH_01"; private String
	 * eoinsTestString = ""; private static final String DATE_FORMAT_DEF =
	 * "yyyy-MM-dd"; final SimpleDateFormat dateFormatter = new
	 * SimpleDateFormat(DATE_FORMAT_DEF, Locale.getDefault());
	 * 
	 * @Test public void testgetKeyNameWithDataType(){ final String mTableId =
	 * "DC_E_MGW:((7)):DC_E_MGW_ATMPORT:DAYBH"; try { final Field dwhrepRockField =
	 * AggregationGroupingAction.class.getDeclaredField("dwhrepRock");
	 * dwhrepRockField.setAccessible(true);
	 * 
	 * TestIntegAggregationGroupingAction aggregationGroupingAction = new
	 * TestIntegAggregationGroupingAction();
	 * dwhrepRockField.set(aggregationGroupingAction, jUnitTestDB);
	 * 
	 * Map<String, String> kvPair =
	 * aggregationGroupingAction.getKeyNameAndDataType(mTableId); Map<String,
	 * String> expected = new HashMap<String, String>();
	 * 
	 * expected.put("NESW", "varchar"); expected.put("MGW", "varchar");
	 * expected.put("NEDN", "varchar"); expected.put("OSS_ID", "varchar");
	 * expected.put("AtmPort", "varchar"); expected.put("NEUN", "varchar");
	 * expected.put("SN", "varchar"); expected.put("TransportNetwork", "varchar");
	 * expected.put("MOID", "varchar"); expected.put("userLabel", "varchar");
	 * assertEquals(expected, kvPair); } catch (Exception e) {
	 * fail("The test: testgetKeyNameWithDataType has failed with the Exception\n "
	 * +e.getMessage()); } }
	 * 
	 * @Test public void testTimeCalculationsWithISNULLCheck(){ String table =
	 * "table"; String tt = "tt"; try { TestIntegAggregationGroupingAction
	 * aggregationGroupingAction = new TestIntegAggregationGroupingAction();
	 * 
	 * Map<String, String> columns = new HashMap<String, String>();
	 * columns.put("name1", "varchar"); columns.put("name2", "int");
	 * columns.put("name3", "numberic"); columns.put("name4", "date");
	 * 
	 * List<String> expected = new ArrayList<String>();
	 * expected.add("ISNULL(table.name3,0)=ISNULL(tt.name3,0)"); expected.
	 * add("ISNULL(table.name4,0001-01-01 00:00:00)=ISNULL(tt.name4,0001-01-01 00:00:00)"
	 * ); expected.add("ISNULL(table.name1,'')=ISNULL(tt.name1,'')");
	 * expected.add("ISNULL(table.name2,0)=ISNULL(tt.name2,0)");
	 * 
	 * List<String> actual =
	 * aggregationGroupingAction.timeCalculationsWithNullCheck(columns, table, tt);
	 * assertEquals(expected, actual); } catch (Exception e) { // TODO
	 * Auto-generated catch block e.printStackTrace(); }
	 * 
	 * }
	 * 
	 * @Test public void testNoGrouping(){ String todaysDate =
	 * dateFormatter.format(getDefaultAggregationDate().getTime());
	 * 
	 * String expected =
	 * "DELETE FROM DC_E_MGW_ATMPORT_DAYBH_01 WHERE (BHTYPE = 'ATMPORT_PP0' or BHTYPE = 'ATMPORT_PP1' or BHTYPE = 'ATMPORT_PP2') "
	 * + "AND DATE_ID='"+todaysDate+"'; " +
	 * "INSERT INTO DC_E_MGW_ATMPORT_DAYBH_01 (OSS_ID, SN, NEUN, NEDN, NESW, MGW, MOID, TransportNetwork, "
	 * +
	 * "AtmPort, userLabel , DATE_ID, YEAR_ID, MONTH_ID, DAY_ID, MIN_ID, BHTYPE, BUSYHOUR, BHCLASS, TIMELEVEL, "
	 * +
	 * "SESSION_ID, BATCH_ID, PERIOD_DURATION, ROWSTATUS, DC_RELEASE, DC_SOURCE, DC_TIMEZONE, DC_SUSPECTFLAG , "
	 * + "pmReceivedAtmCells, pmSecondsWithUnexp, pmTransmittedAtmCells) " +
	 * "SELECT DC_E_MGW_ATMPORT_DAYBH_CALC.OSS_ID, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.SN, " + "DC_E_MGW_ATMPORT_DAYBH_CALC.NEUN, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.NEDN, " + "DC_E_MGW_ATMPORT_DAYBH_CALC.NESW, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.MGW, " + "DC_E_MGW_ATMPORT_DAYBH_CALC.MOID, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.TransportNetwork, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.AtmPort, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.userLabel , " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.DATE_ID, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.YEAR_ID, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.MONTH_ID, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.DAY_ID, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.MIN_ID, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.BHTYPE, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.BUSYHOUR, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.BHCLASS, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.TIMELEVEL, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.SESSION_ID, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.BATCH_ID, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.PERIOD_DURATION, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.ROWSTATUS, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.DC_RELEASE, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.DC_SOURCE, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.DC_TIMEZONE, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.DC_SUSPECTFLAG , " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.pmReceivedAtmCells, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.pmSecondsWithUnexp, " +
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC.pmTransmittedAtmCells " +
	 * "FROM DC_E_MGW_ATMPORT_DAYBH_CALC ,( SELECT DISTINCT OSS_ID, SN, NEUN, NEDN, NESW, MGW, "
	 * +
	 * "MOID, TransportNetwork, AtmPort, userLabel, BHTYPE, CAST(DATE_ID || ' ' || BUSYHOUR || ':' || OFFSET AS TIMESTAMP) "
	 * +
	 * "AS start_timestamp, DATEADD(MINUTE, 60, DATE_ID || ' ' || BUSYHOUR || ':' || OFFSET) "
	 * + "AS end_timestamp FROM DC_E_MGW_ATMPORT_DAYBH_CALC " +
	 * "WHERE (BHTYPE = 'ATMPORT_PP0' or BHTYPE = 'ATMPORT_PP1' or BHTYPE = 'ATMPORT_PP2') "
	 * + "AND DC_E_MGW_ATMPORT_DAYBH_CALC.DATE_ID='"+todaysDate+"') as tt " +
	 * "WHERE (DC_E_MGW_ATMPORT_DAYBH_CALC.BHTYPE = 'ATMPORT_PP0' " +
	 * "or DC_E_MGW_ATMPORT_DAYBH_CALC.BHTYPE = 'ATMPORT_PP1' " +
	 * "or DC_E_MGW_ATMPORT_DAYBH_CALC.BHTYPE = 'ATMPORT_PP2') " +
	 * "AND CAST(DC_E_MGW_ATMPORT_DAYBH_CALC.DATE_ID || ' ' || DC_E_MGW_ATMPORT_DAYBH_CALC.BUSYHOUR || ':' || DC_E_MGW_ATMPORT_DAYBH_CALC.MIN_ID AS TIMESTAMP)"
	 * +
	 * " >= start_timestamp AND CAST(DC_E_MGW_ATMPORT_DAYBH_CALC.DATE_ID || ' ' || DC_E_MGW_ATMPORT_DAYBH_CALC.BUSYHOUR || ':' || DC_E_MGW_ATMPORT_DAYBH_CALC.MIN_ID AS TIMESTAMP) "
	 * + "< end_timestamp "+ "AND DC_E_MGW_ATMPORT_DAYBH_CALC.BHTYPE = tt.BHTYPE "+
	 * "AND ISNULL(DC_E_MGW_ATMPORT_DAYBH_CALC.NESW,'')=ISNULL(tt.NESW,'') "+
	 * "AND ISNULL(DC_E_MGW_ATMPORT_DAYBH_CALC.MGW,'')=ISNULL(tt.MGW,'') "+
	 * "AND ISNULL(DC_E_MGW_ATMPORT_DAYBH_CALC.NEDN,'')=ISNULL(tt.NEDN,'') "+
	 * "AND ISNULL(DC_E_MGW_ATMPORT_DAYBH_CALC.OSS_ID,'')=ISNULL(tt.OSS_ID,'') "+
	 * "AND ISNULL(DC_E_MGW_ATMPORT_DAYBH_CALC.AtmPort,'')=ISNULL(tt.AtmPort,'') "+
	 * "AND ISNULL(DC_E_MGW_ATMPORT_DAYBH_CALC.NEUN,'')=ISNULL(tt.NEUN,'') "+
	 * "AND ISNULL(DC_E_MGW_ATMPORT_DAYBH_CALC.SN,'')=ISNULL(tt.SN,'') "+
	 * "AND ISNULL(DC_E_MGW_ATMPORT_DAYBH_CALC.TransportNetwork,'')=ISNULL(tt.TransportNetwork,'') "
	 * + "AND ISNULL(DC_E_MGW_ATMPORT_DAYBH_CALC.MOID,'')=ISNULL(tt.MOID,'') "+
	 * "AND ISNULL(DC_E_MGW_ATMPORT_DAYBH_CALC.userLabel,'')=ISNULL(tt.userLabel,'')   "
	 * ;
	 * 
	 * 
	 * 
	 * try { final Field dwhrepRockField =
	 * AggregationGroupingAction.class.getDeclaredField("dwhrepRock");
	 * dwhrepRockField.setAccessible(true);
	 * 
	 * final Method noGroupingMethod =
	 * AggregationGroupingAction.class.getDeclaredMethod("noGrouping", List.class,
	 * String.class, List.class, String.class, List.class, String.class);
	 * noGroupingMethod.setAccessible(true);
	 * 
	 * final Method getPartitionColumnListMethod =
	 * AggregationGroupingAction.class.getDeclaredMethod("getPartitionColumnList",
	 * String.class); getPartitionColumnListMethod.setAccessible(true);
	 * 
	 * final String techPackName = "DC_E_MGW"; final String measType = "ATMPORT";
	 * String storageId = "DC_E_MGW_ATMPORT:DAYBH";
	 * 
	 * //default group type is Time, change it to None final Busyhour where = new
	 * Busyhour(jUnitTestDB); where.setBhlevel(techPackName + "_"+measType+"BH");
	 * final BusyhourFactory fac = new BusyhourFactory(jUnitTestDB, where); final
	 * List<Busyhour> toChange = fac.get(); for(Busyhour bh : toChange){
	 * bh.setGrouping("None"); bh.updateDB(); }
	 * 
	 * final Long collectionSetId = getCollectionSetId(jUnitTestDB, techPackName);
	 * final Long collectionId = getCollectionId(jUnitTestDB, collectionSetId,
	 * techPackName, measType); final Long transferBatchId = -1L; final Long
	 * connectId = 2L; final Meta_versions version = getMetaVersions(jUnitTestDB);
	 * final Meta_collections collections = getMetaCollections(jUnitTestDB,
	 * collectionSetId, collectionId); final String schedulingDetails = "";
	 * 
	 * final Meta_transfer_actions actions = getMetaTransferActions(jUnitTestDB,
	 * collectionSetId, collectionId); final Long transferId =
	 * actions.getTransfer_action_id();
	 * 
	 * final ConnectionPool cp = new UnitConnectionPool(jUnitTestDB); final String
	 * actionDetails = actions.getAction_contents();
	 * System.out.println(actionDetails); List<String> bhTypes = new
	 * ArrayList<String>(); bhTypes.add("ATMPORT_PP0"); bhTypes.add("ATMPORT_PP1");
	 * bhTypes.add("ATMPORT_PP2"); TestIntegAggregationGroupingAction aga = new
	 * TestIntegAggregationGroupingAction(version, collectionSetId, collections,
	 * transferId, transferBatchId, connectId, jUnitTestDB, cp, actions, "",
	 * Logger.getAnonymousLogger());
	 * 
	 * dwhrepRockField.set(aga, jUnitTestDB);
	 * aga.getActionProperties(actionDetails);
	 * aga.getSchedulingProperties(schedulingDetails); List<Dwhcolumn> columnList =
	 * aga.getPartitionColumnList(storageId);
	 * 
	 * final List<Measurementcounter> countersToGroup =
	 * aga.getCountersToGroup("DC_E_MGW_ATMPORT"); noGroupingMethod.invoke(aga,
	 * countersToGroup, "DC_E_MGW_ATMPORT_DAYBH_CALC", columnList, storageId,
	 * bhTypes, "DC_E_MGW:((7)):DC_E_MGW_ATMPORT:DAYBH"); } catch (Exception e) {
	 * assertEquals(expected, eoinsTestString); } }
	 * 
	 * @Test public void testIsColumnExistsNot(){ try{
	 * TestIntegAggregationGroupingAction aga = new
	 * TestIntegAggregationGroupingAction(); final Field dwhrepRockField =
	 * AggregationGroupingAction.class.getDeclaredField("dwhrepRock");
	 * dwhrepRockField.setAccessible(true); dwhrepRockField.set(aga, jUnitTestDB);
	 * String columnName = "DATE"; String storageId = "DC_E_MGW_ATMPORT:DAYBH";
	 * 
	 * List<Dwhcolumn> columnList = aga.getPartitionColumnList(storageId); boolean
	 * result = aga.isColumnExists(columnList, columnName, storageId);
	 * assertFalse(result); }catch(Exception e){
	 * fail("test case: testIsColumnExistsNot, failed: "+e.getMessage()); } }
	 * 
	 * @Test public void testGenerateCounterFunctionSql(){ String expectedSQL =
	 * "SUM(DC_E_MGW_ATMPORT_DAYBH_CALC.pmReceivedAtmCells) as pmReceivedAtmCells, "
	 * +
	 * "SUM(DC_E_MGW_ATMPORT_DAYBH_CALC.pmSecondsWithUnexp) as pmSecondsWithUnexp, "
	 * +
	 * "AVG(DC_E_MGW_ATMPORT_DAYBH_CALC.pmTransmittedAtmCells) as pmTransmittedAtmCells"
	 * ;
	 * 
	 * try{ TestIntegAggregationGroupingAction aga = new
	 * TestIntegAggregationGroupingAction(); final Field dwhrepRockField =
	 * AggregationGroupingAction.class.getDeclaredField("dwhrepRock");
	 * dwhrepRockField.setAccessible(true); dwhrepRockField.set(aga, jUnitTestDB);
	 * Method generateCounterFunctionSqlMethod =
	 * AggregationGroupingAction.class.getDeclaredMethod(
	 * "generateCounterFunctionSql", List.class, String.class);
	 * generateCounterFunctionSqlMethod.setAccessible(true); final Field logField =
	 * AggregationGroupingAction.class.getDeclaredField("log");
	 * logField.setAccessible(true); logField.set(aga, Logger.getLogger("dummy"));
	 * final List<Measurementcounter> countersToGroup =
	 * aga.getCountersToGroup("DC_E_MGW_ATMPORT"); String resultSQL =
	 * (String)generateCounterFunctionSqlMethod.invoke(aga, countersToGroup,
	 * "DC_E_MGW_ATMPORT_DAYBH_CALC"); assertEquals(expectedSQL, resultSQL);
	 * 
	 * }catch(Exception e){
	 * fail("test case: testGenerateCounterFunctionSql, failed: "+e.getMessage()); }
	 * }
	 * 
	 * @Test public void testGetCountersToGroup(){ try{ Properties properties = new
	 * Properties(); properties.setProperty("versionid", "DC_E_MGW:((7))"); final
	 * Field dwhrepRockField =
	 * AggregationGroupingAction.class.getDeclaredField("dwhrepRock");
	 * dwhrepRockField.setAccessible(true); final Field actionPropertiesField =
	 * AggregationGroupingAction.class.getDeclaredField("actionProperties");
	 * actionPropertiesField.setAccessible(true); final Field logField =
	 * AggregationGroupingAction.class.getDeclaredField("log");
	 * logField.setAccessible(true);
	 * 
	 * 
	 * TestIntegAggregationGroupingAction aga = new
	 * TestIntegAggregationGroupingAction(); dwhrepRockField.set(aga, jUnitTestDB);
	 * actionPropertiesField.set(aga, properties); logField.set(aga,
	 * Logger.getLogger("dummy"));
	 * 
	 * final String typeName = "DC_E_MGW_ATMPORT";
	 * 
	 * List<Measurementcounter> columnList = aga.getCountersToGroup(typeName);
	 * assertEquals(3, columnList.size()); }catch(Exception e){
	 * fail("test case: testGetCountersToGroup, failed: "+e.getMessage()); } }
	 * 
	 * @Before public void setUp() throws Exception {
	 * StaticProperties.giveProperties(new Properties()); jUnitTestDB = getTestDb();
	 * DatabaseTestUtils.loadSetup(jUnitTestDB, testBhTargetType); }
	 * 
	 * @After public void tearDown(){ DatabaseTestUtils.shutdown(jUnitTestDB); }
	 * 
	 * //-------- Private helper methods ----------------
	 * 
	 * private Date getDefaultAggregationDate(){ final Calendar cal =
	 * Calendar.getInstance(); cal.add(Calendar.DAY_OF_MONTH, -1);
	 * cal.set(Calendar.HOUR_OF_DAY, 0); cal.set(Calendar.MINUTE, 0);
	 * cal.set(Calendar.SECOND, 0); return cal.getTime(); }
	 * 
	 * private RockFactory getTestDb() throws Exception { if(jUnitTestDB == null ||
	 * jUnitTestDB.getConnection().isClosed()){ jUnitTestDB = new
	 * RockFactory(junitDbUrl, "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
	 * } return jUnitTestDB; }
	 * 
	 * private static Long getCollectionSetId(final RockFactory etlFactory, final
	 * String tpName) throws Exception { final Meta_collection_sets where = new
	 * Meta_collection_sets(etlFactory); where.setCollection_set_name(tpName); final
	 * Meta_collection_setsFactory fac = new Meta_collection_setsFactory(etlFactory,
	 * where);
	 * 
	 * @SuppressWarnings({"unchecked"}) final List<Meta_collection_sets> v =
	 * fac.get(); return v.get(0).getCollection_set_id(); }
	 * 
	 * private static Long getCollectionId(final RockFactory etlFactory, final Long
	 * collectionSetId, final String tpName, final String measType) throws Exception
	 * { final Meta_collections where = new Meta_collections(etlFactory);
	 * where.setCollection_set_id(collectionSetId); where.setSettype("Aggregator");
	 * final Meta_collectionsFactory fac = new Meta_collectionsFactory(etlFactory,
	 * where);
	 * 
	 * @SuppressWarnings({"unchecked"}) final List<Meta_collections> v = fac.get();
	 * for(Meta_collections c : v){ if(c.getCollection_name().indexOf(tpName + "_" +
	 * measType.toUpperCase() + "_DAYBH") != -1){ return c.getCollection_id(); } }
	 * return -1L; }
	 * 
	 * private static Meta_versions getMetaVersions(final RockFactory etlrep) throws
	 * Exception { final Meta_versions where = new Meta_versions(etlrep); final
	 * Meta_versionsFactory fac = new Meta_versionsFactory(etlrep, where); return
	 * (Meta_versions)fac.get().get(0); }
	 * 
	 * private static Meta_collections getMetaCollections(final RockFactory etlrep,
	 * final Long collSetId, final Long collId) throws Exception { final
	 * Meta_collections where = new Meta_collections(etlrep);
	 * where.setCollection_set_id(collSetId); where.setCollection_id(collId); final
	 * Meta_collectionsFactory fac = new Meta_collectionsFactory(etlrep, where);
	 * return (Meta_collections)fac.get().get(0); }
	 * 
	 * private static Meta_transfer_actions getMetaTransferActions(final RockFactory
	 * etl, final Long csId, final Long cId) throws Exception { final
	 * Meta_transfer_actions where = new Meta_transfer_actions(etl);
	 * where.setCollection_set_id(csId); where.setCollection_id(cId);
	 * where.setAction_type("Aggregation"); final Meta_transfer_actionsFactory fac =
	 * new Meta_transfer_actionsFactory(etl, where); return
	 * (Meta_transfer_actions)fac.get().get(0); }
	 * 
	 * private class TestIntegAggregationGroupingAction extends
	 * AggregationGroupingAction{ TestIntegAggregationGroupingAction (final
	 * Meta_versions version, final Long collectionSetId,//NOPMD final
	 * Meta_collections collection, final Long transferActionId, final Long
	 * transferBatchId, final Long connectId, final RockFactory etlrep, final
	 * ConnectionPool connectionPool, final Meta_transfer_actions trActions, final
	 * String batchColumnName, final Logger clog) throws Exception { super(version,
	 * collectionSetId, collection, transferActionId, transferBatchId, connectId,
	 * etlrep, connectionPool, trActions, clog); }
	 * 
	 * 
	 * public boolean isColumnExists(List<Dwhcolumn> columnList, String columnName,
	 * String storageId) { return super.isColumnExists(columnList, columnName,
	 * storageId); }
	 * 
	 * public Map<String, String> getKeyNameAndDataType(String mTableId) throws
	 * SQLException, RockException{ return super.getKeyNameAndDataType(mTableId); }
	 * 
	 * public List<String> timeCalculationsWithNullCheck(Map<String, String>
	 * columns, String table, String tt){ return
	 * super.timeCalculationsWithNullCheck(columns, table, tt); }
	 * 
	 * public TestIntegAggregationGroupingAction() { super(); }
	 * 
	 * @Override // Overriding this as the PhysicalTableCache isn't created...
	 * protected String getStoragePartition(String storageId, long dateId) throws
	 * Exception { return realPartition; }
	 * 
	 * @Override protected String generateGroupSql(VelocityContext context) throws
	 * Exception { eoinsTestString = super.generateGroupSql(context); return
	 * eoinsTestString; }
	 * 
	 * 
	 * 
	 * }
	 * 
	 * private static class UnitConnectionPool extends ConnectionPool { private
	 * RockFactory testDb = null; public UnitConnectionPool(RockFactory rockFact) {
	 * super(rockFact); testDb = rockFact; }
	 * 
	 * @Override public RockFactory getConnect(TransferActionBase trActionBase,
	 * String versionNumber, Long connectId) throws EngineMetaDataException { return
	 * testDb; } }
	 */}
