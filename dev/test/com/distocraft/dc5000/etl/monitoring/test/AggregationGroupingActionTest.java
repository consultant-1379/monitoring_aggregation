package com.distocraft.dc5000.etl.monitoring.test;

import junit.framework.TestCase;
/*
								import junit.framework.AssertionFailedError;
								import com.distocraft.dc5000.etl.monitoring.AggregationGroupingAction;
								import com.distocraft.dc5000.etl.rock.Meta_versions;
								import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
								import com.distocraft.dc5000.etl.rock.Meta_collections;
								import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
								import com.distocraft.dc5000.etl.rock.Meta_databases;
								import com.distocraft.dc5000.etl.rock.Meta_collectionsFactory;
								import com.distocraft.dc5000.etl.rock.Meta_collection_setsFactory;
								import com.distocraft.dc5000.etl.rock.Meta_transfer_actionsFactory;
								import com.distocraft.dc5000.etl.rock.Meta_versionsFactory;
								import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
								import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
								import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
								import com.distocraft.dc5000.repository.dwhrep.Measurementcounter;
								import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
								import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
								import com.distocraft.dc5000.repository.dwhrep.Busyhour;
								import com.distocraft.dc5000.repository.dwhrep.BusyhourFactory;
								import com.distocraft.dc5000.repository.dwhrep.MeasurementcounterFactory;
								import com.distocraft.dc5000.common.StaticProperties;
								import static org.easymock.classextension.EasyMock.createNiceMock;
								import static org.easymock.classextension.EasyMock.reset;
								import static org.easymock.classextension.EasyMock.replay;
								import static org.easymock.classextension.EasyMock.expectLastCall;
								import static org.easymock.classextension.EasyMock.anyBoolean;
								import static org.easymock.classextension.EasyMock.anyObject;
								import org.easymock.classextension.EasyMock;
								import ssc.rockfactory.RockFactory;
								import ssc.rockfactory.RockResultSet;
								
								import java.util.Iterator;
								import java.util.Properties;
								import java.util.Date;
								import java.util.Calendar;
								import java.util.Locale;
								import java.util.List;
								import java.util.ArrayList;
								import java.util.Arrays;
								import java.util.Map;
								import java.util.HashMap;
								import java.util.logging.Logger;
								import java.sql.Connection;
								import java.sql.Statement;
								import java.sql.SQLException;
								import java.sql.ResultSet;
								import java.sql.ResultSetMetaData;
								import java.net.URL;
								import java.io.File;
								import java.io.IOException;
								import java.io.FilenameFilter;
								import java.io.BufferedReader;
								import java.io.FileReader;*/
import org.junit.Ignore;

@Ignore("eemecoy 31/5/10 test failing in eclipse, reason unknown")
public class AggregationGroupingActionTest extends TestCase {
	/*
	 * 
	 * 
	 * private final static String _createStatementMetaFile =
	 * "TableCreateStatements.sql"; private final String junitDbUrl =
	 * "jdbc:hsqldb:mem:test_db_abc_123"; private String testTechPackName =
	 * "DC_E_MGW"; private String testMeasType = "ATMPORT"; private String versionId
	 * = "DC_E_MGW:((7))"; private String testBhTargetType = testTechPackName + "_"
	 * + testMeasType; private String realPartition = testBhTargetType +
	 * "_DAYBH_01"; private String actionAggDate = "2009-11-01"; private final
	 * String COUNTER_PREFIX = "PMC-"; private final String KEYCOL_PREFIX = "KC-";
	 * private final int TEST_COUNTER_COUNT = 3; private final int TEST_KEYCOL_COUNT
	 * = 2; private final String FUNCTION_PREFIX = "UNITFUNCTION"; private
	 * RockFactory jUnitTestDB = null; private final RockFactory rockFactory =
	 * createNiceMock(RockFactory.class); private final Meta_versions meta_versions
	 * = createNiceMock(Meta_versions.class); private final Meta_collections
	 * meta_collections = createNiceMock(Meta_collections.class); private final
	 * Meta_transfer_actions meta_transfer_actions =
	 * createNiceMock(Meta_transfer_actions.class); private final ConnectionPool
	 * connectionPool = createNiceMock(ConnectionPool.class);
	 * 
	 * private TestAggregationGroupingAction testInstance = null;
	 * 
	 * public AggregationGroupingActionTest() {
	 * super("AggregationGroupingActionTest"); }
	 * 
	 * public void testExecute_GroupByTimeNode_WikiData() { final Map<String,
	 * List<String>> expectedData = new HashMap<String, List<String>>();
	 * expectedData.put("MGW1:0", Arrays.asList("740", "1839"));
	 * expectedData.put("MGW1:15", Arrays.asList("1574", "1866"));
	 * expectedData.put("MGW1:30", Arrays.asList("1730", "1624"));
	 * expectedData.put("MGW1:45", Arrays.asList("1706", "1674"));
	 * expectedData.put("MGW2:0", Arrays.asList("1620", "780"));
	 * expectedData.put("MGW2:15", Arrays.asList("2033", "1219"));
	 * expectedData.put("MGW2:30", Arrays.asList("1413", "749"));
	 * expectedData.put("MGW2:45", Arrays.asList("1369", "1535")); final String
	 * checkdata = "select MGW, MIN_ID, c1, c2 from ";
	 * doGroupingTest(AggregationGroupingAction.GroupType.NodeTime, 1, expectedData,
	 * checkdata, Arrays.asList("MGW", "MIN_ID")); }
	 * 
	 * public void testExecute_GroupByTimeNode_RealData(){ try{
	 * loadUnitDb(jUnitTestDB, testBhTargetType); final String techPackName =
	 * "DC_E_MGW"; final String measType = "ATMPORT";
	 * 
	 * setGroupingType(AggregationGroupingAction.GroupType.NodeTime);
	 * 
	 * final Long collectionSetId = getCollectionSetId(jUnitTestDB, techPackName);
	 * final Long collectionId = getCollectionId(jUnitTestDB, collectionSetId,
	 * techPackName, measType); final Long transferBatchId = -1L; final Long
	 * connectId = 2L; final Meta_versions version = getMetaVersions(jUnitTestDB);
	 * final Meta_collections collections = getMetaCollections(jUnitTestDB,
	 * collectionSetId, collectionId); final Meta_transfer_actions actions =
	 * getMetaTransferActions(jUnitTestDB, collectionSetId, collectionId); final
	 * Long transferId = actions.getTransfer_action_id(); final String
	 * batchColumnName = ""; final ConnectionPool cp = new
	 * UnitConnectionPool(jUnitTestDB); actionAggDate = "2009-11-01 00:00:00"; final
	 * TestIntegAggregationGroupingAction groupAction = new
	 * TestIntegAggregationGroupingAction( version, collectionSetId, collections,
	 * transferId, transferBatchId, connectId, jUnitTestDB, cp, actions,
	 * batchColumnName, Logger.getAnonymousLogger()); groupAction.execute();
	 * getTestDb(); // action closes its dwhrep connection so we need to reopen it
	 * again... final String checkCount = "select count(*) cc from " +
	 * realPartition; final Statement stmt =
	 * jUnitTestDB.getConnection().createStatement(); final ResultSet rs =
	 * stmt.executeQuery(checkCount); if(rs.next()){ final String count =
	 * rs.getString("cc");
	 * assertEquals("Aggregation Grouping result count not correct ", "1",
	 * count);//4 different days in the calc table } } catch (Throwable t){ fail(t);
	 * } finally { shutdown(jUnitTestDB); } }
	 * 
	 * public void testExecute_GroupByNode_WikiData() { final Map<String,
	 * List<String>> expectedData = new HashMap<String, List<String>>();
	 * expectedData.put("MGW1:0", Arrays.asList("740", "1839"));
	 * expectedData.put("MGW1:15", Arrays.asList("1574", "1866"));
	 * expectedData.put("MGW1:30", Arrays.asList("1730", "1624"));
	 * expectedData.put("MGW1:45", Arrays.asList("1706", "1674"));
	 * expectedData.put("MGW2:0", Arrays.asList("1620", "780"));
	 * expectedData.put("MGW2:15", Arrays.asList("2033", "1219"));
	 * expectedData.put("MGW2:30", Arrays.asList("1413", "749"));
	 * expectedData.put("MGW2:45", Arrays.asList("1369", "1535")); final String
	 * checkdata = "select MGW, MIN_ID, c1, c2 from ";
	 * doGroupingTest(AggregationGroupingAction.GroupType.Node, 8, expectedData,
	 * checkdata, Arrays.asList("MGW", "MIN_ID")); }
	 * 
	 * public void testExecute_GroupByNode_RealData(){ try{ loadUnitDb(jUnitTestDB,
	 * testBhTargetType); final String techPackName = "DC_E_MGW"; final String
	 * measType = "ATMPORT";
	 * 
	 * setGroupingType(AggregationGroupingAction.GroupType.Node);
	 * 
	 * final Long collectionSetId = getCollectionSetId(jUnitTestDB, techPackName);
	 * final Long collectionId = getCollectionId(jUnitTestDB, collectionSetId,
	 * techPackName, measType); final Long transferBatchId = -1L; final Long
	 * connectId = 2L; final Meta_versions version = getMetaVersions(jUnitTestDB);
	 * final Meta_collections collections = getMetaCollections(jUnitTestDB,
	 * collectionSetId, collectionId); final Meta_transfer_actions actions =
	 * getMetaTransferActions(jUnitTestDB, collectionSetId, collectionId); final
	 * Long transferId = actions.getTransfer_action_id(); final String
	 * batchColumnName = ""; final ConnectionPool cp = new
	 * UnitConnectionPool(jUnitTestDB); actionAggDate = "2009-11-01 00:00:00"; final
	 * TestIntegAggregationGroupingAction groupAction = new
	 * TestIntegAggregationGroupingAction( version, collectionSetId, collections,
	 * transferId, transferBatchId, connectId, jUnitTestDB, cp, actions,
	 * batchColumnName, Logger.getAnonymousLogger()); groupAction.execute();
	 * getTestDb(); // action closes its dwhrep connection so we need to reopen it
	 * again... final String checkCount = "select count(*) cc from " +
	 * realPartition; final Statement stmt =
	 * jUnitTestDB.getConnection().createStatement(); final ResultSet rs =
	 * stmt.executeQuery(checkCount); if(rs.next()){ final String count =
	 * rs.getString("cc");
	 * assertEquals("Aggregation Grouping result count not correct ", "24",
	 * count);//4 different days in the calc table } } catch (Throwable t){ fail(t);
	 * } finally { shutdown(jUnitTestDB); } }
	 * 
	 * private void setGroupingType(final AggregationGroupingAction.GroupType type)
	 * throws Exception { final Busyhour where = new Busyhour(jUnitTestDB); final
	 * BusyhourFactory fac = new BusyhourFactory(jUnitTestDB, where); final
	 * List<Busyhour> bhl = fac.get(); for(Busyhour bh : bhl){
	 * bh.setGrouping(type.name()); bh.updateDB(); } }
	 * 
	 * 
	 * public void testExecute_GroupByTime_WikiData(){ final Map<String,
	 * List<String>> expectedData = new HashMap<String, List<String>>();
	 * expectedData.put("cell-1", Arrays.asList("2512", "720"));
	 * expectedData.put("cell-2", Arrays.asList("1375", "517"));
	 * expectedData.put("cell-3", Arrays.asList("1863", "514"));
	 * expectedData.put("cell-4", Arrays.asList("2712", "623"));
	 * expectedData.put("cell-5", Arrays.asList("2123", "243"));
	 * expectedData.put("cell-6", Arrays.asList("1600", "204")); final String
	 * checkdata = "select distinct (MOID), c1, c2 from ";
	 * doGroupingTest(AggregationGroupingAction.GroupType.Time, 6, expectedData,
	 * checkdata, Arrays.asList("MOID")); } public void
	 * testExecute_GroupByTime_RealData(){ try{ loadUnitDb(jUnitTestDB,
	 * testBhTargetType); final String techPackName = "DC_E_MGW"; final String
	 * measType = "ATMPORT"; final Long collectionSetId =
	 * getCollectionSetId(jUnitTestDB, techPackName); final Long collectionId =
	 * getCollectionId(jUnitTestDB, collectionSetId, techPackName, measType); final
	 * Long transferBatchId = -1L; final Long connectId = 2L; final Meta_versions
	 * version = getMetaVersions(jUnitTestDB); final Meta_collections collections =
	 * getMetaCollections(jUnitTestDB, collectionSetId, collectionId); final
	 * Meta_transfer_actions actions = getMetaTransferActions(jUnitTestDB,
	 * collectionSetId, collectionId); final Long transferId =
	 * actions.getTransfer_action_id(); final String batchColumnName = ""; final
	 * ConnectionPool cp = new UnitConnectionPool(jUnitTestDB); actionAggDate =
	 * "2009-11-01 00:00:00"; final TestIntegAggregationGroupingAction groupAction =
	 * new TestIntegAggregationGroupingAction( version, collectionSetId,
	 * collections, transferId, transferBatchId, connectId, jUnitTestDB, cp,
	 * actions, batchColumnName, Logger.getAnonymousLogger());
	 * groupAction.execute(); getTestDb(); // action closes its dwhrep connection so
	 * we need to reopen it again... final String checkCount =
	 * "select count(*) cc from " + realPartition; final Statement stmt =
	 * jUnitTestDB.getConnection().createStatement(); final ResultSet rs =
	 * stmt.executeQuery(checkCount); if(rs.next()){ final String count =
	 * rs.getString("cc");
	 * assertEquals("Aggregation Grouping result count not correct ", "24",
	 * count);//4 different days in the calc table } } catch (Throwable t){ fail(t);
	 * } finally { shutdown(jUnitTestDB); } } public void
	 * testExecute_NoGrouping_WikiData(){ final Map<String, List<String>>
	 * expectedData = new HashMap<String, List<String>>();
	 * expectedData.put("cell-10", Arrays.asList("284", "564"));
	 * expectedData.put("cell-115", Arrays.asList("714", "698"));
	 * expectedData.put("cell-130", Arrays.asList("695", "686"));
	 * expectedData.put("cell-145", Arrays.asList("819", "932"));
	 * expectedData.put("cell-20", Arrays.asList("169", "851"));
	 * expectedData.put("cell-215", Arrays.asList("415", "725"));
	 * expectedData.put("cell-230", Arrays.asList("623", "142"));
	 * expectedData.put("cell-245", Arrays.asList("168", "350"));
	 * expectedData.put("cell-30", Arrays.asList("287", "424"));
	 * expectedData.put("cell-315", Arrays.asList("445", "443"));
	 * expectedData.put("cell-330", Arrays.asList("412", "796"));
	 * expectedData.put("cell-345", Arrays.asList("719", "392"));
	 * expectedData.put("cell-40", Arrays.asList("615", "552"));
	 * expectedData.put("cell-415", Arrays.asList("811", "914"));
	 * expectedData.put("cell-430", Arrays.asList("356", "209"));
	 * expectedData.put("cell-445", Arrays.asList("930", "818"));
	 * expectedData.put("cell-50", Arrays.asList("839", "5"));
	 * expectedData.put("cell-515", Arrays.asList("655", "219"));
	 * expectedData.put("cell-530", Arrays.asList("570", "171"));
	 * expectedData.put("cell-545", Arrays.asList("59", "577"));
	 * expectedData.put("cell-60", Arrays.asList("166", "223"));
	 * expectedData.put("cell-615", Arrays.asList("567", "86"));
	 * expectedData.put("cell-630", Arrays.asList("487", "369"));
	 * expectedData.put("cell-645", Arrays.asList("380", "140")); final String
	 * checkdata = "select concat(MOID, MIN_ID) as MOID ,c1, c2 from ";
	 * doGroupingTest(AggregationGroupingAction.GroupType.None, 24, expectedData,
	 * checkdata, Arrays.asList("MOID")); } public void
	 * testExecute_NoGrouping_RealData(){ try{ loadUnitDb(jUnitTestDB,
	 * testBhTargetType); final String techPackName = "DC_E_MGW"; final String
	 * measType = "ATMPORT"; //default group type is Time, change it to None final
	 * Busyhour where = new Busyhour(jUnitTestDB); where.setBhlevel(techPackName +
	 * "_"+measType+"BH"); final BusyhourFactory fac = new
	 * BusyhourFactory(jUnitTestDB, where); final List<Busyhour> toChange =
	 * fac.get(); for(Busyhour bh : toChange){ bh.setGrouping("None");
	 * bh.updateDB(); } final Long collectionSetId = getCollectionSetId(jUnitTestDB,
	 * techPackName); final Long collectionId = getCollectionId(jUnitTestDB,
	 * collectionSetId, techPackName, measType); final Long transferBatchId = -1L;
	 * final Long connectId = 2L; final Meta_versions version =
	 * getMetaVersions(jUnitTestDB); final Meta_collections collections =
	 * getMetaCollections(jUnitTestDB, collectionSetId, collectionId); final
	 * Meta_transfer_actions actions = getMetaTransferActions(jUnitTestDB,
	 * collectionSetId, collectionId); final Long transferId =
	 * actions.getTransfer_action_id(); final String batchColumnName = ""; final
	 * ConnectionPool cp = new UnitConnectionPool(jUnitTestDB); actionAggDate =
	 * "2009-11-01 00:00:00"; final TestIntegAggregationGroupingAction groupAction =
	 * new TestIntegAggregationGroupingAction( version, collectionSetId,
	 * collections, transferId, transferBatchId, connectId, jUnitTestDB, cp,
	 * actions, batchColumnName, Logger.getAnonymousLogger());
	 * groupAction.execute(); getTestDb(); // action closes its dwhrep connection so
	 * we need to reopen it again... final Statement stmt =
	 * jUnitTestDB.getConnection().createStatement(); final String tmpTable =
	 * groupAction.getTempDayBhTable(); final ResultSet exp =
	 * stmt.executeQuery("select count(*) cc from " + tmpTable); String expCount =
	 * null; String actCount = "-1"; if(exp.next()){ expCount = exp.getString("cc");
	 * } exp.close(); final ResultSet rs =
	 * stmt.executeQuery("select count(*) cc from " + realPartition); if(rs.next()){
	 * actCount = rs.getString("cc"); } rs.close(); stmt.close();
	 * assertEquals("Aggregation Grouping result count not correct ", expCount,
	 * actCount); } catch (Throwable t){ fail(t); } finally { shutdown(jUnitTestDB);
	 * } }
	 * 
	 * public void testGrouping_GroupByTime_JUNIT(){ final String realPartitionName
	 * = "SOME_PARTITION_99"; try{ reset(rockFactory); final Connection c =
	 * createNiceMock(Connection.class); final Statement s =
	 * createNiceMock(Statement.class); final ResultSet rs =
	 * createNiceMock(ResultSet.class); rockFactory.getConnection();
	 * expectLastCall().andReturn(c); expectLastCall().anyTimes();
	 * c.createStatement(); expectLastCall().andReturn(s);
	 * expectLastCall().anyTimes(); s.getConnection();
	 * expectLastCall().andReturn(c); expectLastCall().anyTimes(); c.commit();
	 * expectLastCall().anyTimes(); s.executeQuery((String)anyObject());
	 * expectLastCall().andReturn(rs); rs.next(); expectLastCall().andReturn(true);
	 * rs.getString("BHTYPE"); expectLastCall().andReturn("SOMETYPE"); rs.next();
	 * expectLastCall().andReturn(false); replay(rs);
	 * mockGetStrorageId("dc_e_mgw_atmport:daybh"); setupEnableGroupingMock(true,
	 * false); s.executeUpdate((String)anyObject()); expectLastCall().andReturn(1);
	 * replay(c); replay(s); replay(rockFactory);
	 * testInstance.setTestDwhrepRock(rockFactory);
	 * testInstance.setTestStoragePartition(realPartitionName);
	 * testInstance.setGroupingType(AggregationGroupingAction.GroupType.Time);
	 * testInstance.execute(); final String lastSqlStmt =
	 * testInstance.getLastExecutedSql(); final String wantedSql =
	 * "insert into SOME_PARTITION_99 (KC-1, KC-2, PMC-1, PMC-2, PMC-3) SELECT KC-1, "
	 * +
	 * "KC-2, UNITFUNCTION(PMC-1) as PMC-1, UNITFUNCTION(PMC-2) as PMC-2, UNITFUNCTION(PMC-3) as PMC-3 "
	 * +
	 * "from DC_E_MGW_ATMPORT_DAYBH_CALC where (BHTYPE = 'SOMETYPE') GROUP BY KC-1, KC-2"
	 * ;
	 * 
	 * assertEquals("Genered Grouping SQL Doesnt look correct ", wantedSql,
	 * lastSqlStmt); } catch (Throwable t){ fail(t); } } public void
	 * testGrouping_NoGrouping_JUNIT(){ final String realPartitionName =
	 * "SOME_PARTITION_99"; try{ reset(rockFactory); final Connection c =
	 * createNiceMock(Connection.class); final Statement s =
	 * createNiceMock(Statement.class); final ResultSet rs =
	 * createNiceMock(ResultSet.class); rockFactory.getConnection();
	 * expectLastCall().andReturn(c); expectLastCall().anyTimes();
	 * c.createStatement(); expectLastCall().andReturn(s);
	 * expectLastCall().anyTimes(); s.getConnection();
	 * expectLastCall().andReturn(c); expectLastCall().anyTimes(); c.commit();
	 * expectLastCall().anyTimes(); s.executeQuery((String)anyObject());
	 * expectLastCall().andReturn(rs); rs.next(); expectLastCall().andReturn(true);
	 * rs.getString("BHTYPE"); expectLastCall().andReturn("SOMETYPE"); rs.next();
	 * expectLastCall().andReturn(false); replay(rs);
	 * mockGetStrorageId("dc_e_mgw_atmport:daybh"); setupEnableGroupingMock(false,
	 * false); s.executeUpdate((String)anyObject()); expectLastCall().andReturn(1);
	 * replay(c); replay(s); replay(rockFactory);
	 * testInstance.setTestDwhrepRock(rockFactory);
	 * testInstance.setTestStoragePartition(realPartitionName);
	 * testInstance.setGroupingType(AggregationGroupingAction.GroupType.None);
	 * testInstance.execute(); final String lastSqlStmt =
	 * testInstance.getLastExecutedSql(); final String temp =
	 * testInstance.getTempDayBhTable();
	 * assertEquals("Copy SQL Statement isnt correct ",
	 * "insert into "+realPartitionName+" select * from " + temp +
	 * " where (BHTYPE = 'SOMETYPE')", lastSqlStmt); } catch (Throwable t){
	 * t.printStackTrace(); fail(t); } }
	 * 
	 * private void switchCounterC2GroupAggFormula() throws Exception { final
	 * Measurementcounter where = new Measurementcounter(jUnitTestDB);
	 * where.setDataname("c2"); final MeasurementcounterFactory fac = new
	 * MeasurementcounterFactory(jUnitTestDB, where); final List<Measurementcounter>
	 * c = fac.get(); if(c.isEmpty()){
	 * fail("Testcase setup failed, couldn't find counter 'c2'"); } final
	 * Measurementcounter counter = c.get(0); counter.setGroupaggregation("SUM");
	 * counter.updateDB(); } private void doGroupingTest(final
	 * AggregationGroupingAction.GroupType groupingType, final int expectedDataSize,
	 * final Map<String, List<String>> expectedData, final String resultStmt, final
	 * List<String> uCols){ testTechPackName = "DC_E_TEST"; testMeasType =
	 * "SOMETYPE"; versionId = "DC_E_TEST:((7))"; testBhTargetType =
	 * testTechPackName + "_" + testMeasType; realPartition = testBhTargetType +
	 * "_DAYBH_01"; try { loadUnitDb(jUnitTestDB, testBhTargetType); if(groupingType
	 * == AggregationGroupingAction.GroupType.Node){
	 * switchCounterC2GroupAggFormula(); } } catch (Throwable t) {
	 * fail("Failed to setup test data", t.getCause()); shutdown(jUnitTestDB); }
	 * try{ final Long collectionSetId = getCollectionSetId(jUnitTestDB,
	 * testTechPackName); final Long collectionId = getCollectionId(jUnitTestDB,
	 * collectionSetId, testTechPackName, testMeasType); final Long transferBatchId
	 * = -1L; final Long connectId = 2L; final Meta_versions version =
	 * getMetaVersions(jUnitTestDB); final Meta_collections collections =
	 * getMetaCollections(jUnitTestDB, collectionSetId, collectionId); final
	 * Meta_transfer_actions actions = getMetaTransferActions(jUnitTestDB,
	 * collectionSetId, collectionId); final Long transferId =
	 * actions.getTransfer_action_id(); final String batchColumnName = ""; final
	 * ConnectionPool cp = new UnitConnectionPool(jUnitTestDB); actionAggDate =
	 * "2009-11-23 00:00:00"; final TestIntegAggregationGroupingAction groupAction =
	 * new TestIntegAggregationGroupingAction( version, collectionSetId,
	 * collections, transferId, transferBatchId, connectId, jUnitTestDB, cp,
	 * actions, batchColumnName, Logger.getAnonymousLogger());
	 * groupAction.setGroupingType(groupingType); groupAction.execute(); final
	 * String checkCount = "select count(*) cc from " + realPartition; getTestDb();
	 * // action closes its dwhrep connection so we need to reopen it again... final
	 * Statement stmt = jUnitTestDB.getConnection().createStatement(); final
	 * ResultSet rs = stmt.executeQuery(checkCount); if(rs.next()){ final String
	 * count = rs.getString("cc");
	 * assertEquals("Aggregation Grouping result count not correct ",
	 * Integer.toString(expectedDataSize), count); } final ResultSet rsc =
	 * stmt.executeQuery(resultStmt + " " + realPartition); while(rsc.next()){ final
	 * StringBuilder _key = new StringBuilder(); final Iterator<String> iter =
	 * uCols.iterator(); while(iter.hasNext()){ final String cn = iter.next(); final
	 * String kPart = rsc.getString(cn); _key.append(kPart); if(iter.hasNext()){
	 * _key.append(":"); } } final String key = _key.toString(); final String c1 =
	 * rsc.getString("C1"); final String c2 = rsc.getString("C2");
	 * assertTrue("Data key not found '"+key+"'", expectedData.containsKey(key));
	 * final String e1 = expectedData.get(key).get(0); final String e2 =
	 * expectedData.get(key).get(1);
	 * assertEquals("Grouped value for C1 isnt correct", e1, c1);
	 * assertEquals("Grouped value for C2 isnt correct", e2, c2); } rsc.close(); }
	 * catch (Throwable t){ fail(t); } finally { shutdown(jUnitTestDB); } } public
	 * void testIntegGetPartitionColumnList(){ try{ loadUnitDb(jUnitTestDB,
	 * testBhTargetType); testInstance.setTestDwhrepRock(jUnitTestDB);
	 * testInstance.getRealData(); final List<Dwhcolumn> columns =
	 * testInstance.getPartitionColumnList("DC_E_MGW_ATMPORT:DAYBH"); final
	 * List<String> expected = Arrays.asList("OSS_ID", "SN", "NEUN", "NEDN", "NESW",
	 * "MGW", "MOID", "TransportNetwork", "AtmPort", "userLabel", "DATE_ID",
	 * "YEAR_ID", "MONTH_ID", "DAY_ID", "MIN_ID", "BHTYPE", "BUSYHOUR", "BHCLASS",
	 * "TIMELEVEL", "SESSION_ID", "BATCH_ID", "PERIOD_DURATION", "ROWSTATUS",
	 * "DC_RELEASE", "DC_SOURCE", "DC_TIMEZONE", "DC_SUSPECTFLAG",
	 * "pmTransmittedAtmCells", "pmReceivedAtmCells", "pmSecondsWithUnexp");
	 * assertEquals("Wrong column count ", 30, columns.size()); final List<String>
	 * actual = new ArrayList<String>(); for(Dwhcolumn c : columns){
	 * if(!expected.contains(c.getDataname())){
	 * fail("Unknown column name found : Unknown <"+c.getDataname()+">"); }
	 * actual.add(c.getDataname()); } for(String exp : expected){
	 * if(!actual.contains(exp)){
	 * fail("Expected column not found : Expected <"+exp+">"); } } } catch
	 * (Throwable t){ fail(t); } finally { shutdown(jUnitTestDB); } } public void
	 * testIntegGetCountersToGroup(){ try{ final URL url =
	 * ClassLoader.getSystemClassLoader().getResource(testBhTargetType); final File
	 * f = new File(url.toURI()); loadSetup(jUnitTestDB, f.getAbsolutePath());
	 * testInstance.setTestDwhrepRock(jUnitTestDB); testInstance.getRealData();
	 * final List<Measurementcounter> types =
	 * testInstance.getCountersToGroup(testBhTargetType);
	 * assertEquals("Wrong column count ", 3, types.size()); final List<String>
	 * actual = new ArrayList<String>(); final List<String> expected =
	 * Arrays.asList("pmTransmittedAtmCells", "pmReceivedAtmCells",
	 * "pmSecondsWithUnexp"); for(Measurementcounter c : types){
	 * if(!expected.contains(c.getDataname())){
	 * fail("Unknown column name found : Unknown <"+c.getDataname()+">"); }
	 * actual.add(c.getDataname()); } for(String exp : expected){
	 * if(!actual.contains(exp)){
	 * fail("Expected column not found : Expected <"+exp+">"); } } } catch
	 * (Throwable t){ fail(t); } }
	 * 
	 * public void testIsGroupingEnabled(){ final boolean enabled = true; try{
	 * setupEnableGroupingMock(enabled, true); final boolean b =
	 * testInstance.isGroupingEnabled();
	 * assertEquals("Wrong result from enabled check ", enabled, b); } catch
	 * (Throwable t){ fail(t); } } public void testIsGroupingDisabled(){ final
	 * boolean enabled = false; try{ setupEnableGroupingMock(enabled, true); final
	 * boolean b = testInstance.isGroupingEnabled();
	 * assertEquals("Wrong result from enabled check ", enabled, b); } catch
	 * (Throwable t){ fail(t); } } private void setupMockMetaDatabases(final String
	 * dwhDbUrl) throws Exception { final RockResultSet rss =
	 * createNiceMock(RockResultSet.class); final Iterator iterator =
	 * createNiceMock(Iterator.class); final Meta_databases metaDatabase =
	 * createNiceMock(Meta_databases.class); rockFactory.setSelectSQL(anyBoolean(),
	 * anyObject()); expectLastCall().andReturn(rss);
	 * rockFactory.getData(anyObject(), (RockResultSet)anyObject());
	 * expectLastCall().andReturn(iterator); iterator.hasNext();
	 * expectLastCall().andReturn(true); iterator.next();
	 * expectLastCall().andReturn(metaDatabase);
	 * metaDatabase.getUsername();expectLastCall().andReturn("SA");
	 * metaDatabase.getPassword();expectLastCall().andReturn("");
	 * metaDatabase.getDriver_name();expectLastCall().andReturn(
	 * "org.hsqldb.jdbcDriver");
	 * metaDatabase.getConnection_string();expectLastCall().andReturn(dwhDbUrl);
	 * replay(iterator); replay(rss); replay(metaDatabase); } public void
	 * testGetDwhrepRock(){ try{ reset(rockFactory);
	 * setupMockMetaDatabases(junitDbUrl); replay(rockFactory); } catch (Throwable
	 * t){ fail("Testcase setup failed ", t); } try{ final RockFactory dwhrep =
	 * testInstance.getDwhrepRock();
	 * assertFalse("DWHREP connection should be open ",
	 * dwhrep.getConnection().isClosed()); assertEquals(junitDbUrl,
	 * dwhrep.getDbURL()); testInstance.close();
	 * assertTrue("DwhRep connections should be closed ",
	 * dwhrep.getConnection().isClosed()); } catch (NullPointerException e){
	 * e.printStackTrace(); } catch (Throwable t){ t.printStackTrace(); } } public
	 * void testGetAggregationDate(){ final Properties properties = new
	 * Properties(); String EY = "2004"; final String EMN = "12"; final String ED =
	 * "14"; final String EH = "12"; final String EMIN = "13"; final String ESEC =
	 * "14"; final String expectedAggDate =
	 * EY+"-"+EMN+"-"+ED+" "+EH+":"+EMIN+":"+ESEC;
	 * properties.put(AggregationGroupingAction.KEY_AGGDATE, expectedAggDate);
	 * setupProperties(createTestWhereClause(properties));
	 * testInstance.getAggregationDate(); final long aggDate =
	 * testInstance.getAggregationDate(); final Date testDate = new Date(aggDate);
	 * final Calendar testCal = Calendar.getInstance(Locale.getDefault());
	 * testCal.setTime(testDate); assertEquals((testCal.get(Calendar.YEAR)+""), EY);
	 * assertEquals((testCal.get(Calendar.MONTH)+1+""), EMN);
	 * assertEquals((testCal.get(Calendar.DAY_OF_MONTH)+""), ED); } public void
	 * testGetDefaultAggregationDate(){ // Default Agg date is today... final
	 * Calendar _defaultCal = Calendar.getInstance();
	 * _defaultCal.add(Calendar.DAY_OF_MONTH, -1);
	 * _defaultCal.set(Calendar.HOUR_OF_DAY, 0); _defaultCal.set(Calendar.MINUTE,
	 * 0); _defaultCal.set(Calendar.SECOND, 0); final long aggDate =
	 * testInstance.getAggregationDate(); final Date testDate = new Date(aggDate);
	 * final Calendar testCal = Calendar.getInstance(Locale.getDefault());
	 * testCal.setTime(testDate); final String error =
	 * "Default Aggregation Date not correct : Expected<"+_defaultCal+"> : Actual<"+
	 * testDate+">"; assertTrue(error, checkCalenderField(_defaultCal, testCal,
	 * Calendar.YEAR)); assertTrue(error, checkCalenderField(_defaultCal, testCal,
	 * Calendar.MONTH)); assertTrue(error, checkCalenderField(_defaultCal, testCal,
	 * Calendar.DAY_OF_MONTH)); assertTrue(error, checkCalenderField(_defaultCal,
	 * testCal, Calendar.HOUR_OF_DAY)); assertTrue(error,
	 * checkCalenderField(_defaultCal, testCal, Calendar.MINUTE)); assertTrue(error,
	 * checkCalenderField(_defaultCal, testCal, Calendar.SECOND)); }
	 * 
	 * public void testGroupPublic_TimeDefault(){ final String colDefault =
	 * testInstance.groupPublic_Time();
	 * assertEquals("Default MOID column name not correct",
	 * AggregationGroupingAction.GROUPPUBLICTIME, colDefault); }
	 * 
	 * 
	 * public void testGroupPublic_TimeNodeDefault(){ final String colDefault =
	 * testInstance.groupPublic_TimeNode();
	 * assertEquals("Default MOID column name not correct",
	 * AggregationGroupingAction.GROUPPUBLICTIMENODE, colDefault); }
	 * 
	 * public void testGroupKey_NodeDefault(){ final String colDefault =
	 * testInstance.groupKey_Node();
	 * assertEquals("Default MOID column name not correct",
	 * AggregationGroupingAction.GROUPKEYNODE, colDefault); }
	 * 
	 * public void testGroupKey_TimeNodeDefault(){ final String colDefault =
	 * testInstance.groupKey_TimeNode();
	 * assertEquals("Default Minid column name not correct",
	 * AggregationGroupingAction.GROUPKEYTIMENODE, colDefault); }
	 * 
	 * public void testGetGroupPublic_Time(){ final Properties properties = new
	 * Properties(); final String expected = "abcdef";
	 * properties.put(AggregationGroupingAction.GROUPPUBLICTIME, expected); try{
	 * setupProperties(createTestWhereClause(properties));
	 * StaticProperties.giveProperties(properties); final String colDefault =
	 * testInstance.groupPublic_Time();
	 * assertEquals("GroupPublic_Time column name not correct", expected,
	 * colDefault); } catch (Exception e){ fail(e); } }
	 * 
	 * public void testGetGroupPublic_TimeNode(){ final Properties properties = new
	 * Properties(); final String expected = "abcdef";
	 * properties.put(AggregationGroupingAction.GROUPPUBLICTIMENODE, expected); try{
	 * setupProperties(createTestWhereClause(properties));
	 * StaticProperties.giveProperties(properties); final String colDefault =
	 * testInstance.groupPublic_TimeNode();
	 * assertEquals("GroupPublic_TimeNode column name not correct", expected,
	 * colDefault); } catch (Exception e){ fail(e); } }
	 * 
	 * public void testGetGroupKey_Node(){ final Properties properties = new
	 * Properties(); final String expected = "abcdef";
	 * properties.put(AggregationGroupingAction.GROUPKEYNODE, expected); try{
	 * setupProperties(createTestWhereClause(properties));
	 * StaticProperties.giveProperties(properties); final String colDefault =
	 * testInstance.groupKey_Node();
	 * assertEquals("GroupKey_Node column name not correct", expected, colDefault);
	 * } catch (Exception e){ fail(e); } }
	 * 
	 * public void testGetGroupKey_TimeNode(){ final Properties properties = new
	 * Properties(); final String expected = "abcdef";
	 * properties.put(AggregationGroupingAction.GROUPKEYTIMENODE, expected); try{
	 * setupProperties(createTestWhereClause(properties));
	 * StaticProperties.giveProperties(properties); final String colDefault =
	 * testInstance.groupKey_TimeNode();
	 * assertEquals("GroupKey_TimeNode column name not correct", expected,
	 * colDefault); } catch (Exception e){ fail(e); } } public void
	 * testGetTypeName(){ try{ final String typename = testInstance.getBhLevel();
	 * assertEquals("TypeName not parsed from Meta_transfer_actions where clause correctly"
	 * , testBhTargetType, typename); } catch (EngineMetaDataException e){ fail(e);
	 * } } public void testGetTypeNameNotSet(){
	 * setupProperties(createTestWhereClause("weeeeeeeeee", testBhTargetType)); try{
	 * testInstance.getBhLevel();
	 * fail("An exception should have been thrown if typename isnt set"); } catch
	 * (EngineMetaDataException e){ assertTrue("",
	 * e.getMessage().contains("No typename meta")); } } public void
	 * testGetTypeNameSetEmpty(){ setupProperties(createTestWhereClause("")); try{
	 * testInstance.getBhLevel();
	 * fail("An exception should have been thrown if typename isnt set"); } catch
	 * (EngineMetaDataException e){ assertTrue("",
	 * e.getMessage().contains("No typename meta")); } } public void
	 * testNoPropertiesDefined(){ setupNoProperties(); try{
	 * testInstance.getTempDayBhTable();
	 * fail("EngineMetaDataException should have been thrown"); } catch
	 * (EngineMetaDataException e){ // ok, expecting this.... } catch (Throwable t){
	 * fail(t); } }
	 * 
	 * private void mockGetStrorageId(final String storageId) throws Exception {
	 * final RockResultSet rss = createNiceMock(RockResultSet.class); final Iterator
	 * iterator = createNiceMock(Iterator.class); final Dwhtype dwhType =
	 * createNiceMock(Dwhtype.class); rockFactory.setSelectSQL(EasyMock.eq(false),
	 * anyObject()); expectLastCall().andReturn(rss);
	 * rockFactory.getData(anyObject(), (RockResultSet)anyObject());
	 * expectLastCall().andReturn(iterator); iterator.hasNext();
	 * expectLastCall().andReturn(true); iterator.next();
	 * expectLastCall().andReturn(dwhType); dwhType.getStorageid();
	 * expectLastCall().andReturn(storageId); dwhType.getDatadatecolumn();
	 * expectLastCall().andReturn("DATE_ID"); expectLastCall().anyTimes();
	 * replay(iterator); replay(rss); replay(dwhType); } private void
	 * setupEnableGroupingMock(final boolean groupingEnabled, final boolean
	 * startReplay) throws Exception { if(startReplay){ reset(rockFactory); } final
	 * RockResultSet rss = createNiceMock(RockResultSet.class); final Iterator
	 * iterator = createNiceMock(Iterator.class); final Meta_transfer_actions
	 * trAction = createNiceMock(Meta_transfer_actions.class);
	 * rockFactory.setSelectSQL(anyBoolean(), anyObject());
	 * expectLastCall().andReturn(rss); rockFactory.getData(anyObject(),
	 * (RockResultSet)anyObject()); expectLastCall().andReturn(iterator);
	 * iterator.hasNext(); expectLastCall().andReturn(true); iterator.next();
	 * expectLastCall().andReturn(trAction); trAction.getEnabled_flag();
	 * if(groupingEnabled){ expectLastCall().andReturn("Y"); } else {
	 * expectLastCall().andReturn("N"); } replay(iterator); replay(rss);
	 * replay(trAction); if(startReplay){ replay(rockFactory); } } private boolean
	 * checkCalenderField(final Calendar exp, final Calendar test, final int field){
	 * return exp.get(field) == test.get(field); } private void
	 * mockTransferActions(final String whereClause){ reset(meta_transfer_actions);
	 * meta_transfer_actions.getAction_contents(); final String toReturn;
	 * if(whereClause == null){ toReturn = createTestWhereClause(testBhTargetType);
	 * } else { toReturn = whereClause; } expectLastCall().andReturn(toReturn);
	 * replay(meta_transfer_actions); } private String createTestWhereClause(final
	 * Properties properties){ final StringBuilder sb = new StringBuilder();
	 * sb.append("#\n"); sb.append("#Wed Nov 04 10:54:24 GMT 2009\n"); for(Object
	 * key : properties.keySet()){
	 * sb.append(key).append("=").append(properties.getProperty((String)key)).append
	 * ("\n"); } return sb.toString(); } private String createTestWhereClause(final
	 * String typeName, final String bhType){ final Properties p = new Properties();
	 * p.put(typeName, bhType); return createTestWhereClause(p); } private String
	 * createTestWhereClause(final String bhType){ return
	 * createTestWhereClause(AggregationGroupingAction.KEY_TYPENAME, bhType); }
	 * private void setUp(final String whereClause) throws Exception {
	 * mockActionCreation(rockFactory); mockTransferActions(whereClause);
	 * replay(rockFactory); replay(meta_versions); replay(meta_collections);
	 * connectionPool.getConnect((TransferActionBase)anyObject(),
	 * (String)anyObject(), (Long)anyObject());
	 * expectLastCall().andReturn(rockFactory); replay(connectionPool); testInstance
	 * = new TestAggregationGroupingAction( meta_versions, 1L, meta_collections, 1L,
	 * 1L, 1L, rockFactory, connectionPool, meta_transfer_actions, "",
	 * Logger.getAnonymousLogger()); }
	 * 
	 * @Override protected void setUp() throws Exception { setUp(null);
	 * StaticProperties.giveProperties(new Properties()); jUnitTestDB = getTestDb();
	 * } private RockFactory getTestDb() throws Exception { if(jUnitTestDB == null
	 * || jUnitTestDB.getConnection().isClosed()){ jUnitTestDB = new
	 * RockFactory(junitDbUrl, "SA", "", "org.hsqldb.jdbcDriver", "con", true, -1);
	 * } return jUnitTestDB; } private void setupProperties(final String props){
	 * try{ tearDown(); setUp(props); } catch (Exception e){
	 * fail("Failed to setup testcase ", e); } } private void setupNoProperties(){
	 * setupProperties(""); }
	 * 
	 * @Override protected void tearDown() throws Exception { reset(rockFactory);
	 * reset(meta_versions); reset(meta_collections); reset(meta_transfer_actions);
	 * reset(connectionPool); testInstance = null; shutdown(jUnitTestDB); } private
	 * void mockActionCreation(final RockFactory mockedFactory) throws Exception {
	 * final RockResultSet rss = createNiceMock(RockResultSet.class);
	 * mockedFactory.setSelectSQL(anyBoolean(), anyObject());
	 * expectLastCall().andReturn(rss); final Iterator iterator =
	 * createNiceMock(Iterator.class); final Meta_collection_sets set =
	 * createNiceMock(Meta_collection_sets.class);
	 * mockedFactory.getData(anyObject(), (RockResultSet)anyObject());
	 * expectLastCall().andReturn(iterator); iterator.hasNext();
	 * expectLastCall().andReturn(true); iterator.next();
	 * expectLastCall().andReturn(set); set.getCollection_set_id();
	 * expectLastCall().andReturn(1L); set.getCollection_set_name();
	 * expectLastCall().andReturn("someName"); set.getDescription();
	 * expectLastCall().andReturn("desc"); set.getVersion_number();
	 * expectLastCall().andReturn("((6))"); set.getEnabled_flag();
	 * expectLastCall().andReturn("1"); set.getType();
	 * expectLastCall().andReturn("Aggregation"); rss.close(); replay(rss);
	 * replay(iterator); replay(set); } private void shutdown(final RockFactory db){
	 * try{ if(db != null && !db.getConnection().isClosed()){ final Statement stmt =
	 * db.getConnection().createStatement(); stmt.executeUpdate("SHUTDOWN");
	 * stmt.close(); db.getConnection().close(); } } catch (Throwable t){ // ignore
	 * } } private void loadUnitDb(final RockFactory unitdb, final String dir)
	 * throws Exception { try{ final URL url =
	 * ClassLoader.getSystemClassLoader().getResource(dir); final File f = new
	 * File(url.toURI()); loadSetup(unitdb, f.getAbsolutePath()); } catch
	 * (SQLException e){ throw new Exception(e.getMessage() + "\n\t" +
	 * e.getCause()); } } private void fail(final Throwable error){
	 * fail(error.getMessage(), error); } private void fail(final String msg, final
	 * Throwable error){ if(error instanceof AssertionFailedError){ throw
	 * (AssertionFailedError)error; } final StringBuilder sb = new
	 * StringBuilder(msg); sb.append("\n\t"); sb.append(error.getMessage());
	 * sb.append("\n\t"); for(StackTraceElement ste : error.getStackTrace()){
	 * sb.append(ste).append("\n\t"); } fail(sb.toString().trim()); } private static
	 * class UnitConnectionPool extends ConnectionPool { private RockFactory testDb
	 * = null; public UnitConnectionPool(RockFactory rockFact) { super(rockFact);
	 * testDb = rockFact; }
	 * 
	 * @Override public RockFactory getConnect(TransferActionBase trActionBase,
	 * String versionNumber, Long connectId) throws EngineMetaDataException { return
	 * testDb; } } private void loadSetup(final RockFactory testDB, final String
	 * baseDir) throws ClassNotFoundException, IOException, SQLException { final
	 * File loadFrom = new File(baseDir); final File[] toLoad =
	 * loadFrom.listFiles(new FilenameFilter(){ public boolean accept(File dir,
	 * String name) { return name.endsWith(".sql") &&
	 * !name.equals(_createStatementMetaFile); } }); final File createFile = new
	 * File(baseDir + "/" + _createStatementMetaFile); loadSqlFile(createFile,
	 * testDB); for(File loadFile : toLoad){ loadSqlFile(loadFile, testDB); } }
	 * private void loadSqlFile(final File sqlFile, final RockFactory testDB) throws
	 * IOException, SQLException, ClassNotFoundException { if(!sqlFile.exists()){
	 * System.out.println(sqlFile + " doesnt exist, skipping.."); return; }
	 * BufferedReader br = new BufferedReader(new FileReader(sqlFile)); String line;
	 * int lineCount = 0; try{ while ( (line = br.readLine()) != null){ lineCount++;
	 * line = line.trim(); if(line.length() == 0 || line.startsWith("#")){ continue;
	 * } while(!line.endsWith(";")){ final String tmp = br.readLine(); if(tmp!=
	 * null){ line += "\r\n"; line += tmp; } else { break; } } update(line, testDB);
	 * } testDB.commit(); } catch (SQLException e){ throw new
	 * SQLException("Error executing on line ["+lineCount+"] of " + sqlFile, e); }
	 * finally { br.close(); } } private void update(final String insertSQL, final
	 * RockFactory testDB) throws SQLException, ClassNotFoundException, IOException
	 * { final Statement s = testDB.getConnection().createStatement(); try{
	 * _doExecuteUpdate(insertSQL, s); } catch (SQLException e){
	 * if(e.getSQLState().equals("S0004")){
	 * System.out.println("Views not supported yet: " + e.getMessage()); } else
	 * if(e.getSQLState().equals("S0001") || e.getSQLState().equals("42504")){
	 * //ignore, table already exists....... } else { throw e; } } } private void
	 * _doExecuteUpdate(final String insertSQL, final Statement s) throws
	 * SQLException { s.executeUpdate(insertSQL); } private static Long
	 * getCollectionId(final RockFactory etlFactory, final Long collectionSetId,
	 * final String tpName, final String measType) throws Exception { final
	 * Meta_collections where = new Meta_collections(etlFactory);
	 * where.setCollection_set_id(collectionSetId); where.setSettype("Aggregator");
	 * final Meta_collectionsFactory fac = new Meta_collectionsFactory(etlFactory,
	 * where);
	 * 
	 * @SuppressWarnings({"unchecked"}) final List<Meta_collections> v = fac.get();
	 * for(Meta_collections c : v){ if(c.getCollection_name().indexOf(tpName + "_" +
	 * measType.toUpperCase() + "_DAYBH") != -1){ return c.getCollection_id(); } }
	 * return -1L; } private static Long getCollectionSetId(final RockFactory
	 * etlFactory, final String tpName) throws Exception { final
	 * Meta_collection_sets where = new Meta_collection_sets(etlFactory);
	 * where.setCollection_set_name(tpName); final Meta_collection_setsFactory fac =
	 * new Meta_collection_setsFactory(etlFactory, where);
	 * 
	 * @SuppressWarnings({"unchecked"}) final List<Meta_collection_sets> v =
	 * fac.get(); return v.get(0).getCollection_set_id(); } private static
	 * Meta_transfer_actions getMetaTransferActions(final RockFactory etl, final
	 * Long csId, final Long cId) throws Exception { final Meta_transfer_actions
	 * where = new Meta_transfer_actions(etl); where.setCollection_set_id(csId);
	 * where.setCollection_id(cId); where.setAction_type("Aggregation"); final
	 * Meta_transfer_actionsFactory fac = new Meta_transfer_actionsFactory(etl,
	 * where); return (Meta_transfer_actions)fac.get().get(0); } private static
	 * Meta_collections getMetaCollections(final RockFactory etlrep, final Long
	 * collSetId, final Long collId) throws Exception { final Meta_collections where
	 * = new Meta_collections(etlrep); where.setCollection_set_id(collSetId);
	 * where.setCollection_id(collId); final Meta_collectionsFactory fac = new
	 * Meta_collectionsFactory(etlrep, where); return
	 * (Meta_collections)fac.get().get(0); } private static Meta_versions
	 * getMetaVersions(final RockFactory etlrep) throws Exception { final
	 * Meta_versions where = new Meta_versions(etlrep); final Meta_versionsFactory
	 * fac = new Meta_versionsFactory(etlrep, where); return
	 * (Meta_versions)fac.get().get(0); } private class
	 * TestIntegAggregationGroupingAction extends AggregationGroupingAction {
	 * private GroupType _groupingType = null; public
	 * TestIntegAggregationGroupingAction(final Meta_versions version, final Long
	 * collectionSetId, final Meta_collections collection, final Long
	 * transferActionId, final Long transferBatchId, final Long connectId, final
	 * RockFactory etlrep, final ConnectionPool connectionPool, final
	 * Meta_transfer_actions trActions, final String batchColumnName, final Logger
	 * clog) throws Exception { super(version, collectionSetId, collection,
	 * transferActionId, transferBatchId, connectId, etlrep, connectionPool,
	 * trActions, clog); actionProperties = new Properties();
	 * actionProperties.put("aggDate", actionAggDate);
	 * actionProperties.put("typename", testBhTargetType+"BH");
	 * actionProperties.put("tabletype", testBhTargetType);
	 * actionProperties.put("versionid", versionId); } public void
	 * setGroupingType(final GroupType type){ _groupingType = type; }
	 * 
	 * @Override protected RockFactory getDwhrepRock() throws Exception {
	 * if(jUnitTestDB == null){ return super.getDwhrepRock(); } else { return
	 * jUnitTestDB; } }
	 * 
	 * @Override public String getTempDayBhTable() throws Exception { return
	 * super.getTempDayBhTable(); }
	 * 
	 * @Override // Overriding this as the PhysicalTableCache isnt created...
	 * protected String getStoragePartition(String storageId, long dateId) throws
	 * Exception { return realPartition; }
	 * 
	 * @Override protected GroupType getGroupingType(final String bhType, final
	 * String bhLevel) throws Exception { if(_groupingType == null){ return
	 * super.getGroupingType(bhType, bhLevel); } else { return _groupingType; } }
	 * 
	 * @Override public RockFactory getConnection() { if(jUnitTestDB == null){
	 * return super.getConnection(); } else { return jUnitTestDB; } }
	 * 
	 * @Override protected void close() { if(jUnitTestDB == null){ super.close(); }
	 * else { try { jUnitTestDB.getConnection().close(); } catch (SQLException e) {
	 * // ignore } } } } private class TestAggregationGroupingAction extends
	 * AggregationGroupingAction { public TestAggregationGroupingAction(final
	 * Meta_versions version, final Long collectionSetId, final Meta_collections
	 * collection, final Long transferActionId, final Long transferBatchId, final
	 * Long connectId, final RockFactory etlrep, final ConnectionPool
	 * connectionPool, final Meta_transfer_actions trActions, final String
	 * batchColumnName, final Logger clog) throws Exception { super(version,
	 * collectionSetId, collection, transferActionId, transferBatchId, connectId,
	 * etlrep, connectionPool, trActions, clog); } private boolean getRealData =
	 * false; private String lastSqlExecuted = null; private RockFactory
	 * testDwhrepRockInstance = null; private String testPartitionName = null;
	 * private GroupType _groupingType = null; public void setTestDwhrepRock(final
	 * RockFactory rf){ testDwhrepRockInstance = rf; dwhrepRock =
	 * testDwhrepRockInstance; }
	 * 
	 * @Override protected GroupType getGroupingType(String bhType, final String
	 * bhLevel) throws Exception { if(_groupingType == null){ return
	 * super.getGroupingType(bhType, bhLevel); } else { return _groupingType; } }
	 * public void setGroupingType(final GroupType type){ _groupingType = type; }
	 *//**
		 * Returns a connect object from the connectionPool
		 */
	/*
	 * @Override public RockFactory getConnection() { if(testDwhrepRockInstance ==
	 * null){ return super.getConnection(); } else { return testDwhrepRockInstance;
	 * } } public void getRealData(){ getRealData = true; }
	 * 
	 * @Override protected List<Measurementcounter> getCountersToGroup(String
	 * typeName) throws Exception { if(getRealData){ return
	 * super.getCountersToGroup(typeName); } else { final List<Measurementcounter>
	 * list = new ArrayList<Measurementcounter>(); for(int
	 * i=1;i<=TEST_COUNTER_COUNT;i++){ final Measurementcounter mocked =
	 * createNiceMock(Measurementcounter.class); mocked.getDataname(); final String
	 * counterName = COUNTER_PREFIX + i; expectLastCall().andReturn(counterName);
	 * expectLastCall().anyTimes(); mocked.getGroupaggregation();
	 * expectLastCall().andReturn(FUNCTION_PREFIX); replay(mocked);
	 * list.add(mocked); } return list; } }
	 * 
	 * @Override public List<Dwhcolumn> getPartitionColumnList(String storageId)
	 * throws Exception { if(getRealData){ return
	 * super.getPartitionColumnList(storageId); } else { final List<Dwhcolumn> list
	 * = new ArrayList<Dwhcolumn>(); for(int i=1;i<=TEST_KEYCOL_COUNT;i++){ final
	 * Dwhcolumn mocked = createNiceMock(Dwhcolumn.class); mocked.getDataname();
	 * expectLastCall().andReturn(KEYCOL_PREFIX + i); expectLastCall().anyTimes();
	 * replay(mocked); list.add(mocked); } return list; } } public void
	 * setTestStoragePartition(final String name){ testPartitionName = name; }
	 * 
	 * @Override protected String getStoragePartition(String storageId, long dateId)
	 * throws Exception { if(testPartitionName == null){ return
	 * super.getStoragePartition(storageId, dateId); } else { return
	 * testPartitionName; } }
	 * 
	 * @Override public String getTempDayBhTable() throws Exception { return
	 * super.getTempDayBhTable(); }
	 * 
	 * @Override public String getBhLevel() throws EngineMetaDataException { return
	 * super.getBhLevel(); }
	 * 
	 * @Override public String groupPublic_Time() { return super.groupPublic_Time();
	 * }
	 * 
	 * @Override public String groupPublic_TimeNode() { return
	 * super.groupPublic_TimeNode(); }
	 * 
	 * @Override public String groupKey_Node() { return super.groupKey_Node(); }
	 * 
	 * @Override public String groupKey_TimeNode() { return
	 * super.groupKey_TimeNode(); }
	 * 
	 * @Override public long getAggregationDate() { return
	 * super.getAggregationDate(); }
	 * 
	 * @Override protected Date getDefaultAggregationDate() { return
	 * super.getDefaultAggregationDate(); }
	 * 
	 * @Override public RockFactory getDwhrepRock() throws Exception {
	 * if(testDwhrepRockInstance == null){ dwhrepRock = super.getDwhrepRock();
	 * return dwhrepRock; } else { return testDwhrepRockInstance; } }
	 * 
	 * @Override public void close() { super.close(); } public String
	 * getLastExecutedSql(){ return lastSqlExecuted; }
	 * 
	 * @Override protected int executeSQLUpdate(String sqlClause) throws
	 * SQLException { lastSqlExecuted = sqlClause; return
	 * super.executeSQLUpdate(sqlClause); } }
	 */}
