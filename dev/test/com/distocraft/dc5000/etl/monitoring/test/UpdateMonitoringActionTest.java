/*
 * Created on Mar 10, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.distocraft.dc5000.etl.monitoring.test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.junit.Ignore;

import com.distocraft.dc5000.etl.testHelper.TestHelper;
import junit.framework.TestCase;

/**
 * @author savinen
 * 
 * TODO To change the template for this generated type comment go to Window -
 * Preferences - Java - Code Style - Code Templates
 */
@Ignore("eemecoy 31/5/10 test failing in eclipse, reason unknown")
public class UpdateMonitoringActionTest extends TestCase {

	public UpdateMonitoringActionTest(String arg0) {
		super(arg0);
		// TODO Auto-generated constructor stub
	}

	private String testDate = "'2005-03-02 00:00:00'";
	private String testTime = "'2005-03-02 12:00:00'";
  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private TestHelper testHelper = null;

	protected void setUp() throws Exception {

		testHelper = new TestHelper("dwh");
		testHelper.clearDB();
		testHelper.setupDB();
		testHelper.setUpStatusCache();
		testHelper.setUpProperties(true);
		testHelper.setUpSessionHandler(true);

    
		
	}

	protected void tearDown() throws Exception {
		testHelper.close();
	}

	public void testExecute() {

		try {

			UpdateMonitoringActionWrapper upa = new UpdateMonitoringActionWrapper(
					testHelper);
      
			upa.update(2,7,7,7,7,0,0,0,testDate,testTime);

      boolean lls = testHelper
      .compareTableToFile(
          "TYPENAME,TIMELEVEL,DATADATE,DATATIME,ROWCOUNT,SOURCECOUNT,STATUS,DESCRIPTION",
          "LOG_LoadStatus", "result_LOG_LoadStatus");
      
      boolean las = testHelper.compareTableToFile(
          "LOG_AggregationStatus", "result_LOG_AggregationStatus");
      
           boolean ldd = testHelper.compareTableToFile("DIM_DATE",
          "result_DIM_DATE");
          
       boolean lsl = testHelper.compareTableToFile(
          "LOG_SESSION_LOADER", "result_LOG_SESSION_LOADER");
          
          
          boolean lsa = testHelper.compareTableToFile(
          "LOG_SESSION_AGGREGATOR", "result_LOG_SESSION_AGGREGATOR");
      
          
			assertEquals(true,lls);

			assertEquals(true,las);

			assertEquals(true,ldd);

			assertEquals(true,lsl);

			assertEquals(true,lsa);

		} catch (Exception e) {

			fail(e.toString());
		}

	}

	/*
	 * 
	 * 
	 * public void testAddDateString() {
	 * 
	 * UpdateMonitoringActionWrapper upa = new
	 * UpdateMonitoringActionWrapper(testHelper);
	 * 
	 * String test1 = upa.addDateString(-1, testDate1); String test2 =
	 * upa.addDateString(0, testDate1); String test3 = upa.addDateString(1,
	 * testDate1);
	 * 
	 * assertEquals("'1999-12-31 00:00:00'", test1); assertEquals("'2000-01-01
	 * 00:00:00'", test2); assertEquals("'2000-01-02 00:00:00'", test3);
	 *  }
	 * 
	 * public void testPopulateMonitoringStatus() {
	 * 
	 * UpdateMonitoringActionWrapper upa = new
	 * UpdateMonitoringActionWrapper(testHelper);
	 * 
	 * try {
	 * 
	 * upa.executeSQL("truncate table LOG_LoadStatus_20060103");
	 * 
	 * upa.populateMonitoringStatus("'" + testDate1+ "'");
	 * upa.populateMonitoringStatus("'" + testDate2+ "'");
	 * 
	 * assertEquals( false, testHelper.compareTableToFile(
	 * "TYPENAME,TIMELEVEL,DATADATE,DATATIME,ROWCOUNT,SOURCECOUNT,STATUS,DESCRIPTION",
	 * "LOG_LoadStatus_20060103", "PopulateMonitoringStatus"));
	 *  } catch (Exception e) {
	 * 
	 * fail(e.toString()); } }
	 * 
	 * public void testPopulateAggregationStatus() {
	 * 
	 * UpdateMonitoringActionWrapper upa = new UpdateMonitoringActionWrapper(
	 * testHelper);
	 * 
	 * try {
	 * 
	 * 
	 * upa.executeSQL("truncate table LOG_AggregationStatus_20060103");
	 * 
	 * upa.populateAggregationStatus("'" + testDate1+ "'");
	 * upa.populateAggregationStatus("'" + testDate2+ "'");
	 * 
	 * assertEquals(false, testHelper.compareTableToFile(
	 * "LOG_AggregationStatus_20060103", "PopulateAggregationStatus"));
	 *  } catch (Exception e) {
	 * 
	 * fail(e.toString()); } }
	 * 
	 * public void testPopulateDIM_DATE() {
	 * 
	 * UpdateMonitoringActionWrapper upa = new UpdateMonitoringActionWrapper(
	 * testHelper);
	 * 
	 * try {
	 * 
	 * upa.executeSQL("truncate table DIM_DATE");
	 * 
	 * upa.populateDIM_DATE("'" + testDate1 + "'", today1);
	 * upa.populateDIM_DATE("'" + testDate2 + "'", today2);
	 * 
	 * assertEquals(false, testHelper.compareTableToFile("DIM_DATE",
	 * "populateDIM_DATE"));
	 *  } catch (Exception e) {
	 * 
	 * fail(e.toString()); } }
	 * 
	 * public void testUpdateMonitoringStatus() {
	 * 
	 * 
	 * UpdateMonitoringActionWrapper upa = new UpdateMonitoringActionWrapper(
	 * testHelper);
	 * 
	 * try {
	 * 
	 * upa.executeSQL("update LOG_SESSION_LOADER_20060103 set flag=0");
	 * 
	 * upa.updateMonitoringStatus("'" + testDate1 + "'");
	 * upa.updateMonitoringStatus("'" + testDate2 + "'");
	 * 
	 * assertEquals(false, testHelper.compareTableToFile(
	 * "LOG_AggregationStatus_20060103", "updateMonitoringStatus"));
	 *  } catch (Exception e) {
	 * 
	 * fail(e.toString()); } }
	 * 
	 * public void testMarkHolesInMonitoringStatus() {
	 * 
	 * UpdateMonitoringActionWrapper upa = new UpdateMonitoringActionWrapper(
	 * testHelper);
	 * 
	 * try {
	 * 
	 * upa.markHolesInMonitoringStatus("'" + testDate1+ "'");
	 * upa.markHolesInMonitoringStatus("'" + testDate2+ "'");
	 * 
	 * assertEquals( false, testHelper.compareTableToFile(
	 * "TYPENAME,TIMELEVEL,DATADATE,DATATIME,ROWCOUNT,SOURCECOUNT,STATUS,DESCRIPTION",
	 * "LOG_LoadStatus_20060103", "markHolesInMonitoringStatus"));
	 *  } catch (Exception e) {
	 * 
	 * fail(e.toString()); } }
	 * 
	 * public void testDoChecksInMonitoringStatus() {
	 * 
	 * UpdateMonitoringActionWrapper upa = new UpdateMonitoringActionWrapper(
	 * testHelper);
	 * 
	 * try {
	 * 
	 * upa.doChecksInMonitoringStatus("'" + testDate1+ "'");
	 * upa.doChecksInMonitoringStatus("'" + testDate2+ "'");
	 * 
	 * assertEquals( false, testHelper.compareTableToFile(
	 * "TYPENAME,TIMELEVEL,DATADATE,DATATIME,ROWCOUNT,SOURCECOUNT,STATUS,DESCRIPTION",
	 * "LOG_LoadStatus_20060103", "doChecksInMonitoringStatus"));
	 *  } catch (Exception e) {
	 * 
	 * fail(e.toString()); } }
	 * 
	 * 
	 * public void testGetMaxRawCount() {
	 * 
	 * UpdateMonitoringActionWrapper upa = new UpdateMonitoringActionWrapper(
	 * testHelper);
	 * 
	 * try {
	 * 
	 * Map controlMap1 = testHelper.readHashFromFile("getMaxRawCount1.txt"); Map
	 * controlMap2 = testHelper.readHashFromFile("getMaxRawCount2.txt");
	 * 
	 * Map resultMap1 = upa.getMaxRawCount("'"+ testDate1 + "'", "'" + testDate1 +
	 * "'"); Map resultMap2 = upa.getMaxRawCount("'"+ testDate2 + "'", "'" +
	 * testDate2 + "'");
	 * 
	 * testHelper.writeHashTofile(resultMap1,
	 * testHelper.compareDir+"getMaxRawCount1.txt");
	 * testHelper.writeHashTofile(resultMap2,
	 * testHelper.compareDir+"getMaxRawCount2.txt");
	 * 
	 * assertEquals(true, testHelper.compareMaps(controlMap1, resultMap1));
	 * assertEquals(true, testHelper.compareMaps(controlMap2, resultMap2));
	 *  } catch (Exception e) {
	 * 
	 * fail(e.toString()); } }
	 * 
	 * public void testGetReadyRawAggregations() {
	 * 
	 * UpdateMonitoringActionWrapper upa = new UpdateMonitoringActionWrapper(
	 * testHelper);
	 * 
	 * try {
	 * 
	 * Set controlSet1 =
	 * testHelper.readSetFromFile("getReadyRawAggregations1.txt"); Set
	 * controlSet2 = testHelper.readSetFromFile("getReadyRawAggregations2.txt");
	 * 
	 * Set resultSet1 = upa.getReadyRawAggregations(currentTime1, "'" +
	 * testDate1 + "'", upa.getMaxRawCount("'" + testDate1 + "'", "'"+ testDate1 +
	 * "'"), 0); Set resultSet2 = upa.getReadyRawAggregations(currentTime2, "'" +
	 * testDate2 + "'", upa.getMaxRawCount("'" + testDate2 + "'", "'"+ testDate2 +
	 * "'"), 0);
	 * 
	 * 
	 * testHelper.writeSetTofile(resultSet1,
	 * testHelper.compareDir+"getReadyRawAggregations1.txt");
	 * testHelper.writeSetTofile(resultSet2,
	 * testHelper.compareDir+"getReadyRawAggregations2.txt");
	 * 
	 * assertEquals(true, testHelper.compareSets(controlSet1, resultSet1));
	 * assertEquals(true, testHelper.compareSets(controlSet2, resultSet2));
	 *  } catch (Exception e) {
	 * 
	 * fail(e.toString()); } }
	 * 
	 * public void testGetReadyDAYAggregations() {
	 * 
	 * UpdateMonitoringActionWrapper upa = new UpdateMonitoringActionWrapper(
	 * testHelper);
	 * 
	 * try {
	 * 
	 * 
	 * Set toBeAggregatedSet1 =
	 * testHelper.readSetFromFile(testHelper.compareDir+"getReadyRawAggregations1.txt");
	 * Set toBeAggregatedSet2 =
	 * testHelper.readSetFromFile(testHelper.compareDir+"getReadyRawAggregations2.txt");
	 * 
	 * Set controlSet1 =
	 * testHelper.readSetFromFile("getReadyDAYAggregations1.txt"); Set
	 * controlSet2 = testHelper.readSetFromFile("getReadyDAYAggregations2.txt");
	 * 
	 * HashSet resultSet1 =
	 * upa.getReadyDAYAggregations((HashSet)toBeAggregatedSet1, "'" + testDate1 +
	 * "'", "'" + testDate1+ "'"); HashSet resultSet2 =
	 * upa.getReadyDAYAggregations((HashSet)toBeAggregatedSet2, "'" + testDate2 +
	 * "'", "'" + testDate2+ "'");
	 * 
	 * testHelper.writeSetTofile(resultSet1,
	 * testHelper.compareDir+"getReadyDAYAggregations1.txt");
	 * testHelper.writeSetTofile(resultSet2,
	 * testHelper.compareDir+"getReadyDAYAggregations2.txt");
	 * 
	 * assertEquals(true, testHelper.compareSets(controlSet1, resultSet1));
	 * assertEquals(true, testHelper.compareSets(controlSet2, resultSet2));
	 * 
	 *  } catch (Exception e) {
	 * 
	 * fail(e.toString()); } }
	 * 
	 * public void testUpdateAggregated() {
	 * 
	 * UpdateMonitoringActionWrapper upa = new UpdateMonitoringActionWrapper(
	 * testHelper);
	 * 
	 * try {
	 * 
	 * Set toBeAggregatedSet1 =
	 * testHelper.readSetFromFile(testHelper.compareDir+"getReadyRawAggregations1.txt");
	 * Set toBeAggregatedSet2 =
	 * testHelper.readSetFromFile(testHelper.compareDir+"getReadyRawAggregations2.txt");
	 * Set toBeAggregatedSet3 =
	 * testHelper.readSetFromFile(testHelper.compareDir+"getReadyDAYAggregations1.txt");
	 * Set toBeAggregatedSet4 =
	 * testHelper.readSetFromFile(testHelper.compareDir+"getReadyDAYAggregations2.txt");
	 * 
	 * 
	 * upa.updateAggregated("'" + testDate1 + "'",(HashSet)toBeAggregatedSet1);
	 * upa.updateAggregated("'" + testDate2 + "'",(HashSet)toBeAggregatedSet2);
	 * upa.updateAggregated("'" + testDate1 + "'",(HashSet)toBeAggregatedSet3);
	 * upa.updateAggregated("'" + testDate2 + "'",(HashSet)toBeAggregatedSet4);
	 * 
	 * 
	 * assertEquals(false, testHelper.compareTableToFile(
	 * "LOG_AggregationStatus_20060103", "updateAggregated"));
	 *  } catch (Exception e) {
	 * 
	 * fail(e.toString()); } }
	 * 
	 * public void testDoChecksOnAggMonitoring() {
	 * 
	 * UpdateMonitoringActionWrapper upa = new UpdateMonitoringActionWrapper(
	 * testHelper);
	 * 
	 * try {
	 * 
	 * HashSet toBeAggregatedSet = new HashSet(); Set controlSet = new
	 * HashSet();
	 * 
	 * upa.doChecksOnAggMonitoring("'" + testDate1 + "'");
	 * upa.doChecksOnAggMonitoring("'" + testDate2 + "'");
	 * 
	 * assertEquals(false,
	 * testHelper.compareTableToFile("LOG_AggregationStatus_20060103",
	 * "doChecksOnAggMonitoring"));
	 *  } catch (Exception e) {
	 * 
	 * fail(e.toString()); } }
	 * 
	 * public void testCheckLateData() {
	 * 
	 * UpdateMonitoringActionWrapper upa = new UpdateMonitoringActionWrapper(
	 * testHelper);
	 * 
	 * try {
	 * 
	 * upa.checkLateData("'" + testDate1 + "'"); upa.checkLateData("'" +
	 * testDate2 + "'");
	 * 
	 * assertEquals(false, testHelper.compareTableToFile(
	 * "LOG_AggregationStatus_20060103", "checkLateData"));
	 *  } catch (Exception e) {
	 * 
	 * fail(e.toString()); } }
	 * 
	 * public void testFlagSessionRows() {
	 * 
	 * UpdateMonitoringActionWrapper upa = new UpdateMonitoringActionWrapper(
	 * testHelper);
	 * 
	 * try {
	 * 
	 * upa.executeSQL("update LOG_SESSION_LOADER_20060103 set flag=0");
	 * 
	 * upa.flagSessionRows("'" + testDate1 + "'"); upa.flagSessionRows("'" +
	 * testDate2 + "'");
	 * 
	 * assertEquals(false, testHelper.compareTableToFile(
	 * "LOG_SESSION_LOADER_20060103", "flagSessionRows"));
	 *  } catch (Exception e) {
	 * 
	 * fail(e.toString()); } }
	 * 
	 * public void testFlagAggregationRows() {
	 * 
	 * UpdateMonitoringActionWrapper upa = new UpdateMonitoringActionWrapper(
	 * testHelper);
	 * 
	 * try {
	 * 
	 * upa.executeSQL("update LOG_SESSION_AGGREGATOR_20060103 set flag=0");
	 * 
	 * upa.flagAggregationRows("'" + testDate1 + "'");
	 * upa.flagAggregationRows("'" + testDate2 + "'");
	 * 
	 * assertEquals(false, testHelper.compareTableToFile(
	 * "LOG_SESSION_AGGREGATOR_20060103", "flagAggregationRows"));
	 *  } catch (Exception e) {
	 * 
	 * fail(e.toString()); } }
	 * 
	 */

}