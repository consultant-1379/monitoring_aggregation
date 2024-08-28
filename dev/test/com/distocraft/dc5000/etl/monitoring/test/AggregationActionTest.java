/*
 * Created on Mar 15, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.distocraft.dc5000.etl.monitoring.test;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;

import junit.framework.TestCase;
import org.junit.Ignore;
import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.testHelper.TestHelper;

/**
 * @author savinen
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
@Ignore("eemecoy 31/5/10 test failing in eclipse, reason unknown")
public class AggregationActionTest extends TestCase {

	
	public AggregationActionTest(String arg0) {
		super(arg0);
		// TODO Auto-generated constructor stub
	}


	private String testDate = "'2000-01-01 00:00:00'";

	
	private String testDatestart = "'2000-01-01 00:00:00'";
	private String testDatestop = "'2000-01-01 01:00:00'";
	


	
	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private SimpleDateFormat sdfshort = new SimpleDateFormat("yyyy-MM-dd");

	GregorianCalendar today1;
	GregorianCalendar currentTime1;

	GregorianCalendar today2;
	GregorianCalendar currentTime2;
	
	private TestHelper testHelper = null; 
	
	protected void setUp() throws Exception {
    testHelper = new TestHelper("hsqldb", "testHelper.properties");

//    testHelper.clearDB();
    System.setProperty("test", "C:/Snapshots/eeikbe_view/vobs/eniq/tools/testhelper/jar");
		testHelper.setupDB();
		testHelper.setUpStatusCache();
		testHelper.setUpProperties(true);
		testHelper.setUpSessionHandler(true);


			
			// current date
			currentTime1 = new GregorianCalendar();
			currentTime1.setTime(sdf.parse(testDate.replaceAll("'", "")));

			// today
			today1 = new GregorianCalendar();
			today1.set(currentTime1.get(GregorianCalendar.YEAR), currentTime1
					.get(GregorianCalendar.MONTH), currentTime1
					.get(GregorianCalendar.DATE), 0, 0);


	}

	protected void tearDown() throws Exception {
		testHelper.close();
	}
	
	
    public void testExecute() throws Exception 
    {
   
    	AggregationActionWrapper aaw = new AggregationActionWrapper("DC_E_BSS_LOAS_DAY",  new Long(1), new Long(1), new Long(1),testHelper);
		
    	testHelper.exportDB("*","LOG_AggregationStatus_20060411","Setup_AggregationAction.sql_");    	
    	aaw.executeSQL("truncate table LOG_AggregationStatus_20060411");
    	testHelper.importDB("Setup_AggregationAction.sql");
    	
    	aaw.init("#\n#Fri Sep 16 10:18:00 EEST 2005\n" +
    			"aggregation=DC_E_BSS_LOAS_DAY\n" +
    			"typename=DC_E_BSS_LOAS\n" +
    			"timelevel=HOUR\n" +
    			"status=LOADED\n" +
    			"aggDate="+testDate+"\n");
    	    	
    	aaw.updateStatus( 66,  "OK",  "LOADED");

    	aaw.updateAggLog( sdf.parse(testDatestart).getTime(), sdf.parse(testDatestop).getTime(),  "OK" ,1);

    	assertEquals(true, testHelper.compareFileToFile(new File(StaticProperties.getProperty("SessionHandling.log.AGGREGATOR.inputTableDir")+"AGGREGATOR.unfinished"),new File("AGGREGATOR.unfinished")));
    	   	
    	assertEquals(true, testHelper.compareTableToFile(
				"LOG_AggregationStatus_20060411",
				"Result_AggregationAction"));
    	
    }

	
}
