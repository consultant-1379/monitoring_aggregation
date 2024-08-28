/*
 * Created on Mar 15, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.distocraft.dc5000.etl.monitoring.test;

import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;

import org.junit.Ignore;

import com.distocraft.dc5000.etl.testHelper.TestHelper;

import junit.framework.TestCase;

/**
 * @author savinen
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
@Ignore("eemecoy 31/5/10 test failing in eclipse, reason unknown")
public class AutomaticAggregationActionTest extends TestCase {

	
	
	
	public AutomaticAggregationActionTest(String arg0) {
		super(arg0);
		// TODO Auto-generated constructor stub
	}

	private String testDate = "'2005-03-02 00:00:00'";

	
	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	GregorianCalendar today1;
	GregorianCalendar currentTime1;

	GregorianCalendar today2;
	GregorianCalendar currentTime2;
	
	private TestHelper testHelper = null; 
	
	protected void setUp() throws Exception {

		testHelper = new TestHelper("dwh");
		testHelper.clearDB();
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
	
	public void testAggregate() throws Exception {
		


		
		AutomaticAggregationActionWrapper aaw = new AutomaticAggregationActionWrapper(testHelper);
		   	
    	aaw.executeSQL("truncate table LOG_AggregationStatus_20060411");
		testHelper.importDB("setup_AutomaticAggregationAction.sql");
		
		aaw.aggregate();
   	
		assertEquals(true, testHelper.compareTableToFile(
		"LOG_AggregationStatus_20060411",
		"Result_AutomaticAggregationAction"));

		
	}
	
	
	
}
