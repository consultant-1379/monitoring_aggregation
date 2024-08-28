/*
 * Created on Mar 13, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.distocraft.dc5000.etl.monitoring.test;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.distocraft.dc5000.etl.monitoring.UpdateMonitoringAction;
import com.distocraft.dc5000.etl.testHelper.TestHelper;




/**
 * @author savinen
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
	public class UpdateMonitoringActionWrapper extends UpdateMonitoringAction {
	
	private TestHelper helper = null;
	public UpdateMonitoringActionWrapper(TestHelper helper){
						
		this.helper = helper;		
	}
	
	protected void executeSQL(String sqlClause) throws SQLException {
		
		this.helper.executeSQL( sqlClause);
	}
		
	protected ResultSet executeSQLQuery(String sqlClause) throws Exception {
	
		return this.helper.executeSQLQuery( sqlClause);
	}
	
	
	protected int executeSQLUpdate(String sqlClause) throws SQLException {
		
		return  this.helper.executeSQLUpdate( sqlClause);
	}
	
		
	 protected void update(int lookahead,int poplookback,int loadlookback,int agglookback,int latedatalookback, int flaglookback, int thresholdHour, int compThresholdHour,  String dateString, String timeString) throws Exception {
		 super.update( lookahead, poplookback, loadlookback, agglookback, latedatalookback, flaglookback, thresholdHour, compThresholdHour,  dateString,  timeString);
	 }

	
	public String addDateString(int i,String str){
		return super.addDateString(i,str);
	}
	
	protected void populateAggregationStatus(String dateString, String table) throws Exception {
		super.populateAggregationStatus(dateString,  table);
	}
	
	protected void updateMonitoringStatus(String startString, String flagStartString,String dateString, String table) throws Exception {
		super.updateMonitoringStatus(startString, flagStartString, dateString,  table);
	}
	
	protected void markHolesInMonitoringStatus(String startString,String dateString,String table) throws Exception {
		super.markHolesInMonitoringStatus(startString,dateString, table);
	}
		
	 protected void doChecksInMonitoringStatus(String startString,String dateString, String table) throws Exception {
	 	super.doChecksInMonitoringStatus(startString,dateString,  table);
	 }
			  
		/*
		 * protected Map getMaxRawCount(String dateString) throws Exception { return
		 * super.getMaxRawCount(dateString); }
		 * 
		 * protected HashSet getReadyRawAggregations(GregorianCalendar currentTime,
		 * GregorianCalendar aggTime, String dateString, Map rawCounts,int
		 * thresholdHour, int compThresholdHour) throws Exception { return
		 * super.getReadyRawAggregations( currentTime, aggTime, dateString,
		 * rawCounts,thresholdHour, compThresholdHour); }
		 * 
		 * protected HashSet getReadyDAYAggregations(HashSet toBeAggregated, String
		 * dateString) throws Exception { return super.getReadyDAYAggregations(
		 * toBeAggregated, dateString); }
		 * 
		 * protected void updateAggregated(String dateString, HashSet readyToAggregate)
		 * throws Exception { super.updateAggregated( dateString, readyToAggregate); }
		 */
	 
	  protected void doChecksOnAggMonitoring(String dateString) throws Exception {
	  	super.doChecksOnAggMonitoring(dateString);
	  }
	 
	  protected void checkLateData(String startString,String dateString) throws Exception {
	  	super.checkLateData(startString,dateString);
	  }
	  
	  protected void flagSessionRows(String dateString) throws Exception {
	  	super.flagSessionRows(dateString);
	  }
	 
	  protected void flagAggregationRows(String dateString) throws Exception {
	  	super.flagAggregationRows(dateString);
	  }
	 
	 

    
    
    
}
