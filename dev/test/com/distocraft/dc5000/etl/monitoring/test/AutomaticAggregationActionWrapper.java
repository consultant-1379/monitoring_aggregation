/*
 * Created on Mar 15, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.distocraft.dc5000.etl.monitoring.test;

import java.sql.ResultSet;

import com.distocraft.dc5000.etl.monitoring.AutomaticAggregationAction;
import com.distocraft.dc5000.etl.testHelper.TestHelper;
import java.sql.SQLException;

/**
 * @author savinen
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class AutomaticAggregationActionWrapper extends AutomaticAggregationAction{

	
	private TestHelper helper = null;
	public AutomaticAggregationActionWrapper(TestHelper helper){
						
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
	
	protected void aggregate() throws Exception{
		super.aggregate();
	}
	
}
