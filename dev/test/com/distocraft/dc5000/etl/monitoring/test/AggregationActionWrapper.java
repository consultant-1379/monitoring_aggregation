/*
 * Created on Mar 15, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.distocraft.dc5000.etl.monitoring.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.monitoring.AggregationAction;
import com.distocraft.dc5000.etl.testHelper.TestHelper;

/**
 * @author savinen
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class AggregationActionWrapper extends AggregationAction{

	private TestHelper helper = null;
	public AggregationActionWrapper(String set,  Long setID, Long batchID, Long connectID, TestHelper helper){
			
		super(set, setID, Logger.getLogger("etlengine.Aggregator"));
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
	
	
	protected void init(String info) throws Exception{
		super.init(info);
	}
	
	
    protected void updateAggLog(long startTime,long endTime, String status, int rowCount)  throws EngineException {
    	super.updateAggLog( startTime, endTime,  status, rowCount);
    }

    protected void updateStatus(int rowCount, String status, String oldStatus) throws EngineException {
    	super.updateStatus( rowCount,  status,  oldStatus);
    }
	
}
