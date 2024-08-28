/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2010 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.monitoring;

/*import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.logging.Logger;

import org.jmock.Expectations;
import org.junit.Before;

import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.repository.cache.AggregationStatus;
*/

import org.junit.Test;

import com.distocraft.dc5000.etl.engine.common.EngineException;
/**
 * @author eemecoy
 *
 */
public class AggregationActionTest extends BaseMock {
	AggregationAction testInstance = new AggregationAction();
	
	@Test
	public void testExecute() {
		try {
			testInstance.execute();
		} catch (EngineException e) {
			e.printStackTrace();
		}
	}
	/*
	 * 
	 * private AggregationAction objToTest;
	 * 
	 * Logger mockedLogger;
	 * 
	 * @Before public void setUp() { mockedLogger = context.mock(Logger.class);
	 * allowAnyLogging(); objToTest = new StubbedAggregationAction(); }
	 * 
	 * private void allowAnyLogging() { context.checking(new Expectations() { {
	 * allowing(mockedLogger).fine(with(any(String.class))); } });
	 * 
	 * }
	 * 
	 * @Test public void testUpdateStatusSetsCorrectDescription() throws
	 * EngineException { final int rowCount = 34; final String status =
	 * "some status"; final String oldStatus = ""; objToTest.updateStatus(rowCount,
	 * status, oldStatus); }
	 * 
	 * public void checkAggregationStatusIsAsExpected(final AggregationStatus
	 * aggregationStatus) { assertThat(aggregationStatus.DESCRIPTION,
	 * is("Last Status: ")); }
	 * 
	 * class StubbedAggregationAction extends AggregationAction {
	 * 
	 * public StubbedAggregationAction() { super(); log = mockedLogger; }
	 * 
	 * (non-Javadoc)
	 * 
	 * @see com.distocraft.dc5000.etl.monitoring.AggregationAction#
	 * getStatusFromAggregationStatusCache()
	 * 
	 * @Override AggregationStatus getStatusFromAggregationStatusCache() { return
	 * new AggregationStatus(); }
	 * 
	 * (non-Javadoc)
	 * 
	 * @see com.distocraft.dc5000.etl.monitoring.AggregationAction#
	 * setStatusInAggregationStatusCache(com.distocraft.dc5000.repository.cache.
	 * AggregationStatus)
	 * 
	 * @Override void setStatusInAggregationStatusCache(final AggregationStatus
	 * aggregationStatus) { checkAggregationStatusIsAsExpected(aggregationStatus); }
	 * 
	 * }
	 * 
	 */}
