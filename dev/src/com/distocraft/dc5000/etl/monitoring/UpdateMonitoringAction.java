package com.distocraft.dc5000.etl.monitoring;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import java.text.ParseException;

import ssc.rockfactory.RockFactory;
import java.util.zip.DataFormatException;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.sql.SQLActionExecute;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.AggregationStatus;
import com.distocraft.dc5000.repository.cache.AggregationStatusCache;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.ericsson.eniq.common.CalcFirstDayOfWeek;
import com.ericsson.eniq.common.Constants;
import com.ericsson.eniq.common.DatabaseConnections;
import com.ericsson.eniq.common.VelocityPool;

/**
 * 
 * <br>
 * <br>
 * <br>
 * <table border="1" width="100%" cellpadding="3" cellspacing="0">
 * <tr bgcolor="#CCCCFF" class="TableHeasingColor">
 * <td colspan="4"><font size="+2"><b>Parameter Summary</b></font></td>
 * </tr>
 * <tr>
 * <td><b>Name</b></td>
 * <td><b>Key</b></td>
 * <td><b>Description</b></td>
 * <td><b>Default</b></td>
 * </tr>
 * <tr>
 * <td>Monitoring lookback time</td>
 * <td>lookback</td>
 * <td>If no other lookBacks are defined this is used in all of them.</td>
 * <td>7</td>
 * </tr>
 * <tr> </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>poplookback</td>
 * <td>How many days back do we populate DIM_DATE,LOG_AGGREGATIONSTATUS and
 * LOG_LOADSTATUS or check for new aggregations or monitored types.</td>
 * <td>same as lookback</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>loadlookback</td>
 * <td>How many days back we add (change status from NOT_LOADED to LOADED)
 * datarows from LOG_SESSION_LOADER to LOG_LOADSTATUS</td>
 * <td>same as lookback</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>agglookback</td>
 * <td>How many days back we look for datarows that are ready for aggregation.</td>
 * <td>same as lookback</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>latedatalookback</td>
 * <td>How many days back we look for LATE_DATA (data rows that have been added
 * to already aggregated day).</td>
 * <td>same as lookback</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>flaglookback</td>
 * <td>How many days back we ignore handled flag in loadStatus. see. ignoreflag</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>Ignore Flag</td>
 * <td>ignoreflag</td>
 * <td>Do we ignore the flag in loadStatus.</td>
 * <td>false</td>
 * </tr>
 * <tr>
 * <td>Aggregation Threshold</td>
 * <td>threshold</td>
 * <td>Defines how long (hours) is waited after last loading before incomplete
 * (=not all datatimes contain data) aggregation is done.</td>
 * <td>1</td>
 * </tr>
 * <tr>
 * <td>Complete Aggregation Threshold</td>
 * <td>completeThreshold</td>
 * <td>Defines how long (hours) is waited after last loading before complete
 * (=all datatimes contain data) aggregation is done.</td>
 * <td>1</td>
 * </tr>
 * </table> <br>
 * <br>
 * 
 * 
 * <br>
 * <b>Tables to write to</b> <br>
 * Update monitoring does DB population and update operations for (writes to)
 * DIM_DATE, LOG_AGGREGATIONSTATUS, LOG_LOADSTATUS and LOG_SESSION_LOADER
 * tables.<br>
 * <br>
 * <b>Tables to read from</b> <br>
 * Update monitoring reads information from LOG_SESSION_LOADER,
 * LOG_AGGREGATIONRULES, LOG_MONITOREDTYPES and LOG_MONITORINGRULES tables.<br>
 * <br>
 * <b>LookBack</b> <br>
 * There are four (4) lookback parameters in the update monitoring (<b><i>poplookback</i>,
 * <i>loadlookback</i>, <i>agglookback</i>, <i>latedatalookback</i></b>) and
 * a <b><i>threshold</b></i> parameter for aggregation.<br>
 * All of the lookback parameters are set to 7 (days) by default. <b><i>threshold</b></i>
 * parameter is set to zero (0) hours.<br>
 * Lookback parameters defines how long back does the update monitoring look
 * (DAYS) when updating monitoring system.<br>
 * <br>
 * Parameter <b><i>poplookback</b></i> is used when populating tables. <i>See
 * <b>Table population</b></i><br>
 * Parameter <b><i>loadlookback</b></i> is used when new loaded data is
 * checked. <i>See <b>Loading</b></i><br>
 * Parameter <b><i>loadlookback</b></i> parameter is used also when cheking
 * holes and rowcounts. <i>See <b>Hole Check</b></i> and <b><i>Row Count
 * Check</b></i><br>
 * Parameter <b></>agglookback</b></i> is used when marking rows ready for
 * aggregation. <i>See <b>Aggregation Check</b></i><br>
 * Parameter <b><i>latedatalookback</b></i> is used when checkin late data.
 * <i>See <b>Late data check</b></i><br>
 * <br>
 * <b>Execution Order</b><br>
 * When update monitoring is started following methods is executed in this
 * order. Table population -> Loading Check -> Hole Check -> Row Count Check ->
 * Clean status flags.<br>
 * <br>
 * <b>Table population</b><br>
 * There are three populated tables in monitoring system DIM_DATE,
 * LOG_AGGREGATIONSTATUS and LOG_LOADSTATUS.<br>
 * Population is done fully once a day. Full population reads all aggregation
 * rules and monitored types and creates new rows for each hour of the day. <br>
 * After the initial populatin only new rules (LOG_AGGREGATIONRULES) or new
 * types (LOG_MONITOREDTYPES) is added to the tables. Newly populated rows
 * status is set to NOT_LOADED.<br>
 * DIM_DATE is populated only once a day . DIM_DATE population adds a new (if
 * this date exists no date is added) date to DIM_DATE table.<br>
 * <br>
 * <b>Loading Check</b><br>
 * There is flag column in LOG_SESSION_LOADER table that defines new data (not
 * read by update monitoring before). If flag is zero (0) this is new datarow
 * and one (1) means an old data row.<br>
 * After everything is done flag is changed to one (1) making it of no interest
 * to update monitoring.<br>
 * After a datarow is read from LOG_SESSION_LOADER monitoring changes the status
 * of the corresponding row (defined by typename and datadate) in LOG_LOADSTATUS
 * from NOT_LOADED to LOADED.<br>
 * <br>
 * <b>Hole Check</b><br>
 * Hole check changes the status column of LOG_SESSION_LOADER for each different
 * typename from NOT_LOADED to HOLE when NOT_LOADED status has LOADED status
 * columns before (older datadate same typename) and after (newer datadate same
 * typename).<br>
 * <br>
 * <b>Row Count Check</b><br>
 * Rowcount limits use LOG_MONITORINGRULES table to check rowcounts for defined
 * typename. If rowcount is too large or too small typenames sttaus is changed
 * to CHECKFAILED.<br>
 * <br>
 * <b>Aggregation Check</b><br>
 * Marks aggregations ready for aggregation (status is changed from NOT_LOADED
 * to LOADED) in LOG_AGGREGATIONSTATUS table. Aggregation check looks first for
 * complete sets of loaded datarows. This means aggregations that have every last
 * piece of data loaded (status is set to loaded in LOG_SESSION_LOADER) for one
 * day. Also non complete aggregations (some data is missing, not every
 * timeslots status in LOG_SESSION_LOADER is marked as LOADED) are marked as
 * ready for aggregation if there is threshold hours since last loading of
 * specific typename.<br>
 * AutomaticAggregation set is used to trigger actual ready (status is LOADED)
 * aggregation sets. See <A
 * HREF="AutomaticAggregationAction.htm">AutomaticAggregation</A> <br>
 * <br>
 * <b>Late data check</b><br>
 * Data is considered to be late when data appears to aggregation that is
 * already aggregated (status is AGGREGATED). When this happens aggregation
 * status is changed to LATE_DATA in LOG_AGGREGATIONSTATUS table. All related
 * aggregations are also changed to LATE_DATA. Relations (dependencies) are
 * defined in LOG_AGGREGATIONRULES table.<br>
 * AutomaticREAggregation set is used to trigger actual late (status is
 * LATE_DATA) aggregation sets. See <A
 * HREF="AutomaticReAggregationAction.htm">AutomaticREAggregation</A> <br>
 * <br>
 * <b>Clean status flags</b><br>
 * Changes LOG_SESSION_LOADER and LOG_SESSION_AGGREGATOR tables flag column from
 * zero (0) to one (1). <br>
 * <br>
 * 
 * @author savinen
 * 
 * 
 */
public class UpdateMonitoringAction extends SQLActionExecute {
	public final static String sonDayAgg = Constants.SONAGG ;
	public final static String son15Agg = Constants.SON15AGG ;
  public static final String SESSIONTYPE = "monitor";

  private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private SimpleDateFormat sdfshort = new SimpleDateFormat("yyyy-MM-dd");

  protected Logger log;

  protected Logger sqlLog;

  protected String where = "";

  private static Vector engines = new Vector(); // VEngine instance cache

  private RockFactory rockFact;
  
  private int firstDayOfWeek = CalcFirstDayOfWeek.calcFirstDayOfWeek();
  
  // is DIM_DATE populated
  private boolean populateDimDate = false;
  boolean ignoreFlag = false;
  
  private final static String RESTOREPARENT = "/var/tmp";
  private final static String RESTOREFLAGNAME = "flag_populateLoadStatus";
  private File restoreFlag;
  
  protected UpdateMonitoringAction() {
  }

  /**
   * Constructor
   * 
   * @param versionNumber
   *          metadata version
   * @param collectionSetId
   *          primary key for collection set
   * @param collectionId
   *          primary key for collection
   * @param transferActionId
   *          primary key for transfer action
   * @param transferBatchId
   *          primary key for transfer batch
   * @param connectId
   *          primary key for database connections
   * @param rockFact
   *          metadata repository connection object
   * @param connectionPool
   *          a pool for database connections in this collection
   * @param trActions
   *          object that holds transfer action information (db contents)
   * 
   */
  public UpdateMonitoringAction(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
      final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact, final ConnectionPool connectionPool,
      final Meta_transfer_actions trActions, final Logger clog) throws Exception {

    super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
        trActions);

    log = Logger.getLogger(clog.getName() + ".UpdateMonitoring");

    sqlLog = Logger.getLogger("sql" + log.getName().substring(4));

    this.where = trActions.getWhere_clause();

    this.rockFact = rockFact;

  }

  /**
   * Executes a SQL procedure
   * 
   */

  public void execute() throws EngineException {

  int thresholdMin = 90;
  int completeThresholdMin = 90;
    int poplookback = 3;
    int loadlookback = 3;
    int agglookback = 3;
    int latedatalookback = 3;
    int flaglookback = 1;
    int lookahead = 2;
    
    final String dateString = getDateString();
    final String timeString = getTimeString();
    final Properties properties = new Properties();

    if (where != null && where.length() > 0) {

      try {

        final ByteArrayInputStream bais = new ByteArrayInputStream(where.getBytes());
        properties.load(bais);
        bais.close();

        final Double dblThresholdMin = Double.parseDouble(properties.getProperty("threshold", "1.5")) * 60;
        final Double dblCompleteThresholdMin = Double.parseDouble(properties.getProperty("completeThreshold", "1.5")) * 60;
        thresholdMin = dblThresholdMin.intValue();
        completeThresholdMin = dblCompleteThresholdMin.intValue();

        final String lookback = properties.getProperty("lookback", "3");
        lookahead = Integer.parseInt(properties.getProperty("lookahead", "2"));
        poplookback = Integer.parseInt(properties.getProperty("populatelookback", lookback));
        loadlookback = Integer.parseInt(properties.getProperty("loadlookback", lookback));
        agglookback = Integer.parseInt(properties.getProperty("aggregatelookback", lookback));
        latedatalookback = Integer.parseInt(properties.getProperty("latedatalookback", lookback));
        ignoreFlag = "true".equalsIgnoreCase(properties.getProperty("ignoreflag", "false"));        
        flaglookback = Integer.parseInt(properties.getProperty("flaglookback", "1"));
        flaglookback = Integer.parseInt(properties.getProperty("flaglookback", "1"));
        
        populateDimDate =     "true".equalsIgnoreCase(properties.getProperty("populateDimDate", "false"));
        
      } catch (Exception e) {
        log.log(Level.SEVERE, "Invalid configuration", e);
        
        throw new EngineException(EngineConstants.CANNOT_EXECUTE,
            new String[] { this.getTrActions().getAction_contents() }, e, this, this.getClass().getName(),
            EngineConstants.ERR_TYPE_EXECUTION);
      }
    }

    try {
    log.info("Started");
    update(lookahead, poplookback, loadlookback, agglookback, latedatalookback, flaglookback, thresholdMin, completeThresholdMin, dateString, timeString);
    log.info("Finished");
    } catch (Exception e) {
    	log.log(Level.SEVERE, "Execution failed" , e);

      throw new EngineException(EngineConstants.CANNOT_EXECUTE,
          new String[] { this.getTrActions().getAction_contents() }, e, this, this.getClass().getName(),
          EngineConstants.ERR_TYPE_EXECUTION);
    }
  }

  protected void update(final int lookahead, final int poplookback, final int loadlookback, final int agglookback, final int latedatalookback, final int flaglookback, final int thresholdMin, final int completeThresholdMin,
  final String dateString, final String timeString) throws Exception {

    log.finest("Populate lookback: " + poplookback);
    log.finest("Load lookback: " + loadlookback);
    log.finest("Aggregate lookback: " + agglookback);
    log.finest("Late Data lookback: " + latedatalookback);
    log.finest("Flag ignore lookback: " + flaglookback);
    
    log.finest("ThresholdMin: " + thresholdMin);
    log.finest("CompleteThresholdMin: " + completeThresholdMin);

   
    
    // current date
    final GregorianCalendar currentTime = new GregorianCalendar();
  //set first day of the week to the value from file
    currentTime.setFirstDayOfWeek(firstDayOfWeek);
    currentTime.setMinimalDaysInFirstWeek(4);

    currentTime.setTime(sdf.parse(timeString.replaceAll("'", "")));

    // today
    final GregorianCalendar today = new GregorianCalendar();
  //set first day of the week to the value from file
    today.setFirstDayOfWeek(firstDayOfWeek);
    today.setMinimalDaysInFirstWeek(4);

    today.set(currentTime.get(GregorianCalendar.YEAR), currentTime.get(GregorianCalendar.MONTH), currentTime
        .get(GregorianCalendar.DATE), 0, 0, 0);

    // create the starting date for the flag ignoring 
    final GregorianCalendar flaglookBackCal = (GregorianCalendar) today.clone();
    flaglookBackCal.add(Calendar.DAY_OF_YEAR, -flaglookback);
    final String flagStartTimeString = "'" + sdf.format(flaglookBackCal.getTime()) + "'";
    log.finest("Flag ignoring StartTimeString: " + flagStartTimeString);

    
    // create the starting date for the loads
    final GregorianCalendar loadlookBackCal = (GregorianCalendar) today.clone();
    loadlookBackCal.add(Calendar.DAY_OF_YEAR, -loadlookback);
    final String loadStartTimeString = "'" + sdf.format(loadlookBackCal.getTime()) + "'";
    log.finest("Load StartTimeString: " + loadStartTimeString);

    // create the starting date for the aggregation
    final GregorianCalendar agglookBackCal = (GregorianCalendar) today.clone();
    agglookBackCal.add(Calendar.DAY_OF_YEAR, -agglookback);
    final String aggregationStartTimeString = "'" + sdf.format(agglookBackCal.getTime()) + "'";
    log.finest("Aggregation StartTimeString: " + aggregationStartTimeString);

    // create the starting date for the latedata
    final GregorianCalendar latelookBackCal = (GregorianCalendar) today.clone();
    latelookBackCal.add(Calendar.DAY_OF_YEAR, -latedatalookback);
    final String latedataStartTimeString = "'" + sdf.format(latelookBackCal.getTime()) + "'";
    log.finest("Latedata StartTimeString: " + latedataStartTimeString);

    // populate monitoring,aggregation and dim_date tables, this is done
    // lookback days backwards

    int lookback = poplookback;
    
	  //populate LoadStatus for 30 days lookback

		restoreFlag = new File(RESTOREPARENT,RESTOREFLAGNAME);
		if(restoreFlag.exists())
		{
			lookback = Integer.parseInt(StaticProperties.getProperty("RESTORE_MONITORING_LOOKBACK","30"));
		}
    while (lookback >= -lookahead) {

      final GregorianCalendar lookBackCal = (GregorianCalendar) today.clone();
      lookBackCal.add(Calendar.DAY_OF_YEAR, -lookback);

      String table = getLoadStatusPartitionName(lookBackCal.getTimeInMillis());

      if (table != null) {
        log
            .fine("Populating monitoring ( partition: " + table + " ) for date: "
                + addDateString(-lookback, dateString));
        populateMonitoringStatus(addDateString(-lookback, dateString), table);
      } else {
        log.fine(" LOG_LoadStatus partition not found for date: " + addDateString(-lookback, dateString));
      }

      table = getAggregationStatusPartitionName(lookBackCal.getTimeInMillis());

      if (table != null) {
        log.fine("Populating aggregation ( partition: " + table + " ) for date: "
            + addDateString(-lookback, dateString));
        populateAggregationStatus(addDateString(-lookback, dateString), table);
      } else {
        log.fine(" LOG_AggregationStatus partition not found for date: " + addDateString(-lookback, dateString));
      }

      if (populateDimDate){ 
        log.fine("Populating DIM_DATE for date: " + addDateString(-lookback, dateString));
        populateDIM_DATE(addDateString(-lookback, dateString), lookBackCal);
      }

      lookback--;
    }
    
    log.fine(restoreFlag.getAbsolutePath()+"\t"+restoreFlag.canWrite()+"\t"+restoreFlag.exists());
    if(restoreFlag.exists())
    {
    	restoreFlag.delete();
    	log.info("Restore flag for monitoring deleted successfully");
    }

    final Iterator iter = getLoadStatusPartitionList(loadlookBackCal.getTimeInMillis(), today.getTimeInMillis()).iterator();
    while (iter.hasNext()) {

      final String table = (String) iter.next();
      log.fine("table: " + table);

      // read new (flag = 0) entries from log_session_loader
      // and update monitoring status
      updateMonitoringStatus(loadStartTimeString, flagStartTimeString, dateString, table);

      // if there is data missing mark a hole
      markHolesInMonitoringStatus(loadStartTimeString, dateString, table);

      // 
      doChecksInMonitoringStatus(loadStartTimeString, dateString, table);

    }

	// For Backup/Restore
    updateMonitoringStatusRestore();

    // aggregation is done agglookback days backwards (not to today)
    lookback = agglookback;
    while (lookback > 0) {

      final GregorianCalendar lookBackCal = (GregorianCalendar) today.clone();
      lookBackCal.add(Calendar.DAY_OF_YEAR, -lookback);

      log.fine("Updating Aggregations for date: " + addDateString(-lookback, dateString));

      // if threshold is exceeded we force the aggregation
      updateAggregationStatus(currentTime, lookBackCal, thresholdMin, completeThresholdMin);

      lookback--;

    }

    // check if new entries (flag = 0) from log_session_loader are late
    // the datadate is allredy been aggregated
    checkLateData(latedataStartTimeString, dateString);

    // change flag from 0 to 1
    flagSessionRows(dateString);

    // change flag from 0 to 1
    flagAggregationRows(dateString);

  }

private void updateAggregationStatus(final GregorianCalendar trueTime, final GregorianCalendar aggdate, final int thresholdMin, final int completeThresholdMin)
      throws Exception {

    final String dateTime = "'" + sdf.format(aggdate.getTime()) + "'";
    final String date = addDateString(0, dateTime);
    final String lastWeek = lastWeek(date);
    final String lastMonth = lastMonth(date);

    final HashSet readyToAggAll = new HashSet();
    final HashSet readyToAggRaw = new HashSet();
    final HashSet readyToAggDay = new HashSet();
    final HashSet readyToAggWeek = new HashSet();
    final HashSet readyToAggMonth = new HashSet();

    // retrieve number of measurements for all meastypes in one day
    // if data type is HOUR we should get 24 etc.
    final Map rawCounts = getMaxRawCount(date);

    
    // retrieve all meastypes from log_session_status table that have all
    // needed measurements loaded
    readyToAggRaw.addAll(getReadyRawAggregations(trueTime, aggdate, date, rawCounts, thresholdMin, completeThresholdMin));
    readyToAggAll.addAll(readyToAggRaw);

    // retrieve all meastypes from log_session_status table that have all
    // needed measurements aggregated
    readyToAggDay.addAll(getReadyDAYAggregations(readyToAggAll, date));
    readyToAggAll.addAll(readyToAggDay);

    readyToAggWeek.addAll(getWEEKAggregations(lastWeek));
    readyToAggAll.addAll(readyToAggWeek);

    readyToAggMonth.addAll(getMONTHAggregations(lastMonth));

    // mark all aggregation that are ready to be aggregated
    updateAggregated(dateTime, readyToAggRaw);
    updateAggregated(dateTime, readyToAggDay);
    updateAggregated(lastWeek, readyToAggWeek);
    updateAggregated(lastMonth, readyToAggMonth);

    //
    doChecksOnAggMonitoring(dateTime);

  }

  private String lastWeek(final String str) throws Exception {
    final GregorianCalendar cal = new GregorianCalendar();
    cal.setFirstDayOfWeek(firstDayOfWeek);
    cal.setMinimalDaysInFirstWeek(4);
    cal.setTime(sdf.parse(str.replaceAll("'", "")));
    cal.add(Calendar.WEEK_OF_YEAR, -1);
    cal.add(Calendar.DAY_OF_MONTH, 1);

    return "'" + sdf.format(cal.getTime()) + "'";
  }

  private String lastMonth(final String str) throws Exception {
    final GregorianCalendar cal = new GregorianCalendar();
    cal.setFirstDayOfWeek(firstDayOfWeek);
    cal.setTime(sdf.parse(str.replaceAll("'", "")));
    cal.add(Calendar.DAY_OF_MONTH, 1);
    cal.add(Calendar.MONTH, -1);
    return "'" + sdf.format(cal.getTime()) + "'";
  }

  protected String getDateString() {

    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String date = null;

    try {

      final Calendar cal = Calendar.getInstance();
      cal.setFirstDayOfWeek(firstDayOfWeek);
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);
      date = "'" + sdf.format(cal.getTime()) + "'";

    } catch (Exception e) {

    }

    return date;

  }

  protected String getTimeString() {

    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String date = null;

    try {

      final Calendar cal = Calendar.getInstance();
      cal.setFirstDayOfWeek(firstDayOfWeek);
      date = "'" + sdf.format(cal.getTime()) + "'";

    } catch (Exception e) {

    }

    return date;

  }

  protected String addDateString(final int incDay, final String dateString) {

    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String date = dateString;

    try {

      final GregorianCalendar cal = new GregorianCalendar();
      cal.setFirstDayOfWeek(firstDayOfWeek);
      cal.setTime(sdf.parse(dateString.replaceAll("'", "")));

      cal.add(Calendar.DAY_OF_MONTH, incDay);

      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);

      date = "'" + sdf.format(cal.getTime()) + "'";

    } catch (Exception e) {
      log.log(Level.INFO, "Date mangle failed", e);
    }

    return date;

  }

  /**
   * 
   * Reads s string to a velocity context and replaces &date tag with datestring
   * 
   * 
   * @param dateString
   * @param timeString
   * @param s
   * @return
   */
  public String convert(final String dateString, final String s) {
    final StringWriter writer = new StringWriter();
    VelocityEngine vengine = null;
    try {

      vengine = VelocityPool.reserveEngine();
      final VelocityContext context = new VelocityContext();
      context.put("date", dateString);
      context.put("time", dateString);
      vengine.evaluate(context, writer, "", s);

    } catch (Exception e) {
        log.log(Level.WARNING, "Error in convert: "+s+" inserted with "+dateString, e);
    } finally {
      VelocityPool.releaseEngine(vengine);
    }

    return writer.toString();
  }

  /**
   * 
   * calculates the weekday id according to the first day of the week defined in
   * StaticProperties.firstDayOfTheWeek. default value is 2 (or Monday).
   * 
   * Weekday is the number of the day from first day of the week (number 1).
   * 
   * @param dayOfTheWeek
   * @return
   * @throws Exception
   */
  private int getWeekDayID(final int dayOfTheWeek) throws Exception {

    if (dayOfTheWeek == firstDayOfWeek)
	  {
      return 1;
	  }
    if (dayOfTheWeek < firstDayOfWeek)
	  {
      return (8 - firstDayOfWeek) + dayOfTheWeek;
	  }
    if (dayOfTheWeek > firstDayOfWeek)
	  {
      return (dayOfTheWeek - firstDayOfWeek) + 1;
	  }
    return 0;
  }

  /**
   * 
   * Populates DIM_DATE table with todays row. Only one row per day.
   * 
   * @param dateString
   * @param today
   * @throws Exception
   */
  public void populateDIM_DATE(final String dateString, final GregorianCalendar today) throws Exception {

    String sqlClause = "select DATE_ID from DIM_DATE where DATE_ID = $date";
    
    Statement stmt = null;
    ResultSet rSet = null;
    try {
    	stmt = this.getConnection().getConnection().createStatement();
    	stmt.getConnection().commit();
    	rSet = stmt.executeQuery(sqlClause);
    	stmt.getConnection().commit();
    	
    	final int secondDayOfWeekend = (firstDayOfWeek > 1)?firstDayOfWeek-1:firstDayOfWeek+6;
    int firstDayOfWeekend = (secondDayOfWeekend == 1)?firstDayOfWeekend=7:secondDayOfWeekend-1;

      if (!rSet.next()) {

      	rSet.close();
      	
        String bDay = "1";

        // Saturday and Sundays are not busines days.
        if (today.get(Calendar.DAY_OF_WEEK) == secondDayOfWeekend || today.get(Calendar.DAY_OF_WEEK) == firstDayOfWeekend)
          bDay = "0";

        sqlClause = "insert into DIM_DATE (DATE_ID, YEAR_ID, MONTH_ID, DAY_ID, WEEK_ID, WEEKDAY_ID, BUSINESSDAY) values ("
            + "$date,"
            + today.get(Calendar.YEAR)
            + ","
            + today.get(Calendar.MONTH)+1
            + ","
            + today.get(Calendar.DAY_OF_MONTH)
            + ","
            + today.get(Calendar.WEEK_OF_YEAR)
            + ","
            + getWeekDayID(today.get(Calendar.DAY_OF_WEEK)) + "," + bDay + ")";

        log.fine("Populate DIM_DATE");
        sqlLog.finest("sql:" + convert(dateString, sqlClause));
        
        stmt.execute(convert(dateString, sqlClause));

      } else {
        log.fine(" DIM_DATE allready populated for date: " + dateString);
      }
        
    } finally {
    	try {
    		rSet.close();
    	} catch (Exception e) {}
    	
    	try {
    		stmt.close();
    	} catch(Exception e) {}
    }
    
  }

  public void populateMonitoringStatus(final String dateString, final String table) throws Exception {
    // TODO create VIEW for dim_time
    // TODO on partiton change population should be done only in one partition
    // Popoulate LOG_LoadStatus table

    if (table != null) {

      // populate monitoring status table
      String sqlClause = "  INSERT INTO " + table + " (typename, timelevel, status, datadate, datatime, DATE_ID, modified)" 
          + " SELECT typename, timelevel, 'NOT_LOADED', $date, DATEADD(mi, min_id, DATEADD(hh, HOUR_ID, DATETIME($date))),"
          + " DATETIME($date), NOW() FROM LOG_MonitoredTypes JOIN ( " 
          + " SELECT '1MIN' tlevel, hour_id, min_id FROM dim_time UNION ALL" 
          + " SELECT '5MIN' tlevel, hour_id, min_id FROM dim_time WHERE mod(min_id,5) = 0 UNION ALL"
          + " SELECT '10MIN' tlevel, hour_id, min_id FROM dim_time WHERE mod(min_id,10) = 0 UNION ALL" 
          + " SELECT '15MIN' tlevel, hour_id, min_id FROM dim_time WHERE min_id IN (0,15,30,45) UNION ALL" 
          + " SELECT '30MIN' tlevel, hour_id, min_id FROM dim_time WHERE min_id IN (0,30) UNION ALL"
          + " SELECT 'HOUR' tlevel, hour_id, min_id FROM dim_time WHERE min_id IN (0) UNION ALL" 
          + " SELECT '6HOUR' tlevel, hour_id, min_id FROM dim_time WHERE min_id = 0 AND hour_id IN (0, 6, 12, 18) UNION ALL"
          + " SELECT '24HOUR' tlevel, hour_id, min_id FROM dim_time WHERE min_id = 0 AND hour_id = 0) datatimes"
          + " ON LOG_MonitoredTypes.timelevel = tlevel WHERE status = 'ACTIVE' AND ACTIVATIONDAY <=  $date"
          + " AND LOG_MonitoredTypes.typename+LOG_MonitoredTypes.timelevel  not  in "
          + "(SELECT  distinct(typename)+timelevel FROM LOG_LoadStatus  WHERE datadate = $date)";

      log.fine("Populate LOG_LoadStatus");
      
      String tempOptionSQL = "SET TEMPORARY OPTION RETURN_DATE_TIME_AS_STRING = 'ON';";
      sqlLog.finest("sql: " + tempOptionSQL);
      executeSQL(tempOptionSQL);
      
      sqlClause = convert(dateString, sqlClause);
      sqlLog.finest("sql:" + sqlClause);
      //executeSQL(sqlClause);
      // added as a part of HN11707 TR fix
      executeSQL(sqlClause,table);
      tempOptionSQL = "SET TEMPORARY OPTION RETURN_DATE_TIME_AS_STRING = 'OFF';";
      sqlLog.finest("sql: " + tempOptionSQL);
      executeSQL(tempOptionSQL);
    }

  }

  protected void updateMonitoringStatus(final String startString, final String flagStartString, final String dateString, final String table) throws Exception {

    // update monitoring status
    String sqlClause ="";

    String tempSqlClause = "select DISTINCT(timelevel) FROM LOG_SESSION_LOADER";

    Statement stmt = null;
    ResultSet rs = null;
    
    try {
    
    stmt = this.getConnection().getConnection().createStatement();
  	stmt.getConnection().commit();
  	rs = stmt.executeQuery(tempSqlClause);

    String timeLevel="";
	  
	  while (rs.next()){

		  timeLevel = rs.getString(1);
		  if(timeLevel.equalsIgnoreCase("24HOUR")){
			  break;
		  }
	  }

	  if(timeLevel.equalsIgnoreCase("24HOUR")){

		  if (ignoreFlag){

			  sqlClause = " UPDATE " + table + " st SET STATUS = 'CALC' FROM ("
					  + " SELECT typename, timelevel, datadate, datatime, status FROM LOG_SESSION_LOADER WHERE datadate > " + flagStartString 
					  + " GROUP BY typename, timelevel,datadate,datatime,status ) ses "
		 			  + " WHERE st.typename = ses.typename  AND st.timelevel = ses.timelevel AND ses.timelevel = '24HOUR' AND st.datadate = ses.datadate "
					  + " AND ses.status = 'OK' "	// For Backup/Restore - to not update restored rows
					  + " AND st.datadate <= $date AND st.datadate > " + startString + " ; ";

		  } else {

			  sqlClause = " UPDATE " + table + " st SET STATUS = 'CALC' FROM ("
					  + " SELECT typename, timelevel, datadate, datatime, status FROM LOG_SESSION_LOADER WHERE flag = 0 "
					  + " GROUP BY typename, timelevel,datadate,datatime,status ) ses "
		  			  + " WHERE st.typename = ses.typename AND st.timelevel = ses.timelevel AND ses.timelevel = '24HOUR' AND st.datadate = ses.datadate"
		  			  + " AND ses.status = 'OK' "	// For Backup/Restore - to not update restored rows
		  			  + " AND st.datadate <= $date AND st.datadate > " + startString + " ; ";
		  }
		  log.fine("update LoadStatus to CALC in partition " + table);
		  sqlLog.finest("sql:" + convert(dateString, sqlClause));
		  //executeSQLUpdate(convert(dateString, sqlClause));
		  executeSQLUpdate(convert(dateString, sqlClause),table);
	  }

    if (ignoreFlag){
      
      sqlClause = " UPDATE " + table + " st SET STATUS = 'CALC' FROM ("
      + " SELECT typename, timelevel, datadate, datatime, status FROM LOG_SESSION_LOADER WHERE datadate > " + flagStartString 
      + " GROUP BY typename, timelevel,datadate,datatime, status ) ses "
      + " WHERE st.typename = ses.typename AND st.timelevel = ses.timelevel AND st.datatime = ses.datatime "
      + " AND ses.status = 'OK' "	// For Backup/Restore - to not update restored rows
      + " AND st.datadate <= $date AND st.datadate > " + startString + " ; ";

    } else {
    
     sqlClause = " UPDATE " + table + " st SET STATUS = 'CALC' FROM ("
        + " SELECT typename, timelevel, datadate, datatime, status FROM LOG_SESSION_LOADER WHERE flag = 0 "
        + " GROUP BY typename, timelevel,datadate,datatime, status ) ses "
        + " WHERE st.typename = ses.typename AND st.timelevel = ses.timelevel AND st.datatime = ses.datatime "
        + " AND ses.status = 'OK' "	// For Backup/Restore - to not update restored rows
        + " AND st.datadate <= $date AND st.datadate > " + startString + " ; ";
    }
    
    log.fine("update LoadStatus to CALC in partition " + table);
    sqlLog.finest("sql:" + convert(dateString, sqlClause));
    //executeSQLUpdate(convert(dateString, sqlClause));
    executeSQLUpdate(convert(dateString, sqlClause),table);

    if(timeLevel.equalsIgnoreCase("24HOUR")){
		  sqlClause = " UPDATE " + table + " st SET RowCount = ses1.SumRows, SourceCount = ses1.CntSources, STATUS = 'LOADED', "
				  + " modified = ses1.sesendtime, description = string('Latest loading: ', ses1.sesendtime) FROM ("
				  + " SELECT typename, timelevel, datadate, datatime, SUM(RowCount) SumRows, COUNT(DISTINCT source) CntSources, "
				  + " max(sessionendtime) sesendtime FROM LOG_SESSION_LOADER WHERE "
				  + " datadate IN (SELECT distinct datadate FROM " + table + " where status='CALC') "
				  + " GROUP BY typename,timelevel,datadate,datatime ) ses1 WHERE st.status='CALC' "
				  + " AND st.typename = ses1.typename AND ses1.timelevel = '24HOUR'"
				  + " AND st.datadate = ses1.datadate AND st.timelevel = ses1.timelevel AND st.datadate <= $date "
				  + " AND st.datadate >= " + startString + " ; ";
		  log.fine("update LoadStatus to LOADED in partition " + table);
		  sqlLog.finest("sql:" + convert(dateString, sqlClause));
		  //executeSQLUpdate(convert(dateString, sqlClause));
		  executeSQLUpdate(convert(dateString, sqlClause),table);
  }

    sqlClause = " UPDATE " + table + " st SET RowCount = ses1.SumRows, SourceCount = ses1.CntSources, STATUS = 'LOADED', "
        + " modified = ses1.sesendtime, description = string('Latest loading: ', ses1.sesendtime) FROM ("
        + " SELECT typename, timelevel, datadate, datatime, SUM(RowCount) SumRows, COUNT(DISTINCT source) CntSources, "
        + " max(sessionendtime) sesendtime FROM LOG_SESSION_LOADER WHERE "
        + " datatime IN (SELECT distinct CAST(dateformat(datatime,'yyyy-Mm-Dd HH:nn')||':00.0' AS TIMESTAMP) FROM " + table + " where status='CALC') "
        + " GROUP BY typename,timelevel,datadate,datatime ) ses1 WHERE st.status='CALC' "
        + " AND st.datatime = ses1.datatime AND st.typename = ses1.typename "
        + " AND st.datadate = ses1.datadate AND st.timelevel = ses1.timelevel AND st.datadate <= $date "
        + " AND st.datadate >= " + startString + " ; ";

    log.fine("update LoadStatus to LOADED in partition " + table);
    sqlLog.finest("sql:" + convert(dateString, sqlClause));
    //executeSQLUpdate(convert(dateString, sqlClause));
    executeSQLUpdate(convert(dateString, sqlClause),table);
    } finally {
    	try {
    		rs.close();
    	} catch (Exception e) {}
    	
    	try {
    		stmt.close();
    	} catch(Exception e) {}
    }
    
  }

  protected void checkLateData(final String startTime, final String dateString) throws Exception {
  
    // check for day data.
    final String sqlClause = "SELECT ruli.source_type, ruli.source_level, dateformat(sesi.datadate,'yyyy-mm-dd') datadate "
        + "FROM LOG_SESSION_LOADER sesi, LOG_AggregationRules ruli, LOG_AggregationStatus aggi "
        + "WHERE sesi.flag = 0 AND sesi.typename = ruli.source_type AND ruli.aggregation = aggi.aggregation AND "
        + "aggi.datadate = sesi.datadate AND sesi.datadate <= $date AND sesi.datadate > " + startTime
        + " AND aggi.status IN ('AGGREGATED','MANUAL','ERROR','CHECKFAILED','FAILEDDEPENDENCY') AND "
        + "aggi.aggregationscope = 'DAY' AND ruli.aggregationscope = 'DAY' "
        + "GROUP BY ruli.source_type, ruli.source_level, sesi.datadate ";

    log.fine("check for late data DAY from " + startTime + " to " + dateString);
    sqlLog.finest("Unparsed sql: " + sqlClause);
    
    Statement stmt = null;
    ResultSet rSet = null;
    
    try {
    	stmt = this.getConnection().getConnection().createStatement();
    	stmt.getConnection().commit();
    	rSet = stmt.executeQuery(convert(dateString, sqlClause));
    	stmt.getConnection().commit();
    	
      while (rSet.next()) {

        final String typename = (String) rSet.getString("source_type");
        final String timelevel = (String) rSet.getString("source_level");
        final String datadate = (String) rSet.getString("datadate") + " 00:00:00";
        
        final ReAggregation reag = new ReAggregation(log, rockFact);
        reag.reAggregate("LATE_DATA", typename, timelevel,datadate);
      }

    } finally {
    	try {
    		rSet.close();
    	} catch(Exception e) {}
    	
    	try {
    		stmt.close();
    	} catch (Exception e) {}
    }
    
  }

  protected void markHolesInMonitoringStatus(final String startString, final String dateString, final String table) throws Exception {

    // update LOG_LoadStatus status

    // Mark holes
    final String sqlClause = "   UPDATE " + table + " st SET status = 'HOLE', modified = NOW() FROM (" 
        + " SELECT typename, timelevel, MAX(datatime) maxtime, MIN(datatime) mintime FROM LOG_LoadStatus " 
        + " WHERE datadate <= $date AND datadate > " + startString +" and status = 'LOADED'"
        + " GROUP BY typename, timelevel) ses "
        + " WHERE st.typename = ses.typename AND st.timelevel = ses.timelevel "
        + " AND st.datatime < ses.maxtime AND st.datatime > ses.mintime AND st.datadate <= $date AND status = 'NOT_LOADED'"
        + " AND st.timelevel IN ('1MIN', '5MIN', '10MIN', '15MIN', '30MIN', 'HOUR', '6HOUR', '24HOUR');";

    log.fine("Mark holes in " + table);
    sqlLog.finest("sql:" + convert(dateString, sqlClause));
    //executeSQLUpdate(convert(dateString, sqlClause));
    executeSQLUpdate(convert(dateString, sqlClause),table);
    
    //Code Part for TR HP88452
    //======================================================================================================
    String sqlClause1= " UPDATE " + table + " st SET RowCount = ses1.SumRows, SourceCount = ses1.CntSources, STATUS = 'LOADED'," 
              + " modified = ses1.sesendtime, description = string('Latest loading: ', ses1.sesendtime) FROM (" 
              + " SELECT typename, timelevel, datadate, datatime, SUM(RowCount) SumRows, COUNT(DISTINCT source) CntSources, " 
              + " max(sessionendtime) sesendtime FROM LOG_SESSION_LOADER WHERE " 
              + " dateformat(datatime,'yyyy-Mm-Dd HH:nn') IN ( "
              + " SELECT distinct dateformat(datatime,'yyyy-Mm-Dd HH:nn') AS TIMESTAMP "
              + " FROM " + table + " where status='HOLE' "
              + " ) GROUP BY typename,timelevel,datadate,datatime " 
              + " ) ses1 WHERE st.status='HOLE' "
              + " AND dateformat(st.datatime,'yyyy-Mm-Dd HH:nn') = dateformat(ses1.datatime ,'yyyy-Mm-Dd HH:nn') "
              + " AND st.typename = ses1.typename "
              + " AND st.datadate = ses1.datadate AND st.timelevel = ses1.timelevel AND st.datadate <= $date " 
              + " AND st.datadate >= " + startString + " ; ";
        
    //log.fine("Change HOLE SQL :: " + convert(dateString, sqlClause1)); 
    sqlLog.finest("sql:" + convert(dateString, sqlClause1));
        
    int rowsaffected = executeSQLUpdate(convert(dateString, sqlClause1),table);
    log.fine("Change HOLE in " + table +" , "+ rowsaffected + " rows affected");
  }

  protected void flagSessionRows(final String dateString) throws Exception {

    final Iterator iter = getSessionLoaderPartitionSet(0, sdf.parse(dateString.replaceAll("'", "")).getTime()).iterator();

    while (iter.hasNext()) {

      final String table = (String) iter.next();
      final String sqlClause = "   UPDATE " + table + " SET flag = 1 WHERE flag = 0 and DATADATE <= $date ; ";

      log.fine("flag session rows in " + table);
      sqlLog.finest("sql:" + convert(dateString, sqlClause));
      //executeSQLUpdate(convert(dateString, sqlClause));
      executeSQLUpdate(convert(dateString, sqlClause),table);
    }
  }

  protected void flagAggregationRows(final String dateString) throws Exception {

    final Iterator iter = getSessionAggregationPartitionSet(0, sdf.parse(dateString.replaceAll("'", "")).getTime())
        .iterator();

    while (iter.hasNext()) {

      final String table = (String) iter.next();
      final String sqlClause = "   UPDATE " + table + " SET flag = 1 WHERE flag = 0 and DATADATE <= $date ; ";

      log.fine("flag session rows in " + table);
      sqlLog.finest("sql:" + convert(dateString, sqlClause));
      //executeSQLUpdate(convert(dateString, sqlClause));
      executeSQLUpdate(convert(dateString, sqlClause),table);
    }

  }

  protected void doChecksInMonitoringStatus(final String startString, final String dateString, final String table) throws Exception {

    // MINROW check
    String sqlClause = "UPDATE " + table + " st SET status = 'CHECKFAILED',"
        + " description = 'MINROW: ' || CONVERT(VARCHAR(12), rowcount) || '<' || CONVERT(VARCHAR(12), threshold)"
        + " FROM LOG_MonitoringRules rules WHERE st.typename = rules.typename"
        + " AND st.timelevel = rules.timelevel AND st.datadate <= $date  AND st.datadate > " + startString
        + " AND rules.rulename = 'MINROW' AND st.rowcount < rules.threshold"
        + " AND st.status = 'LOADED' AND rules.status = 'ACTIVE';";
    log.fine("MINROW check in  " + table + "");
    sqlLog.finest("sql:" + convert(dateString, sqlClause));
  //  executeSQLUpdate(convert(dateString, sqlClause));
    executeSQLUpdate(convert(dateString, sqlClause),table);

    // MAXROW check
    sqlClause = "UPDATE " + table + " st SET status = 'CHECKFAILED',"
        + " description = 'MAXROW: ' || CONVERT(VARCHAR(12), rowcount) || '>' || CONVERT(VARCHAR(12), threshold)"
        + " FROM LOG_MonitoringRules rules WHERE st.typename = rules.typename"
        + " AND st.timelevel = rules.timelevel AND st.datadate <= $date AND st.datadate > " + startString
        + " AND rules.rulename = 'MAXROW' AND st.rowcount > rules.threshold"
        + " AND st.status = 'LOADED' AND rules.status = 'ACTIVE';";
    log.fine("MAXROW check in  " + table + "");
    sqlLog.finest("sql:" + convert(dateString, sqlClause));
    //executeSQLUpdate(convert(dateString, sqlClause));
    executeSQLUpdate(convert(dateString, sqlClause),table);

    // MINSOURCE check
    sqlClause = "    UPDATE " + table + " st SET status = 'CHECKFAILED',"
        + " description = 'MINSOURCE: ' || CONVERT(VARCHAR(12), sourcecount) || '<' || CONVERT(VARCHAR(12), threshold)"
        + " FROM LOG_MonitoringRules rules WHERE st.typename = rules.typename"
        + " AND st.timelevel = rules.timelevel AND st.datadate <= $date AND st.datadate > " + startString
        + " AND rules.rulename = 'MINSOURCE' AND st.sourcecount < rules.threshold"
        + " AND st.status = 'LOADED' AND rules.status = 'ACTIVE';";
    log.fine("MINSOURCE check in  " + table + "");
    sqlLog.finest("sql:" + convert(dateString, sqlClause));
    //executeSQLUpdate(convert(dateString, sqlClause));
    executeSQLUpdate(convert(dateString, sqlClause),table);
    // MAXSOURCE check
    sqlClause = "    UPDATE " + table + " st SET status = 'CHECKFAILED',"
        + " description = 'MAXSOURCE: ' || CONVERT(VARCHAR(12), sourcecount) || '>' || CONVERT(VARCHAR(12), threshold)"
        + " FROM LOG_MonitoringRules rules WHERE st.typename = rules.typename"
        + " AND st.timelevel = rules.timelevel AND st.datadate <= $date AND st.datadate > " + startString
        + " AND rules.rulename = 'MAXSOURCE' AND st.sourcecount > rules.threshold"
        + " AND st.status = 'LOADED' AND rules.status = 'ACTIVE';";
    log.fine("MAXSOURCE check in  " + table + "");
    sqlLog.finest("sql:" + convert(dateString, sqlClause));
    //executeSQLUpdate(convert(dateString, sqlClause));
    executeSQLUpdate(convert(dateString, sqlClause),table);
  }

  protected void populateAggregationStatus(final String dateString, final String table) throws Exception {

    if (table != null) {

      log.fine("Check if LOG_AggregationStatus is populated");
      String group = "'DAY'";

      // first day of the week
      if (firstDayOfTheWeek(dateString)) {

        group += ",'WEEK'";

      }

      // first day of the MONTH
      if (firstDayOfTheMonth(dateString)) {

        group += ",'MONTH'";

      }
      
      group += ",'" + Constants.ROPAGGSCOPE + "'";

      // populate aggregation status table
      final String sqlClause = " INSERT INTO " + table + " (aggregation, typename, timelevel, status, datadate, DATE_ID, aggregationscope)" 
          + " SELECT DISTINCT aggregation, target_type, target_level, 'NOT_LOADED', $date,$date, aggregationscope " 
          + " FROM LOG_AggregationRules WHERE aggregationscope IN (" + group + ")"
          + " AND aggregation NOT IN (SELECT aggregation FROM " + table + " WHERE DATADATE = $date GROUP BY aggregation )";

      log.fine("Populate LOG_AggregationStatus using query: " + convert(dateString, sqlClause));
      sqlLog.finest("sql:" + convert(dateString, sqlClause));
      AggregationStatusCache.update(convert(dateString, sqlClause));

    }

  }

  private boolean firstDayOfTheWeek(final String dateString) throws Exception {

    final Date date = sdfshort.parse(dateString.replaceAll("'", ""));
    final GregorianCalendar cal = new GregorianCalendar();
    cal.setFirstDayOfWeek(firstDayOfWeek);
    cal.setTime(date);

    if (cal.get(Calendar.DAY_OF_WEEK) == firstDayOfWeek) {
       log.fine("First Day of the week is: " + firstDayOfWeek );
      return true;
    }

    return false;
  }

  private boolean firstDayOfTheMonth(final String dateString) throws Exception {

    final Date date = sdfshort.parse(dateString.replaceAll("'", ""));
    final GregorianCalendar cal = new GregorianCalendar();
    cal.setFirstDayOfWeek(firstDayOfWeek);
    cal.setTime(date);

    if (cal.get(Calendar.DAY_OF_MONTH) == cal.getMinimum(Calendar.DAY_OF_MONTH)) {
      return true;
    }

    return false;
  }

  protected Map getMaxRawCount(final String dateString) throws Exception {

    // calculates max values to map..
    final String sqlClause = "SELECT agg.aggregation,agg.source_type, count(*) as _count "
        + "FROM LOG_LoadStatus sta, LOG_AggregationRules agg WHERE sta.datadate = $date "
        + "AND sta.typename = agg.source_type AND agg.ruletype IN (" + "'" + sonDayAgg + "', " + "'" + son15Agg + "','total','count','rankbh')  "
        + "GROUP BY agg.aggregation,agg.source_type; ";
    log.fine("get Max Raw Count");
    sqlLog.finest("Unparsed sql: " + sqlClause);

  	final Map maxValueMap = new HashMap();
    
    Statement stmt = null;
    ResultSet rSet = null;
    
    try {
    	stmt = this.getConnection().getConnection().createStatement();
    	stmt.getConnection().commit();
    	rSet = stmt.executeQuery(convert(dateString, sqlClause));
    	stmt.getConnection().commit();
    	
    int maxValue = -1;

      while (rSet.next()) {

        maxValue = (int) rSet.getInt("_count");
        final String typename = (String) rSet.getString("source_type");
        maxValueMap.put(typename, new Integer(maxValue));

      }

    } finally {
    	try {
    		rSet.close();
    	} catch (Exception e) {}
    	
    	try {
    		stmt.close();
    	} catch (Exception e) {}
    }

    return maxValueMap;
  }

  protected HashSet getReadyRawAggregations(final GregorianCalendar currentTime, final GregorianCalendar aggTime, final String dateString,
  final Map rawCounts, final int thresholdMin, final int completeThresholdMin) throws Exception {

    // counts number of loaded raw types to every meastype
    final String sqlClause = "SELECT agg.aggregation, agg.source_type, max(sta.modified) latestLoading, count(*) as _count "
        + "FROM LOG_LoadStatus sta, LOG_AggregationRules agg where sta.datadate = $date "
        + "AND sta.status IN ('loaded','checkfailed') AND sta.typename = agg.source_type "
        + "AND agg.ruletype IN (" + "'" + sonDayAgg + "', " + "'" + son15Agg + "','total','rankbh','count') GROUP BY agg.aggregation,agg.source_type; ";

    final String setType = StaticProperties.getProperty("AggregationStartTP", "Nosets");
    final HashSet readyToAggregate = new HashSet();
    log.fine("get Ready Raw Aggregations");
    sqlLog.finest("sql:" + convert(dateString, sqlClause));
    
    Statement stmt = null;
    ResultSet rSet = null;
    
    try {
    	stmt = this.getConnection().getConnection().createStatement();
    	stmt.getConnection().commit();
    	rSet = stmt.executeQuery(convert(dateString, sqlClause));
    	stmt.getConnection().commit();
    	
      while (rSet.next()) {

        final int curValue = (int) rSet.getInt("_count");
        final String typename = (String) rSet.getString("source_type");
        final String aggregation = (String) rSet.getString("aggregation");

        final Integer tmp = (Integer)rawCounts.get(typename);       
        if (tmp==null)
          throw new Exception("Error while checking loaded raw types for aggregation "+aggregation+" no source type found "+typename);
        
        final int maxValue = tmp.intValue();
          
        if (aggregation.startsWith(setType))
        {
        	
    		try{
 
    		log.fine("Adding aggregator sets on the basis of static.properties ");
    		final String time = StaticProperties.getProperty("aggStartTime", "");
    		final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss"); 
    		GregorianCalendar aggStartTime = new GregorianCalendar();
    		aggStartTime.setTime(sdf.parse(time));
    	    log.fine("time set in static.properties "+ aggStartTime.get(GregorianCalendar.HOUR_OF_DAY)+":"+aggStartTime.get(GregorianCalendar.MINUTE));
    	    
    	    aggStartTime.set(currentTime.get(GregorianCalendar.YEAR), currentTime.get(GregorianCalendar.MONTH), currentTime
    	        .get(GregorianCalendar.DATE), aggStartTime.get(GregorianCalendar.HOUR_OF_DAY),aggStartTime.get(GregorianCalendar.MINUTE), aggStartTime.get(GregorianCalendar.SECOND));
    		
            GregorianCalendar completeThreshold = new GregorianCalendar();
            completeThreshold.set(currentTime.get(GregorianCalendar.YEAR), currentTime.get(GregorianCalendar.MONTH), currentTime
                .get(GregorianCalendar.DATE), 0, 0, 0);
            int tempCompleteThresholdMin = completeThresholdMin % 60;
            int tempCompleteThresholdHour = completeThresholdMin / 60;
                    
            completeThreshold.add(Calendar.MINUTE, tempCompleteThresholdMin);
            completeThreshold.add(Calendar.HOUR, tempCompleteThresholdHour);
        
    		
        	if (aggStartTime.before(currentTime)&& !completeThreshold.after(currentTime)){
        	readyToAggregate.add(aggregation);
            log.info("Adding readyToAggregate set"+aggregation);
        	}
        	    }
    		catch(ParseException e){
    					log.info(" time set in static.properties, is not in correct format  "+e);}
    		catch(Exception e){
    			      log.info("Adding readyToAggregate set failed with."+e);
    		}
        }	 
        
        // threshold
        final GregorianCalendar threshold = new GregorianCalendar();
        threshold.setFirstDayOfWeek(firstDayOfWeek);
        threshold.setTime(rSet.getTimestamp("latestLoading"));
        
        final int tempThresholdMin = thresholdMin % 60;
        final int tempThresholdHour = thresholdMin / 60;
        threshold.add(Calendar.MINUTE, tempThresholdMin);
        threshold.add(Calendar.HOUR, tempThresholdHour);
  
        // completeThreshold
        final GregorianCalendar completeThreshold = new GregorianCalendar();
        completeThreshold.setFirstDayOfWeek(firstDayOfWeek);
        completeThreshold.setTime(rSet.getTimestamp("latestLoading"));
        final int tempCompleteThresholdMin = completeThresholdMin % 60;
        final int tempCompleteThresholdHour = completeThresholdMin / 60;
                
        completeThreshold.add(Calendar.MINUTE, tempCompleteThresholdMin);
        completeThreshold.add(Calendar.HOUR, tempCompleteThresholdHour);

        // ready to aggregate if:
        // 1) everything is loaded (full set) and it has been completeThreshold number of hours since last loading
        // 2) it has been threshold number of hours since last loading
        
        if ((curValue == maxValue && !completeThreshold.after(currentTime)) || (curValue != maxValue && !threshold.after(currentTime))) {
          readyToAggregate.add(aggregation);
        }
      }

    } finally {
    	try {
    		rSet.close();
    	} catch (Exception e) {}
    	
    	try {
    		stmt.close();
    	} catch (Exception e) {}
    }
      
    return readyToAggregate;

  }

  protected void updateAggregated(final String dateString, final HashSet readyToAggregate) throws Exception {

    // Mark types ready for aggregation
    final long longDate = sdf.parse(dateString.replaceAll("'", "")).getTime();

    final Iterator iter = readyToAggregate.iterator();

    while (iter.hasNext()) {

      final String aggregation = (String) iter.next();

      final AggregationStatus aggSta = AggregationStatusCache.getStatus(aggregation, longDate);

      if (aggSta != null) {

        if (aggSta.STATUS.equalsIgnoreCase("NOT_LOADED")) {

          aggSta.STATUS = "LOADED";
          AggregationStatusCache.setStatus(aggSta);

        }

      }
    }

  }

  protected HashSet getReadyDAYAggregations(final HashSet toBeAggregated, final String dateString) throws Exception {

    final HashMap loaded = new HashMap();
    final HashSet result = new HashSet();

    final String sqlClause = "SELECT rul.aggregation,  rul1.aggregation needed FROM "
        + "LOG_AggregationStatus sta, LOG_AggregationRules rul, LOG_AggregationRules rul1 "
        + "WHERE sta.status = 'not_loaded' AND rul1.target_table = rul.source_table "
        + "AND rul1.AGGREGATIONSCOPE = 'DAY' AND rul1.ruletype IN (" + "'" + sonDayAgg + "', " + "'" + son15Agg + "','TOTAL','COUNT','BHSRC','RANKSRC','RANKBH') "
        + "AND rul1.aggregation <> rul.aggregation AND rul.source_type = sta.typename "
        + "AND sta.datadate = $date AND rul.ruletype IN ('BHSRC','RANKSRC') GROUP BY rul.aggregation,needed";

    log.fine("get aggregation candidates");
    sqlLog.finest("Unparsed sql:" + sqlClause);
    
    Statement stmt = null;
    ResultSet rSet = null;

    try {
    	stmt = this.getConnection().getConnection().createStatement();
    	stmt.getConnection().commit();
    	rSet = stmt.executeQuery(convert(dateString, sqlClause));
    	stmt.getConnection().commit();

      while (rSet.next()) {
        final String aggregation = (String) rSet.getString("aggregation");
        String altered_aggregation = aggregation;
        final String needed = (String) rSet.getString("needed");
        String altered_needed = needed;
        
        // lets remove last two numerics from aggregation name
        Pattern p = null;
        p = Pattern.compile(".+_\\d{2}$");
        Matcher m = p.matcher(aggregation);
        if (m.matches())
          altered_aggregation = aggregation.substring(0, aggregation.length() - 3);
 
        // lets remove last two numerics from needed name
        p = null;
        p = Pattern.compile(".+_\\d{2}$");
        m = p.matcher(needed);
        if (m.matches())
          altered_needed = needed.substring(0, needed.length() - 3);

        // if aggregation and needed is the same skip, this is mostly done in SQL
        if (!altered_needed.equals(altered_aggregation)) {
          
          if (loaded.containsKey(aggregation)) {

            final ArrayList tmp = (ArrayList) loaded.get(aggregation);
            tmp.add(needed);

          } else {
            final ArrayList tmp = new ArrayList();
            tmp.add(needed);
            loaded.put(aggregation, tmp);
          } 
        }      
      }

      final Iterator iter = loaded.keySet().iterator();
      while (iter.hasNext()) {

        final String key = (String) iter.next();
        final ArrayList tmp = (ArrayList) loaded.get(key);

        final Iterator iter2 = toBeAggregated.iterator();
        int count = 0;

        while (iter2.hasNext())
          if (tmp.contains(iter2.next()))
            count++;

        if (count == tmp.size())
          result.add(key);

      }

    } finally  {
    	try {
    		rSet.close();
    	} catch (Exception e) {}

    	try {
    		stmt.close();
    	} catch (Exception e) {}
    } 

    return result;
  }

  private HashSet getWEEKAggregations(final String dateString) throws Exception {

    final HashSet result = new HashSet();

    final String sqlClause = "SELECT sta.aggregation FROM LOG_AggregationStatus sta WHERE sta.status = 'NOT_LOADED'  "
        + "AND sta.datadate = $date AND sta.AGGREGATIONSCOPE = 'WEEK' GROUP BY sta.aggregation ";

    log.fine("get aggregation candidates");
    sqlLog.finest("Unparsed sql:" + sqlClause);
    
    Statement stmt = null;
    ResultSet rSet = null;

    try {
    	stmt = this.getConnection().getConnection().createStatement();
    	stmt.getConnection().commit();
    	rSet = stmt.executeQuery(convert(dateString, sqlClause));
    	stmt.getConnection().commit();
    	
      while (rSet.next()) {

        final String aggregation = (String) rSet.getString("aggregation");

        result.add(aggregation);

      }
      
    } finally {
    	try {
    		rSet.close();
    	} catch (Exception e) {}
    	
    	try {
    		stmt.close();
    	} catch (Exception e) {}
    }

    return result;
  }

  private HashSet getMONTHAggregations(final String dateString) throws Exception {

    final HashSet result = new HashSet();

    final String sqlClause = "SELECT sta.aggregation FROM LOG_AggregationStatus sta WHERE sta.status = 'NOT_LOADED' "
        + "AND sta.datadate = $date AND sta.AGGREGATIONSCOPE = 'MONTH' GROUP BY sta.aggregation ";

    log.fine("get aggregation candidates");
    sqlLog.finest("Unparsed sql:" + sqlClause);
    
    Statement stmt = null;
    ResultSet rSet = null;

    try {
    	stmt = this.getConnection().getConnection().createStatement();
    	stmt.getConnection().commit();
    	rSet = stmt.executeQuery(convert(dateString, sqlClause));
    	stmt.getConnection().commit();

      while (rSet.next()) {

        final String aggregation = (String) rSet.getString("aggregation");

        result.add(aggregation);

      }
      
    } finally {
    	try {
    		rSet.close();
    	} catch (Exception e) {}
    	
    	try {
    		stmt.close();
    	} catch (Exception e) {}
    }
    	
    return result;
  }

  protected void doChecksOnAggMonitoring(final String dateString) throws Exception {

    final long longDate = sdf.parse(dateString.replaceAll("'", "")).getTime();

    // MINROW check
    String sqlClause = "SELECT rules.threshold, rowcount, aggregation "
        + " FROM LOG_MonitoringRules rules, LOG_AggregationStatus st WHERE st.typename = rules.typename"
        + " AND st.timelevel = rules.timelevel AND st.datadate = $date"
        + " AND rules.rulename = 'MINROW' AND st.rowcount < rules.threshold"
        + " AND st.status = 'LOADED' AND rules.status = 'ACTIVE';";

    log.fine("MINROW check");
    sqlLog.finest("Unparsed sql:" + sqlClause);
    
    Statement stmt = null;    
    ResultSet rSet = null;
    
    try {
    	stmt = this.getConnection().getConnection().createStatement();
    	stmt.getConnection().commit();
    	rSet = stmt.executeQuery(convert(dateString, sqlClause));
    	stmt.getConnection().commit();
    	
      while (rSet.next()) {

        final String aggregation = (String) rSet.getString("aggregation");
        final String rowcount = (String) rSet.getString("rowcount");
        final String threshold = (String) rSet.getString("threshold");

        final AggregationStatus aggSta = AggregationStatusCache.getStatus(aggregation, longDate);

        if (aggSta != null) {
          aggSta.STATUS = "CHECKFAILED";
          aggSta.DESCRIPTION = "MINROW: " + rowcount + " < " + threshold;
        }

        AggregationStatusCache.setStatus(aggSta);
      }

      rSet.close();
      
    // MAXROW check
    sqlClause = "SELECT rules.threshold, rowcount FROM LOG_MonitoringRules rules, LOG_AggregationStatus st"
        + " WHERE st.typename = rules.typename AND st.timelevel = rules.timelevel"
        + " AND st.datadate = $date AND rules.rulename = 'MAXROW' AND st.rowcount < rules.threshold"
        + " AND st.status = 'LOADED' AND rules.status = 'ACTIVE';";

    log.fine("MINROW check");
    sqlLog.finest("Unparsed sql:" + sqlClause);
      
      rSet = stmt.executeQuery(convert(dateString, sqlClause));

      while (rSet.next()) {

        final String rowcount = (String) rSet.getString("rowcount");
        final String threshold = (String) rSet.getString("threshold");
        final String aggregation = (String) rSet.getString("aggregation");

        final AggregationStatus aggSta = AggregationStatusCache.getStatus(aggregation, longDate);

        if (aggSta != null) {
          aggSta.STATUS = "CHECKFAILED";
          aggSta.DESCRIPTION = "MAXROW: " + rowcount + " > " + threshold;
        }

        AggregationStatusCache.setStatus(aggSta);
      }

    } finally {
    	try {
    		rSet.close();
    	} catch (Exception e) {}
    	
    	try {
    		stmt.close();
    	} catch(Exception e) {}
    }

  }

  private List getSessionLoaderPartitionSet(final long start, final long end) throws Exception {

    final PhysicalTableCache ptc = PhysicalTableCache.getCache();
    return ptc.getTableName("LOG_SESSION_LOADER:PLAIN", start, end);

  }

  private List getSessionAggregationPartitionSet(final long start, final long end) throws Exception {

    final PhysicalTableCache ptc = PhysicalTableCache.getCache();
    return ptc.getTableName("LOG_SESSION_AGGREGATOR:PLAIN", start, end);

  }

  private String getLoadStatusPartitionName(final long datadate) throws Exception {

    final PhysicalTableCache ptc = PhysicalTableCache.getCache();
    return ptc.getTableName("LOG_LoadStatus:PLAIN", datadate);

  }

  private String getAggregationStatusPartitionName(final long datadate) throws Exception {

    final PhysicalTableCache ptc = PhysicalTableCache.getCache();
    return ptc.getTableName("LOG_AggregationStatus:PLAIN", datadate);

  }

  private List getLoadStatusPartitionList(final long start, final long end) throws Exception {

    final PhysicalTableCache ptc = PhysicalTableCache.getCache();
    return ptc.getTableName("LOG_LoadStatus:PLAIN", start, end);

  }
  
  /**
   * Checks if restore flag is enabled, updates status
   * as 'RESTORED' in LOG_LoadStatus
   * Also updates status of LOG_SESSSION_LOADER restored rows to 'DONE'
   * 
   */
  private void updateMonitoringStatusRestore() {
	  List<String> loadStatusList = new ArrayList<>();
	  List<String> sessionLoaderList = new ArrayList<>();
	  final String sql1 = "Select distinct DATADATE from LOG_SESSION_LOADER where STATUS = 'RESTORED'";
	  Connection con = null;
	  Statement stmt = null;
	  ResultSet rSetDate = null;
	  try {
		  con = DatabaseConnections.getDwhDBConnection().getConnection();
		  stmt = con.createStatement();
		  rSetDate = stmt.executeQuery(sql1);
		  if(rSetDate == null || !rSetDate.next()){
			  log.fine("No restored rows found");
			  return;
		  }
		  Set<String> tempSetLoad = new HashSet<>(); // To remove any duplicate entries
		  Set<String> tempSetSession = new HashSet<>(); // To remove any duplicate entries
		  while(rSetDate.next()){
			  tempSetLoad.add(getLoadStatusPartitionName(sdfshort.parse(rSetDate.getString("DATADATE")).getTime()));
			  tempSetSession.add(getSessionLoaderPartitionName(sdfshort.parse(rSetDate.getString("DATADATE")).getTime()));
		  }
		  loadStatusList.addAll(tempSetLoad);
		  sessionLoaderList.addAll(tempSetSession);
		  log.finest("loadstatuslist - " + loadStatusList);
		  log.finest("sessionLoaderList - " + sessionLoaderList);
		  for (String loadStatus : loadStatusList){
			  updateMonitoringSqlExec(loadStatus);
		  }
		  for (String sessionLoader : sessionLoaderList){
			  updateSessionSqlExec(sessionLoader);
		  }
	  }
	  catch (SQLException e) {
		  log.warning("Could not execute - " + sql1 + " - because " + e);
		  return;
	  }
	  catch (ParseException e) {
		  log.warning("Could not parse date - " + e);
		  return;
	  }
	  catch (Exception e) {
		  log.warning("Exception during restore update - " + e);
		  return;
	  }
	  finally{
		  try {
			  if(rSetDate != null)
				  rSetDate.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		  try {
			  if(stmt != null)
				  stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		  try {
			  if(con != null)
				  con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	  }
  }

  /**
   * Updates LOG_SESSION_LOADER rows already updated to 'DONE' so that they are not picked up in subsequent executions
   * 
   * @param sessionLoaderTable
 ``*/
  private void updateSessionSqlExec(String sessionLoaderTable) {
	  final String sqlClause = " UPDATE " + sessionLoaderTable + " st SET STATUS = 'DONE' FROM ("
		        + " SELECT typename, timelevel, datadate, datatime, status FROM LOG_LoadStatus "
		        + " GROUP BY typename, timelevel,datadate,datatime, status ) ses "
		        + " WHERE st.typename = ses.typename AND st.timelevel = ses.timelevel AND st.datatime = ses.datatime "
		        + " AND ses.status = 'RESTORED' AND st.STATUS != 'DONE' ";
	  try{
		  int rowCount = executeSQLUpdate(sqlClause);
		  log.finer("Rows of Session Loader updated - " + rowCount);
	  }
	  catch (SQLException e) {
		  log.warning("FAILED TO UPDATE STATUS TO DONE! " + e);
	  }
  }

  /**
   * Returns LOG_SESSION_LOADER partition for given date
   * 
   * @param datadate
   * @return
   */
  private String getSessionLoaderPartitionName(long datadate) {
	   final PhysicalTableCache ptc = PhysicalTableCache.getCache();
	   return ptc.getTableName("LOG_SESSION_LOADER:PLAIN", datadate);
  }

   /**
    * Updates LOG_LoadStatus rows to 'RESTORED'
    * 
 	* @param loadStatusTable
 	*/
  private void updateMonitoringSqlExec(String loadStatusTable) {
	  final String sqlClause = "UPDATE " + loadStatusTable + " st SET RowCount = ses1.SumRows, SourceCount = ses1.CntSources, STATUS = 'RESTORED', "
        + " modified = ses1.sesendtime, description = string('Latest loading: ', ses1.sesendtime) FROM ("
        + " SELECT typename, timelevel, datadate, datatime, SUM(RowCount) SumRows, COUNT(DISTINCT source) CntSources, "
        + " max(sessionendtime) sesendtime FROM LOG_SESSION_LOADER WHERE "
        + " status = 'RESTORED' "
        + " GROUP BY typename,timelevel,datadate,datatime ) ses1 WHERE "
        + " st.datatime = ses1.datatime AND st.typename = ses1.typename "
        + " AND st.datadate = ses1.datadate AND st.timelevel = ses1.timelevel ; ";
	  try{
		  int rowCount = executeSQLUpdate(sqlClause);
		  log.finer("Rows of Load Status updated - " + rowCount);
	  }
	  catch (SQLException e) {
		  log.warning("FAILED TO UPDATE STATUS TO RESTORED! " + e);
	  }
  }
}
