package com.distocraft.dc5000.etl.monitoring;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;


import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.SessionHandler;
import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.sql.SQLActionExecute;
import com.distocraft.dc5000.etl.rock.Meta_collection_sets;
import com.distocraft.dc5000.etl.rock.Meta_collection_setsFactory;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.AggregationStatus;
import com.distocraft.dc5000.repository.cache.AggregationStatusCache;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.distocraft.dc5000.repository.dwhrep.Aggregationrule;
import com.distocraft.dc5000.repository.dwhrep.AggregationruleFactory;
import com.distocraft.dc5000.repository.dwhrep.Tpactivation;
import com.distocraft.dc5000.repository.dwhrep.TpactivationFactory;
import com.ericsson.eniq.common.Constants;
import com.ericsson.eniq.common.VelocityPool;
import com.ericsson.etl.monitoring.aggregation.EventDrivenAggregatorNomination;

/**
 * 
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
 * <td>Table Name</td>
 * <td>tablename</td>
 * <td>Defines the name of the aggregation (target table). No two sets with same
 * tablename can be executed (in the execution slots) at the same time.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>Aggregation Clause</td>
 * <td>Action_contents</td>
 * <td>Actual aggregation SQL clause template.</td>
 * <td>&nbsp;</td>
 * </tr>
 * </table>
 * <br>
 * <br>
 * Aggregation action creates and executes the actual aggregation (SQL clause)
 * using aggregation SQL clause template (velocity) and data given from
 * triggering set (
 * <A HREF="AutomaticAggregationAction.htm">AutomaticAggregation</A> or
 * <A HREF="AutomaticRAggregationAction.htm">AutomaticREAggregation</A>).<br>
 * <br>
 * There are 9 objects addded to the context of the template: <b>sessionid,
 * batchid, dateid, targetDerivedTable, sourceDerivedTable, targetTableList,
 * sourceTableList, targetTable and sourceTable.</b><br>
 * <br>
 * <b>sessionid</b><br>
 * Contains the session id of the current aggregation.<br>
 * <br>
 * <b>batchid </b><br>
 * Contains the batch id of the current aggregation.<br>
 * <br>
 * <b>dateid </b><br>
 * Contains the date id of the current aggregation.<br>
 * <br>
 * <b>targetDerivedTable </b><br>
 * Contains all needed target tables (partitions) in a derived table sql clause.
 * <br>
 * <br>
 * <b>sourceDerivedTable </b><br>
 * Contains all needed source tables (partitions) in a derived table sql clause
 * <br>
 * <br>
 * <b>targetTableList </b><br>
 * Contains all needed target tables (partitons) in a list. This list can be
 * iterated in template with foreach.<br>
 * <br>
 * <b>sourceTableList </b><br>
 * Contains all needed source tables (partitons) in a list. This list can be
 * iterated in template with foreach.<br>
 * <br>
 * <b>targetTable </b><br>
 * Contains the singel table (no partitions) from target table column.<br>
 * <br>
 * <b>sourceTable</b><br>
 * Contains the singel table (no partitions) from source table column.<br>
 * <br>
 * 
 * After succesfull aggregation status (in LOG_AGGREGATIONSTATUS) is changed to
 * AGGREGATED.<br>
 * <br>
 * If there was error(s) in aggregation, status is changed to ERROR <br>
 * <br>
 * After aggregation aggregation log is written containing following information
 * on aggregation: SESSION_ID, BATCH_ID, DATE_ID, AGGREGATORSET_ID, SOURCE,
 * TYPENAME, SESSIONSTARTTIME, SESSIONENDTIME, STATUS, ROWCOUNT, DATATIME,
 * DATADATE, TIMELEVEL <br>
 * <br>
 * <b>SESSION_ID</b><br>
 * <br>
 * <b>BATCH_ID</b><br>
 * <br>
 * <b>DATE_ID</b><br>
 * <br>
 * <b>AGGREGATORSET_ID</b><br>
 * <br>
 * <b>SOURCE</b><br>
 * <br>
 * <b>TYPENAME</b><br>
 * <br>
 * <b>SESSIONSTARTTIME</b><br>
 * <br>
 * <b>SESSIONENDTIME</b><br>
 * <br>
 * <b>STATUS</b><br>
 * <br>
 * <b>ROWCOUNT</b><br>
 * <br>
 * <b>DATATIME</b><br>
 * <br>
 * <b>DATADATE</b><br>
 * <br>
 * <b>TIMELEVEL</b><br>
 * <br>
 * <br>
 * <br>
 * <br>
 * 
 */
public class AggregationAction extends SQLActionExecute {

	public static final String SESSIONTYPE = "AGGREGATOR";

	protected String loggerName = "etlengine.AggregationAction";

	protected Logger log;

	protected Logger sqlLog;
	
	protected Logger aggregatorLog;
	
	protected FileInputStream fstream;
	
	protected BufferedReader br;

	protected File f;
	
	protected Properties orig;

	protected String logfile;
	
	final String plogdir = System.getProperty("LOG_DIR");
	
	private String set = "";

	private String typename = "";

	private String aggregation = "";

	private String timelevel = "";

	private String stringDate = "";

	private String aggStatus = "";

	private String aggregationScope = "";

	private Long setID = null;

	private long longDate = 0;
	
	private boolean statFlag = false;

	private RockFactory etlreprock;

	private RockFactory dwhreprock;

	private RockFactory dwhrock;

	String tpName = "";

	private long sessionID;

	Map<String, String> sourceTableMap = null;

	Map<String, String> targetTableMap = null;

	Map<String, String> sourceTableMapForCount = null;

	Map<String, String> targetTableMapForCount = null;

	Map<String, List<Map<String, String>>> sourceTableMapList = null;

	Map<String, List<Map<String, String>>> targetTableMapList = null;

	Map<String, String> sourceTable = new HashMap<String, String>();

	Map<String, String> targetTable = new HashMap<String, String>();

	Map<String, String> sourceType = new HashMap<String, String>();

	Map<String, String> targetType = new HashMap<String, String>();

	Long collectionSetID;

	protected AggregationAction() {
	}

	// for JUnit
	protected AggregationAction(final String set, final Long setID, final Logger clog) {
		orig = new Properties();
		this.set = set;
		this.setID = setID;

		log = Logger.getLogger(clog.getName() + ".Aggregator");
		sqlLog = Logger.getLogger("sql" + log.getName().substring(4));
		aggregatorLog = Logger.getLogger("aggregator."+log.getName().substring(4));
	}

	/**
	 * Constructor
	 * 
	 * @param versionNumber
	 *            metadata version
	 * @param collectionSetId
	 *            primary key for collection set
	 * @param collectionId
	 *            primary key for collection
	 * @param transferActionId
	 *            primary key for transfer action
	 * @param transferBatchId
	 *            primary key for transfer batch
	 * @param connectId
	 *            primary key for database connections
	 * @param rockFact
	 *            metadata repository connection object
	 * @param connectionPool
	 *            a pool for database connections in this collection
	 * @param trActions
	 *            object that holds transfer action information (db contents)
	 * 
	 */

	public AggregationAction(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
			final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
			final ConnectionPool connectionPool, final Meta_transfer_actions trActions, final Logger clog)
					throws Exception {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact,
				connectionPool, trActions);

		log = Logger.getLogger(clog.getName() + ".Aggregator");
		sqlLog = Logger.getLogger("sql" + log.getName().substring(4));
		aggregatorLog = Logger.getLogger("aggregator."+log.getName().substring(4));
		
		orig = new Properties();
		this.set = collection.getCollection_name();
		this.setID = collection.getCollection_id();
		this.collectionSetID = collectionSetId;
		log.fine("Inside Constructor. Set Name comes: " + this.set);
		this.etlreprock = rockFact;
		
		
		String timeStamp = new SimpleDateFormat("yyyy_MM_dd").format(new Date());
		
		if (plogdir == null) {
		      throw new IOException("System property \"LOG_DIR\" not defined");
		    }
		logfile = plogdir + File.separator + "engine" + File.separator + "aggregator-" + timeStamp + ".log" ;
		
		f= new File(logfile);
		   

	}

	/**
	 * Executes a SQL procedure
	 * 
	 */

	@Override
	public synchronized void execute() throws EngineException {
		String oldStatus = "";
		String status = "FAILED";
		long startTime = 0;
		long endTime = 0;
		int rowCount = 0;
		String sqlClause = "";
		String actionName = this.getTransferActionName();

		try {

			try {

				final Meta_databases md_cond = new Meta_databases(etlreprock);
				md_cond.setType_name("USER");
				final Meta_databasesFactory md_fact = new Meta_databasesFactory(etlreprock, md_cond);

				final Vector<Meta_databases> dbs = md_fact.get();

				final Iterator<Meta_databases> it = dbs.iterator();
				while (it.hasNext()) {
					final Meta_databases db = (Meta_databases) it.next();

					if (db.getConnection_name().equalsIgnoreCase("dwhrep")) {
						dwhreprock = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(),
								db.getDriver_name(), "Aggregator", true);
					} else if (db.getConnection_name().equalsIgnoreCase("dwh")) {
						dwhrock = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(),
								db.getDriver_name(), "Aggregator", true);
					}

				} // for each Meta_databases

				if (dwhrock == null) {
					throw new Exception("Database dwh is not defined in Meta_databases?!");
				}

				if (dwhreprock == null) {
					throw new Exception("Database dwhrep is not defined in Meta_databases?!");
				}

				final Meta_collection_sets mcs_cond = new Meta_collection_sets(etlreprock);
				mcs_cond.setCollection_set_id(this.collectionSetID);
				final Meta_collection_setsFactory mcs_fact = new Meta_collection_setsFactory(etlreprock, mcs_cond);

				final Vector<Meta_collection_sets> tps = mcs_fact.get();

				final Meta_collection_sets tp = (Meta_collection_sets) tps.get(0);

				tpName = tp.getCollection_set_name();

				if (tpName == null) {
					throw new Exception("Unable to resolve TP name");
				}

			} catch (final Exception e) {
				// Failure cleanup connections			
				try {
					dwhreprock.getConnection().close();
				} catch (final Exception se) {
				}

				try {
					dwhrock.getConnection().close();
				} catch (final Exception ze) {
				}
				e.getMessage();
				log.log(Level.WARNING,"Unable to resolve techpack name {0}", e.getMessage());
			}

			startTime = new Date().getTime();
			init(collection.getScheduling_info());

			final AggregationStatus aggSta = AggregationStatusCache.getStatus(aggregation, longDate);
			if (aggSta != null) {
				oldStatus = aggSta.STATUS;
			}

			log.finest("OLDAggregationStatus: " + oldStatus);

			log.info("Beginning the execution of SQLActionExecute for action:" + actionName);

			sessionID = SessionHandler.getSessionID(SESSIONTYPE);

			sqlClause = this.getTrActions().getAction_contents();
			sqlLog.finer("Unparsed sql:" + sqlClause);
			sqlClause = convert(sqlClause);
			sqlLog.finer("Parsed sql:" + sqlClause);
			log.fine("Parsed SQL clause: " + sqlClause);
			rowCount = executeSQLUpdate(sqlClause); //Execution & Re-Triggering
			status = "OK";
			log.info("The execution of SQLActionExecute for action:" + actionName + "finished successfully");
		} catch (SQLException s) {
			if ((s.getSQLState().contains("QSB34")) || (s.getMessage().contains("Insufficient buffers for 'Sort'."))) {
				log.info("SQL state is " + s.getSQLState());
				try {
					/*Check if the aggregator log file exists
					If the file exists, search for the actionName and DataDate for which the Re-execution mechanism
					has failed for the Re-execution.*/
				if(f.exists()){
				fstream = new FileInputStream(f);
				 br = new BufferedReader(new InputStreamReader(fstream));
				String strLine;			  
					 while ((strLine = br.readLine()) != null)  {
					 if(strLine.contains(actionName+ " : "+ stringDate)){
				    		statFlag = true;
				    		break;
						   }
					 }
					 }
				}
			catch (Exception ex) {
				log.log(Level.SEVERE, ex.getMessage());
			}	
				if (statFlag == true){
				/* statFlag = true, implies that actionName (MO) is already present in the aggregator.log file and the Re-triggering also has failed.
				 * Hence setting it's status to "ERROR"
					*/
					status = "ERROR";
					log.warning("Re-Triggering of sqlClause failed. SqlClause is : " + sqlClause + " Hence the status is " + status);						
				}
				else {
					//if statFlag is false, Re-execute the sqlClause
					try {
					log.warning("Re-executing the sqlClause");
					rowCount = executeSQLUpdate(sqlClause);
					status = "OK";
					log.info("Re-executed the sqlClause " + sqlClause + " and the status is " + status);
				} catch (SQLException sqle) {
					if ((sqle.getSQLState().contains("QSB34")) || (sqle.getMessage().contains("Insufficient buffers for 'Sort'."))) {
						status = "LOADED";
						log.info("Re-triggering the " +actionName + "and the status is "+status);
						//Adding the failed actionName and DataDate to the aggregator log when Re-execution fails
						aggregatorLog.info(actionName+ " : "+ stringDate);
						}
						} 
						}
					}
		
			else if(s.getMessage().contains("Bitmap") && sqlClause.contains("Insert into")){
				log.log(Level.SEVERE, "Error while initializing aggregation due to an SQL exception " + s.getMessage() + " and the sql clause is : " +sqlClause.substring(sqlClause.indexOf("Insert"),sqlClause.indexOf("(")) );
				throw new EngineException("Error while initializing aggregation due to an SQL exception",new String[] { sqlClause }, s, this,
						this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
			}
			else{
				log.log(Level.SEVERE, "Error while initializing aggregation due to an SQL exception " + s.getMessage());
				throw new EngineException("Error while initializing aggregation due to an SQL exception",new String[] { sqlClause }, s, this,
						this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
			}
		} catch (Exception exception) {
			throw new EngineException("Cannot execute action.", new String[] { sqlClause }, exception, this,
					this.getClass().getName(), "EXECUTION ERROR");
		}

		finally {

			try {
				
				final AggregationStatus aggSta = getStatusFromAggregationStatusCache();
				if((aggSta.AGGREGATIONSCOPE.equalsIgnoreCase("WEEK") | aggSta.AGGREGATIONSCOPE.equalsIgnoreCase("MONTH")) && aggSta.TIMELEVEL.equalsIgnoreCase("RANKBH") && status.equals("OK")) {
					final EventDrivenAggregatorNomination edan = new EventDrivenAggregatorNomination(log);
					edan.triggerDependantAggregations(aggSta);
					
				} else {
					log.info(aggSta.AGGREGATION+" is not required to be nominated!");
				}
				if (!actionName.equals("createAliasView")) {
					updateStatus(rowCount, status, oldStatus);
					endTime = new Date().getTime();
					updateAggLog(startTime, endTime, status, rowCount);
				}
				
			} catch(Exception ee) {
				throw new EngineException("Cannot execute action.", new String[] { sqlClause }, ee, this,
						this.getClass().getName(), "EXECUTION ERROR");
			}
			finally {
				try {
					dwhreprock.getConnection().close();
				} catch (final Exception se) {
				}

				try {
					dwhrock.getConnection().close();
				} catch (final Exception ze) {
				}
			}

		}
	}

	protected void updateStatus(final int rowCount, final String status, final String oldStatus)
			throws EngineException {

		final long endTime = new Date().getTime();

		try {

			// update
			final AggregationStatus aggSta = getStatusFromAggregationStatusCache();
			if (aggSta != null) {

				// if status has NOT changed during aggregation
				if (oldStatus.length() < 1 || oldStatus.equalsIgnoreCase(aggSta.STATUS)) {
					if (status.equalsIgnoreCase("OK")) {
						aggSta.STATUS = "AGGREGATED";
					}else if (status.equalsIgnoreCase("LOADED")){
						aggSta.STATUS = "LOADED";
					} else{
						aggSta.STATUS = "ERROR";
					}

					if (aggSta.INITIAL_AGGREGATION == 0) {
						// this is first aggregation (INITIAL_AGGREGATION is
						// zero)
						aggSta.INITIAL_AGGREGATION = endTime;
					} else {
						// this is NOT the first aggregation
						// (INITIAL_AGGREGATION is NOT
						// zero)
						aggSta.LAST_AGGREGATION = endTime;
					}

					/*
					 * if (rowCount == 0) { aggSta.STATUS = "ERROR";
					 * log.fine("sql result rowCount is 0, status set to
					 * ERROR"); }
					 */

					log.fine("Update aggregation monitorings status to " + aggSta.STATUS + " : " + aggregation);

					aggSta.ROWCOUNT = rowCount;
					aggSta.DESCRIPTION = "Last Status: " + this.aggStatus;
					setStatusInAggregationStatusCache(aggSta);

				} else {

					log.fine("Aggregation monitorings status changed during aggregation ( " + oldStatus + " -> "
							+ aggSta.STATUS + " ): Status not updatet: " + aggregation);
				}

			}

		} catch (final Exception e) {

			log.log(Level.SEVERE, "Error updating LOG_AggregationStatus", e);
			throw new EngineException("Error updating LOG_AggregationStatus", new String[] {}, e, this,
					this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);

		}

	}

	/**
	 * extracting out to get under unit test
	 */
	void setStatusInAggregationStatusCache(final AggregationStatus aggSta) {
		AggregationStatusCache.setStatus(aggSta);
	}

	/**
	 * extracting out to get under unit test
	 */
	AggregationStatus getStatusFromAggregationStatusCache() {
		return AggregationStatusCache.getStatus(aggregation, longDate);
	}

	protected void updateAggLog(final long startTime, final long endTime, final String status, final int rowCount)
			throws EngineException {

		try {

			final HashMap<String, Object> aMap = new HashMap<String, Object>();

			aMap.put("SESSION_ID", String.valueOf(sessionID));
			aMap.put("BATCH_ID", "1");
			aMap.put("DATE_ID", this.stringDate);
			aMap.put("AGGREGATORSET_ID", String.valueOf(this.setID.longValue()));
			aMap.put("SOURCE", "");
			aMap.put("TYPENAME", this.aggregation);
			aMap.put("SESSIONSTARTTIME", String.valueOf(startTime));
			aMap.put("SESSIONENDTIME", String.valueOf(endTime));
			aMap.put("STATUS", status);
			aMap.put("ROWCOUNT", String.valueOf(rowCount));
			aMap.put("DATATIME", String.valueOf(this.longDate));
			aMap.put("DATADATE", String.valueOf(this.longDate));
			aMap.put("TIMELEVEL", this.timelevel);

			// aggregation status

			SessionHandler.log(SESSIONTYPE, aMap);

		} catch (final Exception e) {
			log.log(Level.SEVERE, "Error creating aggregationLog", e);
			throw new EngineException("Error creating aggregationLog", new String[] {}, e, this,
					this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
		}
	}

	protected void init(final String info) throws Exception {

		log.fine("Initializing...");
		log.fine("Info comes: " + info);

		// '2005-03-16 00:00:00'
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		final SimpleDateFormat sdfShort = new SimpleDateFormat("yyyy-MM-dd");
		String dateString = "";
		String sqlClause = "";

		Statement stmt = null;
		ResultSet rSet = null;
		log.fine("Set: " + this.set);

		try {

			sqlClause = "select agg.aggregation,agg.target_type, agg.target_level " + "from LOG_AggregationRules agg "
					+ "where agg.aggregation = '" + this.set + "' ";

			log.fine("Query to get infor for agg: " + sqlClause);
			log.fine("Retrieve " + this.set + " target.type and target.level");

			sqlLog.finest("Unparsed sql:" + sqlClause);

			stmt = this.getConnection().getConnection().createStatement();
			stmt.getConnection().commit();
			rSet = stmt.executeQuery(sqlClause);
			stmt.getConnection().commit();
			log.fine("ResultSet next: " + rSet.next());
			while (rSet.next()) {
				this.timelevel = rSet.getString("target_level");
				this.typename = rSet.getString("target_type");
				this.aggregation = rSet.getString("aggregation");
				log.fine("Aggregation Name: " + this.aggregation);
				log.fine("TypeName Name: " + this.typename);
				log.fine("TimeLevel Name: " + this.timelevel);
			}
			log.fine("Aggregation Name: " + this.aggregation);
			log.fine("TypeName Name: " + this.typename);
			log.fine("TimeLevel Name: " + this.timelevel);

		} catch (final Exception e) {
			log.fine("Error while accesing LOG_AggregationRules table " + sqlClause);
		} finally {
			try {
				rSet.close();
			} catch (Exception e) {
			}

			try {
				stmt.close();
			} catch (final Exception e) {
			}

		}

		orig = new Properties();
		if (info != null && info.length() > 0) {
			final ByteArrayInputStream bais = new ByteArrayInputStream(info.getBytes());
			orig.load(bais);
			bais.close();
		}

		if (orig != null) {

			log.fine("Parsing info");
			log.fine("Parsing info for SET: " + this.set);
			if (this.set.contains(Constants.SON15AGG)) {
				log.fine("SET contains " + Constants.SON15AGG);
				this.aggregation = this.set.substring(this.set.indexOf("_DC_") + 1, this.set.length());
				log.fine("INTERMITTENT AGGREGATIION:  " + this.aggregation);
				this.typename = this.aggregation.substring(0, this.aggregation.indexOf("_" + Constants.SON15AGG));
				log.fine("INTERMITTENT typename:  " + this.typename);
				this.timelevel = Constants.SON15AGG;
				log.fine("INTERMITTENT TIMELEVEL:  " + this.timelevel);
				this.aggStatus = "DUMMY";
				this.aggregationScope = Constants.ROPAGGSCOPE;
				dateString = String.valueOf(System.currentTimeMillis());
				log.fine("INTERMITTENT DATESTRING:  " + dateString);
			} else {
				log.fine("SET not contains " + Constants.SON15AGG);
				this.aggregation = orig.getProperty("aggregation", this.aggregation);
				this.typename = orig.getProperty("typename", this.typename);
				this.timelevel = orig.getProperty("timelevel", this.timelevel);
				this.aggStatus = orig.getProperty("status", this.aggStatus);
				this.aggregationScope = orig.getProperty("aggregationscope", this.aggregationScope);
				dateString = orig.getProperty("aggDate", "");
			}

			log.fine("After prop Aggregation Name: " + this.aggregation);
			log.fine("After prop TypeName Name: " + this.typename);
			log.fine("After prop TimeLevel Name: " + this.timelevel);

			log.info("Aggregating " + aggregation + " for " + dateString);

			log.finer("Info: type (" + typename + ") timelevel (" + timelevel + ") status (" + aggStatus + ") scope ("
					+ aggregationScope + ") date (" + dateString + ")");

			try {
				// try to parse from yyyy-MM-dd HH:mm:ss format
				this.longDate = sdf.parse(dateString).getTime();
				this.stringDate = dateString;
			} catch (final Exception e) {
				// not in yyyy-MM-dd HH:mm:ss format
				this.stringDate = "";
			}

			try {
				// try to parse from yyyy-MM-dd format
				final Date date = sdfShort.parse(dateString);
				this.longDate = date.getTime();
				this.stringDate = sdf.format(date);
			} catch (final Exception e) {
				// not in yyyy-MM-dd HH:mm:ss format
				this.stringDate = "";
			}

			if (this.stringDate.equalsIgnoreCase("")) {

				try {
					// try to parse from long format
					final GregorianCalendar curCal = new GregorianCalendar();
					curCal.setTimeInMillis(Long.parseLong(dateString));
					this.stringDate = sdf.format(curCal.getTime());
					this.longDate = Long.parseLong(dateString);
				} catch (final Exception e) {

					// not in long format
					this.stringDate = "";
				}
			}

		}

		// if stringDate is empty, use previous day
		if (stringDate.equalsIgnoreCase("")) {

			final Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -1);

			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);

			this.stringDate = sdf.format(cal.getTime());
			this.longDate = cal.getTimeInMillis();

			log.fine("Unable to parse date of aggregation. Using yesterday.");

		}

		log.fine("Date of aggregation is " + stringDate + " (" + longDate + ")");

		sourceTableMap = new HashMap<String, String>();
		targetTableMap = new HashMap<String, String>();
		sourceTableMapForCount = new HashMap<String, String>();
		targetTableMapForCount = new HashMap<String, String>();
		sourceTableMapList = new HashMap<String, List<Map<String, String>>>();
		targetTableMapList = new HashMap<String, List<Map<String, String>>>();

		final PhysicalTableCache physicalTableCache = PhysicalTableCache.getCache();

		if (physicalTableCache == null) {
			throw new Exception("PhysicalTableCache not initialized");
		}

		final Tpactivation tpa = new Tpactivation(dwhreprock);
		tpa.setTechpack_name(this.tpName);
		tpa.setStatus("ACTIVE");
		final TpactivationFactory tpaf = new TpactivationFactory(dwhreprock, tpa);

		if (tpaf.get().isEmpty()) {
			throw new Exception("No Active TechPacks found for " + this.tpName);
		}

		final Tpactivation tpatmp = ((Vector<Tpactivation>) tpaf.get()).get(0);

		log.finer("Active TP version is " + tpatmp.getVersionid());

		final Aggregationrule ar = new Aggregationrule(dwhreprock);
		ar.setAggregation(this.aggregation);
		ar.setVersionid(tpatmp.getVersionid());
		AggregationruleFactory arf = new AggregationruleFactory(dwhreprock, ar);

		final Vector<Aggregationrule> rulez = arf.get();

		log.fine(rulez.size() + " aggregation rules found for version");

		final Iterator<Aggregationrule> iter = rulez.iterator();

		while (iter.hasNext()) {

			final Aggregationrule aggregationRule = (Aggregationrule) iter.next();

			log.finer("Rule " + aggregationRule.getAggregation() + " id " + aggregationRule.getRuleid());

			targetTable.put(aggregationRule.getRuletype(), aggregationRule.getTarget_table());
			sourceTable.put(aggregationRule.getRuletype(), aggregationRule.getSource_table());
			targetType.put(aggregationRule.getRuletype(), aggregationRule.getTarget_type());
			sourceType.put(aggregationRule.getRuletype(), aggregationRule.getSource_type());

			final GregorianCalendar greg = new GregorianCalendar();
			greg.setTimeInMillis(this.longDate);
			greg.add(Calendar.DATE, -1);
			final long prevDay = greg.getTimeInMillis();

			final List sourceTables = physicalTableCache.getTableName("ACTIVE,READONLY",
					aggregationRule.getSource_type() + ":" + aggregationRule.getSource_level(), this.longDate,
					increaseByScope(this.longDate, this.aggregationScope));

			if (sourceTables.size() < 1 && this.set.contains(Constants.SON15AGG)) {
				final String sourceTableName = aggregationRule.getSource_type() + "_" + Constants.SONVISTEMPTABLE;
				sourceTables.add(sourceTableName);
			}

			// TODO: DEBUG
			log.info("Sourcetable size: " + sourceTables.size() + ", type: " + aggregationRule.getSource_type()
					+ ", level: " + aggregationRule.getSource_level() + ", date: " + longDate + ", increase: "
					+ increaseByScope(this.longDate, this.aggregationScope));

			// if ruletype is RANKBH we ignore the source table checks because
			// they will be empty
			if (!"RANKBH".equalsIgnoreCase(aggregationRule.getRuletype()) && sourceTables.size() < 1) {
				throw new Exception("No Tables/Partitions found for Aggregations " + aggregationRule.getAggregation()
						+ " (SourceType/SourceLevel) " + aggregationRule.getSource_type() + "/"
						+ aggregationRule.getSource_level());
			}

			final List targetTables = physicalTableCache.getTableName("ACTIVE,READONLY",
					aggregationRule.getTarget_type() + ":" + aggregationRule.getTarget_level(), this.longDate,
					increaseByScope(this.longDate, this.aggregationScope));

			if (targetTables.size() < 1) {
				throw new Exception("No Tables/Partitions found for Aggregation " + aggregationRule.getAggregation()
						+ " (TargetType/TargetLevel) " + aggregationRule.getTarget_type() + "/"
						+ aggregationRule.getTarget_level());
			}

			final List sourceTablesForCount = physicalTableCache.getTableName("ACTIVE,READONLY",
					aggregationRule.getSource_type() + ":" + aggregationRule.getSource_level(), prevDay,
					increaseByScope(this.longDate, this.aggregationScope));

			if (sourceTablesForCount.size() < 1 && this.set.contains(Constants.SON15AGG)) {
				final String sourceTableCountName = aggregationRule.getSource_type() + "_" + Constants.SONVISTEMPTABLE;
				sourceTablesForCount.add(sourceTableCountName);
			}

			// if ruletype is RANKBH we ignore the source table checks because
			// they will be empty
			if (!"RANKBH".equalsIgnoreCase(aggregationRule.getRuletype()) && sourceTablesForCount.size() < 1) {
				throw new Exception("No Tables/Partitions found for Aggregations " + aggregationRule.getAggregation()
						+ " (SourceType/SourceLevel) " + aggregationRule.getSource_type() + "/"
						+ aggregationRule.getSource_level());
			}

			final List targetTablesForCount = physicalTableCache.getTableName("ACTIVE,READONLY",
					aggregationRule.getTarget_type() + ":" + aggregationRule.getTarget_level(), prevDay,
					increaseByScope(this.longDate, this.aggregationScope));

			if (targetTablesForCount.size() < 1) {
				throw new Exception("No Tables/Partitions found for Aggregation " + aggregationRule.getAggregation()
						+ " (TargetType/TargetLevel) " + aggregationRule.getTarget_type() + "/"
						+ aggregationRule.getTarget_level());
			}

			final String sourceDerived = createDerivedTable(sourceTables);
			sourceTableMap.put(aggregationRule.getRuletype(), sourceDerived);
			log.finer("Aggregation source " + sourceDerived);

			final String targetDerived = createDerivedTable(targetTables);
			targetTableMap.put(aggregationRule.getRuletype(), targetDerived);
			log.finer("Aggregation target " + targetDerived);

			final String sourceDerivedForCount = createDerivedTable(sourceTablesForCount);
			sourceTableMapForCount.put(aggregationRule.getRuletype(), sourceDerivedForCount);
			log.finer("Aggregation source for count " + sourceDerivedForCount);

			final String targetDerivedForCount = createDerivedTable(targetTablesForCount);
			targetTableMapForCount.put(aggregationRule.getRuletype(), targetDerivedForCount);
			log.finer("Aggregation target for count " + targetDerivedForCount);

			final List<Map<String, String>> stmList = new ArrayList<Map<String, String>>();
			final List<Map<String, String>> ttmList = new ArrayList<Map<String, String>>();

			for (int s = 0; s < sourceTables.size(); s++) {
				if (this.set.contains(Constants.SON15AGG)) {
					final Map<String, String> stm = new HashMap<String, String>();
					stm.put("table", (String) sourceTables.get(s));
					stm.put("startTime", "'" + sdf.format(new Date(System.currentTimeMillis() - 60000)) + "'");
					stm.put("endTime", "'" + sdf.format(new Date(System.currentTimeMillis() + 60000)) + "'");
					stmList.add(stm);
				} else {
					final Map<String, String> stm = new HashMap<String, String>();
					stm.put("table", (String) sourceTables.get(s));
					stm.put("startTime",
							"'" + sdf.format(new Date(physicalTableCache.getStartTime((String) sourceTables.get(s))))
									+ "'");
					stm.put("endTime", "'"
							+ sdf.format(new Date(physicalTableCache.getEndTime((String) sourceTables.get(s)))) + "'");
					stmList.add(stm);
				}

			}

			log.finer(stmList.size() + " sourceTables added");

			for (int t = 0; t < targetTables.size(); t++) {

				final Map<String, String> ttm = new HashMap<String, String>();
				ttm.put("table", (String) targetTables.get(t));
				ttm.put("startTime", "'"
						+ sdf.format(new Date(physicalTableCache.getStartTime((String) targetTables.get(t)))) + "'");
				ttm.put("endTime",
						"'" + sdf.format(new Date(physicalTableCache.getEndTime((String) targetTables.get(t)))) + "'");
				ttmList.add(ttm);
			}

			log.finer(ttmList.size() + " targetTables added");

			sourceTableMapList.put(aggregationRule.getRuletype(), stmList);
			targetTableMapList.put(aggregationRule.getRuletype(), ttmList);

		} // for each AggregationRule

		log.fine("Initialized");

	}

	private long increaseByScope(final long date, final String scope) {

		final GregorianCalendar greg = new GregorianCalendar();
		greg.setTimeInMillis(date);

		if (scope.equalsIgnoreCase("day")) {

			greg.add(GregorianCalendar.DATE, 1);
			greg.add(GregorianCalendar.SECOND, -1);
			return greg.getTimeInMillis();

		} else if (scope.equalsIgnoreCase("week")) {

			greg.add(GregorianCalendar.WEEK_OF_MONTH, 1);
			return greg.getTimeInMillis();

		} else if (scope.equalsIgnoreCase("month")) {

			greg.add(GregorianCalendar.MONTH, 1);
			return greg.getTimeInMillis();

		}

		return date;

	}

	private String createDerivedTable(final List<String> tables) {

		if (tables.size() != 0) {

			if (tables.size() == 1) {
				// one table

				return (String) tables.get(0);

			} else {
				// multiple tables , need to create derived table
				final StringBuffer sb = new StringBuffer();

				sb.append("(");
				boolean first = true;
				final Iterator<String> iter = tables.iterator();
				while (iter.hasNext()) {

					final String table = (String) iter.next();
					if (first) {
						sb.append("select * from " + table);
						first = false;
					} else {
						sb.append(" union all \n select * from " + table);
					}
				}

				sb.append(") ");

				return sb.toString();
			}
		}

		return "";

	}

	public String convert(final String s) throws Exception {
		final StringWriter writer = new StringWriter();
		VelocityEngine ve = null;
		try {

			ve = VelocityPool.reserveEngine();
			final VelocityContext context = new VelocityContext();
			context.put("sessionid", new Long(sessionID));
			context.put("batchid", "1");
			context.put("dateid", "'" + this.stringDate + "'");

			context.put("targetDerivedTable", targetTableMap);
			context.put("sourceDerivedTable", sourceTableMap);

			context.put("targetDerivedTableForCount", targetTableMapForCount);
			context.put("sourceDerivedTableForCount", sourceTableMapForCount);

			context.put("targetTableList", targetTableMapList);
			context.put("sourceTableList", sourceTableMapList);

			context.put("targetTable", targetTable);
			context.put("sourceTable", sourceTable);

			context.put("targetType", targetType);
			context.put("sourceType", sourceType);

			ve.evaluate(context, writer, "", s);

		} catch (final Exception e) {
			log.log(Level.SEVERE, "Conversion error", e);
			throw new EngineException("conversion error", new String[] { s }, e, this, this.getClass().getName(),
					EngineConstants.ERR_TYPE_EXECUTION);
		} finally {
			VelocityPool.releaseEngine(ve);
		}

		return writer.toString();
	}

}
