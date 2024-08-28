package com.distocraft.dc5000.etl.monitoring;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.sql.SQLActionExecute;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.repository.cache.AggregationStatus;
import com.distocraft.dc5000.repository.cache.AggregationStatusCache;

public class ManualReAggregationAction extends SQLActionExecute {

	public static final String SESSIONTYPE = "aggregator";

	private final Logger log;
	private final Logger sqlLog;

	protected RockFactory rockFact;

	protected ManualReAggregationAction() {
		this.log = Logger.getLogger("etlengine.ManualReAgg");
		this.sqlLog = Logger.getLogger("sql.ManualReAgg");
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
	public ManualReAggregationAction(final Meta_versions version, final Long collectionSetId,
			final Meta_collections collection, final Long transferActionId, final Long transferBatchId, final Long connectId,
			final RockFactory rockFact, final ConnectionPool connectionPool, final Meta_transfer_actions trActions,
			final Logger clog) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.rockFact = rockFact;
		
		this.log = Logger.getLogger(clog.getName() + ".ManualReAgg");
		this.sqlLog = Logger.getLogger("sql" + log.getName().substring(4));
		
	}

	/**
	 * Executes a SQL procedure
	 * 
	 */

	public void execute() throws EngineException {

		try {

			// find relations from aggregationrules table and set all 'connected
			// aggregations' manual. This is done only for day data.

			final String sqlClause = " SELECT ruli.target_type, ruli.target_level , aggi.datadate FROM LOG_AggregationStatus aggi, LOG_AGGREGATIONRULES ruli  WHERE aggi.status = 'MANUAL'   and aggi.aggregation = ruli.aggregation  group by ruli.target_type,ruli.target_level, aggi.datadate ";
			log.fine("check for manual data");
			sqlLog.finest("Unparsed sql:" + sqlClause);

			Statement stmt = null;
			ResultSet rSet = null;

			try {
				stmt = this.getConnection().getConnection().createStatement();
				stmt.getConnection().commit();
				rSet = stmt.executeQuery(sqlClause);
				stmt.getConnection().commit();
				while (rSet.next()) {
					final String typename = (String) rSet.getString("target_type");
					final String timelevel = (String) rSet.getString("target_level");
					final String datadate = (String) rSet.getString("datadate");

					final ReAggregation reag = new ReAggregation(log, rockFact);
					reag.reAggregate("MANUAL", typename, timelevel, datadate);
				}

			} finally {
				try {
					rSet.close();
				} catch (Exception e) {
				}

				try {
					stmt.close();
				} catch (Exception e) {
				}
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "ManualReAggregation failed exceptionally", e);
			throw new EngineException(EngineConstants.CANNOT_EXECUTE,
					new String[] { this.getTrActions().getAction_contents() }, e, this, this.getClass().getName(),
					EngineConstants.ERR_TYPE_EXECUTION);
		}
	}

	protected Properties stringToProperty(final String str) throws Exception {

		final Properties prop = null;

		if (str != null && str.length() > 0) {
			final ByteArrayInputStream bais = new ByteArrayInputStream(str.getBytes());
			prop.load(bais);
			bais.close();
		}

		return prop;

	}

	protected String propertyToString(final Properties prop) throws Exception {

		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		prop.store(baos, "");

		return baos.toString();
	}

	protected Properties createProperty(final String str) throws Exception {

		final String result = "";

		final Properties prop = new Properties();
		final StringTokenizer st = new StringTokenizer(str, "=");
		final String key = st.nextToken();
		final String value = st.nextToken();
		prop.setProperty(key.trim(), value.trim());

		return prop;

	}

	protected void changeDayToManual(final String typename, final String datadate, final String aggregationscope) throws Exception {

		// do this only if day aggregation
		if (aggregationscope.equalsIgnoreCase("day")) {

			final String sqlClause = "SELECT " + "ruli.aggregation," + "aggi.datadate "
					+ "FROM LOG_AggregationStatus aggi, LOG_AggregationRules ruli " + "WHERE aggi.typename = '" + typename + "'"
					+ "and aggi.datadate = '" + datadate + "'"
					+ "and aggi.status in ('AGGREGATED','ERROR','CHECKFAILED', 'FAILEDDEPENDENCY') "
					+ "and aggi.aggregationscope = 'DAY' " + "and ruli.aggregationscope = 'DAY' "
					+ "and ruli.source_type = aggi.typename " + "group by " + "ruli.aggregation," + "aggi.datadate ";

			log.fine("check for late data DAY");
			sqlLog.finest("Unparsed sql:" + sqlClause);

			Statement stmt = null;
			ResultSet rSet = null;

			try {
				stmt = this.getConnection().getConnection().createStatement();
				stmt.getConnection().commit();
				rSet = stmt.executeQuery(sqlClause);
				stmt.getConnection().commit();

				while (rSet.next()) {

					final String aggregation = (String) rSet.getString("aggregation");
					final long longDate = (long) rSet.getDate("datadate").getTime();

					final AggregationStatus aggSta = AggregationStatusCache.getStatus(aggregation, longDate);
					aggSta.STATUS = "MANUAL";
					AggregationStatusCache.setStatus(aggSta);

				}

			} finally {
				try {
					rSet.close();
				} catch (Exception e) {
				}

				try {
					stmt.close();
				} catch (Exception e) {
				}
			}

		}

	}

	protected void changeWeekAndMonthToManual(final String typename, final String datadate, final String aggregationscope) throws Exception {
		// check also possible week aggregations

		Statement stmt = null;
		ResultSet rSet = null;

		try {
			String sqlClause = "SELECT " + "ruli.aggregation," + "aggi.datadate "
					+ "FROM LOG_AggregationStatus aggi, LOG_AggregationRules ruli " + "WHERE aggi.typename = '" + typename + "' "
					+ "and aggi.datadate between dateadd(dd,-6,'" + datadate + "') and '" + datadate + "' "
					+ "and aggi.status in ('AGGREGATED','ERROR','CHECKFAILED', 'FAILEDDEPENDENCY') "
					+ "and aggi.aggregationscope = 'WEEK' " + "and ruli.aggregationscope = 'WEEK' "
					+ "and ruli.target_type = aggi.typename " + "group by " + "ruli.aggregation," + "aggi.datadate ";

			log.fine("check for MANUAL data WEEK");
			sqlLog.finest("Unparsed sql:" + sqlClause);

			stmt = this.getConnection().getConnection().createStatement();
			stmt.getConnection().commit();
			rSet = stmt.executeQuery(sqlClause);
			stmt.getConnection().commit();

			while (rSet.next()) {
				final String aggregation = (String) rSet.getString("aggregation");
				final long longDate = (long) rSet.getDate("datadate").getTime();

				final AggregationStatus aggSta = AggregationStatusCache.getStatus(aggregation, longDate);
				aggSta.STATUS = "MANUAL";
				aggSta.DESCRIPTION = "Latest loading: " + datadate;
				AggregationStatusCache.setStatus(aggSta);
			}

			rSet.close();

			// check also possible month aggregations

			sqlClause = "SELECT " + "ruli.aggregation," + "aggi.datadate "
					+ "FROM LOG_AggregationStatus aggi, LOG_AggregationRules ruli " + "WHERE aggi.typename = '" + typename + "' "
					+ "and aggi.datadate between dateadd(mm,-1,'" + datadate + "')-1 and  '" + datadate + "' "
					+ "and aggi.status in ('AGGREGATED','ERROR','CHECKFAILED', 'FAILEDDEPENDENCY') "
					+ "and aggi.aggregationscope = 'MONTH' " + "and ruli.aggregationscope = 'MONTH' "
					+ "and ruli.target_type = aggi.typename " + "group by " + "ruli.aggregation," + "aggi.datadate ";

			log.fine("check for MANUAL data MONTH");
			sqlLog.finest("Unparsed sql:" + sqlClause);

			rSet = stmt.executeQuery(sqlClause);
			stmt.getConnection().commit();

			while (rSet.next()) {
				final String aggregation = (String) rSet.getString("aggregation");
				final long longDate = (long) rSet.getDate("datadate").getTime();

				final AggregationStatus aggSta = AggregationStatusCache.getStatus(aggregation, longDate);
				aggSta.STATUS = "MANUAL";
				aggSta.DESCRIPTION = "Latest loading: " + datadate;
				AggregationStatusCache.setStatus(aggSta);
			}

		} finally {
			try {
				rSet.close();
			} catch (Exception e) {
			}

			try {
				stmt.close();
			} catch (Exception e) {
			}
		}

	}

}
