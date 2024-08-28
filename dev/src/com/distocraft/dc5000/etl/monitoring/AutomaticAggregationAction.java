package com.distocraft.dc5000.etl.monitoring;

import java.io.ByteArrayOutputStream;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.StaticProperties;
import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.sql.SQLActionExecute;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.etl.scheduler.ISchedulerRMI;
import com.distocraft.dc5000.etl.scheduler.SchedulerConnect;
import com.distocraft.dc5000.repository.cache.AggregationStatus;
import com.distocraft.dc5000.repository.cache.AggregationStatusCache;
import com.ericsson.eniq.common.Constants;

/**
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
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * <br>
 * <br>
 * AutomaticAggregation action triggers (start in scheduler) all aggregations
 * that have status set to (in LOG_AGGREGATIONSTATUS) LOADED or BLOCKED.<br>
 * <br>
 * Aggregations are sorted before triggering using aggregations rule type so
 * that aggragtions are inserted to the priority queue in a following order:
 * RAW, COUNT, TOTAL, RANKBH, RANKSRC, BHSRC, RANKBHCLASS, DAYBHCLASS.<br>
 * <br>
 * Triggered aggregations status (in LOG_AGGREGATIONSTATUS) is changed to
 * QUEUED. <br>
 * Following columns are retrived from LOG_AGGREGATIONRULES and
 * LOG_AGGREGATIONSTATUS table and given to the aggregation as a propertyString
 * in Scheduling_info: datadate, aggregation, typename, timelevel, status,
 * aggregationscope.<br>
 * <br>
 * <br>
 * 
 */
public class AutomaticAggregationAction extends SQLActionExecute {

	public static final String SESSIONTYPE = "aggregator";

	public final static String sonDayAgg = Constants.SONAGG ;
	public final static String son15Agg = Constants.SON15AGG ;
	
	private String[] ruletypeOrder = { "RAW", "COUNT", sonDayAgg, "TOTAL", "RANKBH", "RANKSRC", "BHSRC", "RANKBHCLASS",
			"DAYBHCLASS_DAYBH", "DAYBHCLASS" };

	private final Logger log;

	private final Vector ruletypeOrderVector;

	private Vector handleRuletypeOrder() {
		Vector ruletypeOrderVector = new Vector();

		final String ruletypeOrderString = StaticProperties.getProperty("Aggregator.ruletypeOrder", "");

		if (ruletypeOrderString.length() > 0) {

			ruletypeOrder = ruletypeOrderString.split(",");
		}

		for (int i = 0; i < ruletypeOrder.length; i++)
			ruletypeOrderVector.add(ruletypeOrder[i]);

		return ruletypeOrderVector;
	}

	// for JUnit
	protected AutomaticAggregationAction() {
		this.ruletypeOrderVector = handleRuletypeOrder();
		this.log = Logger.getLogger("etlengine.AutomaticAgg");
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
	 */
	public AutomaticAggregationAction(final Meta_versions version, final Long collectionSetId,
			final Meta_collections collection, final Long transferActionId, final Long transferBatchId, final Long connectId,
			final RockFactory rockFact, final ConnectionPool connectionPool, final Meta_transfer_actions trActions,
			final Logger clog) throws EngineMetaDataException {

		super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
				trActions);

		this.ruletypeOrderVector = handleRuletypeOrder();
		this.log = Logger.getLogger(clog.getName() + ".AutomaticAgg");
		
	}

	/**
	 * Executes a SQL procedure
	 */
	public void execute() throws EngineException {

		try {

			aggregate();

		} catch (Exception e) {
      final String msg = e.getMessage();

			if (msg != null && (msg.indexOf("-141: Table 'LOG_AggregationRules'") > 0
					|| msg.indexOf("-141: Table 'LOG_AggregationStatus'") > 0)) {
				log.info("Automatic aggregation was cancelled because techpack DWH_MONITOR is not created into DWH");
			} else {

				log.log(Level.SEVERE, "Automatic aggregation failed exceptionally", e);

				throw new EngineException(EngineConstants.CANNOT_EXECUTE, new String[] { this.getTrActions()
						.getAction_contents() }, e, this, this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
			}
		}
	}

	class comp implements Comparator {

		public int compare(final Object d1, final Object d2) {

			final List l1 = (List) d1;
			final List l2 = (List) d2;

			final Integer i1 = (Integer) l1.get(4);
			final Integer i2 = (Integer) l2.get(4);

			return i1.compareTo(i2);
		}
	}

	protected void aggregate() throws Exception {

		// get all ready-to-be-aggregated for this day
		final String sqlClause = "select sta.aggregation, sta.typename, sta.timelevel, sta.datadate, agg.ruletype, sta.status, sta.aggregationscope   "
				+ "from LOG_AggregationRules agg,LOG_AggregationStatus sta "
				+ "where sta.status  in ('LOADED','BLOCKED') "
				+ "and agg.aggregation = sta.aggregation "
				+ "and agg.target_type = sta.typename "
				+ "group by sta.aggregation, sta.typename, sta.timelevel, sta.datadate , agg.ruletype, sta.status, sta.aggregationscope     ";

		final ArrayList aggregations = new ArrayList();
    log.info("aggregate -- start");
		log.fine(" get all ready-to-be-aggregated types ");
		log.finest("sql :" + sqlClause);

		Statement stmt = null;
		ResultSet rSet = null;
		try {
			stmt = this.getConnection().getConnection().createStatement();
			stmt.getConnection().commit();
			rSet = stmt.executeQuery(sqlClause);

			final List doublicateList = new ArrayList();

			if (rSet != null) {
				while (rSet.next()) {

					final String aggregation = rSet.getString("aggregation");
					final String timelevel = rSet.getString("timelevel");
					final String typename = rSet.getString("typename");
					final java.sql.Date sqldatadate = rSet.getDate("datadate");
					final Long datadate = new Long(sqldatadate.getTime());
					final String ruletype = rSet.getString("ruletype");
					final String status = rSet.getString("status");
					final String aggregationScope = rSet.getString("aggregationscope");

					if (!doublicateList.contains(aggregation + datadate)) {

						doublicateList.add(aggregation + datadate);

						final ArrayList subList = new ArrayList();
						subList.add(0, aggregation);
						subList.add(1, timelevel);
						subList.add(2, typename);
						subList.add(3, datadate);
						subList.add(4, new Integer(ruletypeOrderVector.indexOf(ruletype)));
						subList.add(5, status);
						subList.add(6, aggregationScope);
						aggregations.add(subList);

					}

				}
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
			
			try {
				stmt.getConnection().close();
			} catch (Exception e) {
			}
		}
    log.info("aggregate -- sort");

		// sort arrray by ruletype
		Collections.sort(aggregations, new comp());

		final ISchedulerRMI scheduler = SchedulerConnect.connectScheduler();

		final Iterator iter = aggregations.iterator();
    log.info("aggregate -- trigger");
		while (iter.hasNext()) {

			final ArrayList subList = (ArrayList) iter.next();

			final String aggregation = (String) subList.get(0);
			final String timelevel = (String) subList.get(1);
			final String typename = (String) subList.get(2);
			final Long datadate = (Long) subList.get(3);
			final String status = (String) subList.get(5);
			final String aggregationScope = (String) subList.get(6);

			final Properties prop = new Properties();
			prop.setProperty("aggDate", datadate.toString());
			prop.setProperty("aggregation", aggregation);
			prop.setProperty("typename", typename);
			prop.setProperty("timelevel", timelevel);
			prop.setProperty("status", status);
			prop.setProperty("aggregationscope", aggregationScope);

			try {

				final AggregationStatus aggSta = AggregationStatusCache.getStatus(aggregation, datadate.longValue());

				log.info("Triggering set: " + "Aggregator_" + aggregation + ", " + datadate);
				scheduler.trigger("Aggregator_" + aggregation, this.propertyToString(prop));

				if (aggSta != null) {
					aggSta.STATUS = "QUEUED";
					AggregationStatusCache.setStatus(aggSta);
				}

			} catch (Exception e) {

				log.warning("error in starting trigger: " + aggregation);
				log.log(Level.FINE, "Error starting trigger: " + aggregation, e);

			}

		}
    log.info("aggregate -- finish");

	}

	private String propertyToString(final Properties prop) throws Exception {

		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		prop.store(baos, "");

		return baos.toString();
	}

}
