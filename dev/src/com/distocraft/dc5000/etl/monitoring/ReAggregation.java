package com.distocraft.dc5000.etl.monitoring;

import static com.ericsson.eniq.common.Constants.ENGINE_DB_DRIVERNAME;
import static com.ericsson.eniq.common.Constants.ENGINE_DB_PASSWORD;
import static com.ericsson.eniq.common.Constants.ENGINE_DB_USERNAME;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.repository.cache.AggregationRule;
import com.distocraft.dc5000.repository.cache.AggregationRuleCache;
import com.distocraft.dc5000.repository.cache.AggregationStatus;
import com.distocraft.dc5000.repository.cache.AggregationStatusCache;
import com.ericsson.eniq.common.CalcFirstDayOfWeek;
import com.ericsson.eniq.exception.ConfigurationException;
import com.ericsson.eniq.repository.ETLCServerProperties;

/**
 * <br>
 * ReAggregation class contains ReAggregation methods that reads
 * aggregationRules table and updates the status column to all related
 * aggregations.<br>
 * <br>
 * Relation between aggregations (rows in the aggregation rule table) is defined
 * as relations between source_type,source_level and target_type,target_level
 * columns.<br>
 * <br>
 * When travelling trough rules ReAggregation reads Target_type + target_level
 * pairs from aggregation rules.<br>
 * <br>
 * Target_type + target_level pair matches (target type and level is matched to
 * source type and level) an aggregation or aggregations.<br>
 * <br>
 * Every aggregation contains one (only one) target type + level pair that is
 * again matched.<br>
 * <br>
 * ReAggragtes visits only once each type + level pair, so loops cannot be
 * formed. <br>
 * <br>
 * <br>
 * Ex.<br>
 * <br>
 * aggregation <b>a0</b> has target_type <b>type0</b> and target_level
 * <b>level0</b> and source_type <b>typex</b> and source_level <b>levelx</b>.<br>
 * aggregation <b>a1</b> has target_type <b>type1</b> and target_level
 * <b>level1</b> and source_type <b>type0</b> and source_level <b>level0</b>.<br>
 * aggregation <b>a2</b> has target_type <b>type2</b> and target_level
 * <b>level2</b> and source_type <b>type0</b> and source_level <b>level0</b>.<br>
 * aggregation <b>a2</b> has target_type <b>type2</b> and target_level
 * <b>level2</b> and source_type <b>type1</b> and source_level <b>level1</b>.<br>
 * aggregation <b>a3</b> has target_type <b>type3</b> and target_level
 * <b>level3</b> and source_type <b>type2</b> and source_level <b>level2</b>.<br>
 * <br>
 * <b>a0</b> has one source (it needs aggregation that have target type
 * <b>typex</b> and <b>levelx</b>) and one result <b>r0</b>.<br>
 * <b>a1</b> has one source (it needs aggregation that have target type
 * <b>type0</b> and <b>level0</b>) and one result<b>r1</b>.<br>
 * <b>a2</b> has two sources (it needs aggregations that have targe type
 * <b>type0</b> and <b>type1</b> and/or level <b>level0</b> and <b>level1</b>)
 * and one result <b>r2</b>.<br>
 * <b>a3</b> has one source (it needs aggregation that have targe type
 * <b>type2</b> and <b>level2</b>) and one result <b>r3</b>.<br>
 * <br>
 * if <b>a0</b> is updated also <b>a1,a2</b> and <b>a3</b> are updated<br>
 * if <b>a1</b> is updated also <b>a2</b> and <b>a3</b> are updated<br>
 * if <b>a2</b> is updated only <b>a3</b> is updated<br>
 * 
 */
public class ReAggregation {

	public static final String SESSIONTYPE = "monitor";

	protected Logger log;

	protected Logger sqlLog;

	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private final RockFactory etlreprock;
	private final RockFactory dwhreprock;
	private final RockFactory dwhrock;

	private final int firstDayOfTheWeek;

	public ReAggregation(final Logger clog, final RockFactory rock) throws Exception {

		etlreprock = rock;
		firstDayOfTheWeek = CalcFirstDayOfWeek.calcFirstDayOfWeek();

		log = Logger.getLogger(clog.getName() + ".ReAggregate");
		sqlLog = Logger.getLogger("sql." + clog.getName() + ".ReAggregate");

		RockFactory dwhrep = null;
		RockFactory dwh = null;

		try {

			final Meta_databases md_cond = new Meta_databases(etlreprock);
			md_cond.setType_name("USER");
			final Meta_databasesFactory md_fact = new Meta_databasesFactory(etlreprock, md_cond);

			for (Meta_databases db : md_fact.get()) {
				if (db.getConnection_name().equalsIgnoreCase("dwhrep")) {
					dwhrep = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(), db.getDriver_name(),
							"DWHMgr", true);
				} else if (db.getConnection_name().equalsIgnoreCase("dwh")) {
					dwh = new RockFactory(db.getConnection_string(), db.getUsername(), db.getPassword(), db.getDriver_name(),
							"DWHMgr", true);
				}

			} // for each Meta_databases

			if (dwh == null) {
				throw new ConfigurationException("etlrep.Meta_databases", "dwh", ConfigurationException.Reason.MISSING);
			}

			if (dwhrep == null) {
				throw new ConfigurationException("etlrep.Meta_databases", "dwhrep", ConfigurationException.Reason.MISSING);
			}

		} catch (Exception e) {
			// Failure -> cleanup connections\

			try {
				dwhrep.getConnection().close();
			} catch (Exception se) {
			}

			try {
				dwh.getConnection().close();
			} catch (Exception ze) {
			}
			e.getMessage();
			log.log(Level.WARNING,"{0}", e.getMessage());
		}

		this.dwhrock = dwh;
		this.dwhreprock = dwhrep;

	}

	/**
	 * Updates aggregation status according the dependencies in the
	 * aggregationRules -table
	 * 
	 * 
	 * @param status
	 *          what is status column changed to
	 * @param aggregation
	 *          aggregations name where aggregation rule iteration and status
	 *          update is started.
	 * @param longdate
	 *          Date of the aggregation
	 * @throws Exception
	 */
	public void reAggregate(final String status, final String aggregation, final long longdate) throws ParseException {

		try {

			log.fine("Retriving target types and levels");
			final List<AggregationRule> aggRulList = AggregationRuleCache.getCache().getAggregationRules(aggregation);

			if (aggRulList != null) {

				final long start = System.currentTimeMillis();

				for (AggregationRule aggRul : aggRulList) {

					final String target_type = aggRul.getTarget_type();
					final String target_level = aggRul.getTarget_level();

					log.fine("ReAggregate: " + target_type + "/" + target_level);
					final Date day = new Date(longdate);
					reAggregate_NoClose(status, target_type, target_level, sdf.format(day));

				}

				log.finest("reAggregate TIME: " + (System.currentTimeMillis() - start));

			}

		} finally {

			try {
				dwhreprock.getConnection().close();
			} catch (Exception e) {
			}

			try {
				dwhrock.getConnection().close();
			} catch (Exception e) {
			}
		}

	}

	/**
	 * 
	 * Updates aggregation status according the dependencies in the
	 * aggregationRules -table
	 * 
	 * @param status
	 *          what is status column changed to
	 * @param type
	 *          source_type of the aggregation or aggregations where the update is
	 *          started.
	 * @param level
	 *          source_level of the aggregation or aggregations where the update
	 *          is started.
	 * @param longdate
	 *          Date of the aggregation
	 * @throws Exception
	 */
	private void reAggregate_NoClose(final String status, final String type, final String level,
			final String aggDateString) throws ParseException {

		final long starta = System.currentTimeMillis();

		final Date curDate = today(new Date(System.currentTimeMillis()));
		final Set<String> visited = new HashSet<String>();
		final Set<Map<String, String>> aggregations = new HashSet<Map<String, String>>();
		final String lastWeekString = lastWeek(aggDateString);
		final String lastMonthString = lastMonth(aggDateString);
		final Date lastWeek = lastWeek(curDate);
		final Date lastMonth = lastMonth(curDate);
		final Date aggDate = sdf.parse(aggDateString);

		final long startb = System.currentTimeMillis();
		aggregations.addAll(getRules(type, level, visited, ""));
		log.finest("getRules TIME: " + (System.currentTimeMillis() - startb));

		for (Map<String, String> map : aggregations) {

			final String aggregation = (String) map.get("aggregation");
			final String aggregationscope = (String) map.get("aggregationscope");

			String dateString = "";

			if (aggregationscope.equalsIgnoreCase("day")) {

				dateString = aggDateString;

			} else if (aggregationscope.equalsIgnoreCase("week")) {

				if (lastWeek.getTime() > aggDate.getTime()) {
					dateString = lastWeekString;
				} else {
					log.info("Week Aggregation " + aggregation + " skipped for ongoing week " + aggDateString);
				}

			} else if (aggregationscope.equalsIgnoreCase("month")) {

				if (lastMonth.getTime() > aggDate.getTime()) {
					dateString = lastMonthString;
				} else {
					log.info("Month Aggregation " + aggregation + " skipped for ongoing month " + aggDateString);
				}

			} else {

				log.warning("Unknown aggregationsope " + aggregationscope);

			}

			final long start = System.currentTimeMillis();

			if (dateString.length() > 0) {
				final AggregationStatus aggSta = AggregationStatusCache.getStatus(aggregation, sdf.parse(dateString).getTime());
				// do not change if status already the same
				if (aggSta != null) {
					if (!aggSta.STATUS.equals(status)) {
						aggSta.STATUS = status;
						AggregationStatusCache.setStatus(aggSta);
						log.info("Aggregation: " + aggregation + " status changed to: " + status);

					} else {
						log.info("Aggregation: " + aggregation + " status allready  " + status);
					}
				} else {
					log.warning("Aggregation " + aggregation + " at " + dateString + " NOT found.");
				}
			}

			log.finest("AggregationStatusCache TIME: " + (System.currentTimeMillis() - start));

		}

		log.finest("reAggregate_NoClose TIME: " + (System.currentTimeMillis() - starta));
	}

	public void reAggregate(final String status, final String type, final String level, final String date)
			throws ParseException {

		try {

			reAggregate_NoClose(status, type, level, date);

		} finally {

			try {
				dwhreprock.getConnection().close();
			} catch (Exception e) {
			}

			try {
				dwhrock.getConnection().close();
			} catch (Exception e) {
			}
		}

	}

	private String lastWeek(final String str) throws ParseException {
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setFirstDayOfWeek(firstDayOfTheWeek);
		cal.setMinimalDaysInFirstWeek(4);
		cal.setTime(sdf.parse(str.replaceAll("'", "")));
		cal.set(Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek());
		return sdf.format(cal.getTime());
	}

	private String lastMonth(final String str) throws ParseException {
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setFirstDayOfWeek(firstDayOfTheWeek);
		cal.setTime(sdf.parse(str.replaceAll("'", "")));
		cal.add(Calendar.DAY_OF_MONTH, -(cal.get(Calendar.DAY_OF_MONTH) - 1));
		return sdf.format(cal.getTime());
	}

	private Date lastWeek(final Date time) {
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setFirstDayOfWeek(firstDayOfTheWeek);
		cal.setMinimalDaysInFirstWeek(4);
		cal.setTime(time);
		cal.set(Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek());
		return cal.getTime();
	}

	private Date lastMonth(final Date time) {
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setFirstDayOfWeek(firstDayOfTheWeek);
		cal.setTime(time);
		cal.add(Calendar.DAY_OF_MONTH, -(cal.get(Calendar.DAY_OF_MONTH) - 1));
		return cal.getTime();
	}

	private Date today(final Date time) {
		final GregorianCalendar cal = new GregorianCalendar();
		cal.setFirstDayOfWeek(firstDayOfTheWeek);
		cal.setTime(time);
		cal.set(GregorianCalendar.MILLISECOND, 0);
		cal.set(GregorianCalendar.SECOND, 0);
		cal.set(GregorianCalendar.MINUTE, 0);
		cal.set(GregorianCalendar.HOUR_OF_DAY, 0);
		return cal.getTime();
	}

	private Set<Map<String, String>> getRules(final String sourceType, final String sourceLevel,
			final Set<String> visited, final String tmp) {

		log.finest(tmp + "SourceType: " + sourceType + " SourceLevel: " + sourceLevel + "\n");

		visited.add(sourceType + sourceLevel);

		final Set<Map<String, String>> result = new HashSet<Map<String, String>>();

		log.fine("retrieving aggregation rules");
		final List<AggregationRule> aggRulList = AggregationRuleCache.getCache().getAggregationRules(sourceType,
				sourceLevel);

		if (aggRulList != null) {

			for (AggregationRule aggRul : aggRulList) {

				if (!visited.contains(aggRul.getTarget_type() + aggRul.getTarget_level())) {

					log.finest(tmp + "Found: " + aggRul.getAggregation());
					result.addAll(getRules(aggRul.getTarget_type(), aggRul.getTarget_level(), visited, tmp + "  "));

				}

				final Map<String, String> map = new HashMap<String, String>();
				map.put("aggregation", aggRul.getAggregation());
				map.put("aggregationscope", aggRul.getAggregationscope());
				result.add(map);

			}
		}

		log.finest("\n");
		return result;
	}

	/**
	 * for testing only
	 * 
	 * @param args
	 */
	public static void main(final String args[]) {

		try {

			final Logger log = Logger.getLogger("TEST");
			 ETLCServerProperties props =null;
				try {
					props = new ETLCServerProperties();
				} catch (IOException e) {
					
					log.warning("Properties is not initialised");
				}
			final String url = "jdbc:sybase:Tds:192.168.1.32:2641";

			final RockFactory rockFact = new RockFactory(url, props.getProperty(ENGINE_DB_USERNAME), props.getProperty(ENGINE_DB_PASSWORD), props.getProperty(ENGINE_DB_DRIVERNAME), "ETLEngThr", true);

			final ReAggregation ra = new ReAggregation(log, rockFact);
			ra.reAggregate("HEP", "a11", 0);

		} catch (Exception e) {
			System.out.println(e);
		}

	}

}
