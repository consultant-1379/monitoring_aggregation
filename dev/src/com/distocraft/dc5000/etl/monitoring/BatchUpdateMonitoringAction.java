package com.distocraft.dc5000.etl.monitoring;

import java.io.ByteArrayInputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;

/**
 * 
 * Updates monitoring and aggregation status tables
 * 
 * 
 */
public class BatchUpdateMonitoringAction extends UpdateMonitoringAction {

  public static final String SESSIONTYPE = "monitor";

  private SimpleDateFormat sdfLong = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private SimpleDateFormat sdfShort = new SimpleDateFormat("yyyy-MM-dd");

  protected Logger log;

  protected Logger sqlLog;

  protected String where = "";

  protected BatchUpdateMonitoringAction() {
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
  public BatchUpdateMonitoringAction(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
      final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact, final ConnectionPool connectionPool,
      final Meta_transfer_actions trActions, final Logger clog) throws Exception {

    super(version, collectionSetId, collection, transferActionId, transferBatchId, connectId, rockFact, connectionPool,
        trActions, clog);

    this.where = trActions.getWhere_clause();

    log = Logger.getLogger(clog.getName() + ".BatchUpdateMonitoring");
    sqlLog = Logger.getLogger("sql" + log.getName().substring(4));

  }

  /**
   * Executes a SQL procedure
   * 
   */

  public void execute() throws EngineException {

    final String sqlClause = "";
    int thresholdHour = 0;
    String startDateString = getDateString();
    String endDateString = getDateString();
    final Properties properties = new Properties();

    try {
      if (where != null && where.length() > 0) {

        try {

          final ByteArrayInputStream bais = new ByteArrayInputStream(where.getBytes());
          properties.load(bais);
          bais.close();

          thresholdHour = Integer.parseInt(properties.getProperty("threshold", "0"));

          startDateString = "'" + properties.getProperty("startDate", getDateString()) + "'";

          endDateString = "'" + properties.getProperty("endDate", getDateString()) + "'";

        } catch (Exception e) {

          log.log(Level.SEVERE, "Invalid configuration", e);

          throw new EngineException("Error loading where clause", new String[] { this.getTrActions()
              .getAction_contents() }, e, this, this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
        }

        try {

          final String info = collection.getScheduling_info();
          final ByteArrayInputStream bais = new ByteArrayInputStream(info.getBytes());
          properties.load(bais);
          bais.close();

          thresholdHour = Integer.parseInt(properties.getProperty("threshold", Integer.toString(thresholdHour)));

          startDateString = "'" + properties.getProperty("startDate", startDateString) + "'";

          endDateString = "'" + properties.getProperty("endDate", endDateString) + "'";

        } catch (Exception e) {

          log.log(Level.SEVERE, "Error loading scheduling info", e);

          throw new EngineException("Error loading scheduling info", new String[] { this.getTrActions()
              .getAction_contents() }, e, this, this.getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
        }

      }

      log.fine("Started");

      final GregorianCalendar start = new GregorianCalendar();
      start.setTime(sdfLong.parse(startDateString.replaceAll("'", "")));

      final GregorianCalendar end = new GregorianCalendar();
      end.setTime(sdfLong.parse(endDateString.replaceAll("'", "")));

      while (!start.after(end)) {

        log.info("starting updateMonitoring: " + sdfLong.format(start.getTime()));
        update(2,7,7,7,7,1, thresholdHour, 0, "'" + sdfShort.format(start.getTime()) + " 00:00:00'", "'"
            + sdfLong.format(start.getTime()) + "'");
        start.add(Calendar.DATE, 1);

      }

      log.fine("Finished");

    } catch (Exception e) {
      log.log(Level.SEVERE, "BatchUpdateMonitoring failed exceptionally", e);
      throw new EngineException(EngineConstants.CANNOT_EXECUTE,
          new String[] { this.getTrActions().getAction_contents() }, e, this, this.getClass().getName(),
          EngineConstants.ERR_TYPE_EXECUTION);
    }
  }

}
