package com.distocraft.dc5000.etl.monitoring;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.log4j.PatternLayout;
import org.apache.log4j.net.SyslogAppender;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.common.HtmlEntities;
import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.SetContext;
import com.distocraft.dc5000.etl.engine.structure.TransferActionBase;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_servers;
import com.distocraft.dc5000.etl.rock.Meta_serversFactory;
import com.distocraft.dc5000.etl.rock.Meta_system_monitors;
import com.distocraft.dc5000.etl.rock.Meta_system_monitorsFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.ericsson.eniq.common.VelocityPool;

public class SystemMonitorAction extends TransferActionBase {

  private Logger log = Logger.getLogger("monitoring.");

  private String alarmTemplatePath = "/dc/dc5000/conf/system_alarm_templates/";

  private String alarmTemplateName = new String("");

  private String alarmFileOutputDirectory = new String("");

  private String alarmFilenamePattern = new String("");

  private String syslogTemplateName = new String("");

  private String syslogHost = new String("");

  private String syslogFacilityString = new String("");

  private String syslogPriority = new String("");

  private Properties configuration = null;

  private RockFactory rockFactory = null;

  public SystemMonitorAction(final Meta_versions version, final Long collectionSetId, final Meta_collections collection,
      final Long transferActionId, final Long transferBatchId, final Long connectId, final RockFactory rockFact,
      final Meta_transfer_actions trActions, final SetContext setContext, final Logger clog) throws EngineMetaDataException {

  	this.log = Logger.getLogger(clog.getName() + ".SystemMonitor");
  	this.configuration = TransferActionBase.stringToProperties(trActions.getAction_contents());
  }

  public void execute() throws EngineException {
    try {

      final Meta_system_monitors whereMetaSystemMonitor = new Meta_system_monitors(this.rockFactory);
      final Meta_system_monitorsFactory metaSystemMonitorFactory = new Meta_system_monitorsFactory(this.rockFactory,
          whereMetaSystemMonitor);
      final Vector metaSystemMonitorsVector = metaSystemMonitorFactory.get();
      final Iterator metaSystemMonitorsIterator = metaSystemMonitorsVector.iterator();

      while (metaSystemMonitorsIterator.hasNext()) {
        final Meta_system_monitors currentMetaSystemMonitor = (Meta_system_monitors) metaSystemMonitorsIterator.next();
        // Get the previous status before executing action.
        final String oldSystemMonitorStatus = currentMetaSystemMonitor.getStatus();

        String newSystemMonitorStatus = new String("ERROR");

        if (currentMetaSystemMonitor.getType().equalsIgnoreCase("CONNECTION") == true
            || currentMetaSystemMonitor.getType().equalsIgnoreCase("SQL") == true) {
          // System monitor is a local system monitor.
          newSystemMonitorStatus = this.executeLocalSystemMonitor(currentMetaSystemMonitor);

        } else if (currentMetaSystemMonitor.getType().equalsIgnoreCase("DISK") == true
            || currentMetaSystemMonitor.getType().equalsIgnoreCase("SERVER") == true) {
          // System monitor is a remote system monitor.
          newSystemMonitorStatus = this.executeRemoteSystemMonitor(currentMetaSystemMonitor);
        } else {
          this.log.severe("SystemMonitorAction.execute() system monitor " + currentMetaSystemMonitor.getMonitor()
              + " has an unknown type. Exiting...");
          return;
        }

        currentMetaSystemMonitor.setStatus(newSystemMonitorStatus);
        final Date currentTime = new Date();
        currentMetaSystemMonitor.setExecuted(new Timestamp(currentTime.getTime()));

        if (!oldSystemMonitorStatus.equalsIgnoreCase(newSystemMonitorStatus)) {
          this.alarmTemplateName = this.configuration.getProperty("alarmTemplate");
          this.alarmFileOutputDirectory = this.configuration.getProperty("alarmFileOutputDirectory");
          this.alarmFilenamePattern = this.configuration.getProperty("alarmFilenamePattern");

          // Check if a system monitor alarm needs to be created.
          if (this.alarmFileOutputDirectory != null && !this.alarmFileOutputDirectory.equalsIgnoreCase("")) {
            // Alarm template is set. Create the system monitor alarm with the specified template.
            final SimpleDateFormat usedFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            final String executedString = usedFormat.format(currentTime);
            // Create the Velocity context and set it's references.
            final VelocityContext context = new VelocityContext();
            context.put("oldSystemMonitorStatus", oldSystemMonitorStatus);
            context.put("newSystemMonitorStatus", newSystemMonitorStatus);
            context.put("systemMonitor", currentMetaSystemMonitor.getMonitor());
            context.put("systemMonitorExecuted", executedString);
            context.put("systemMonitorHost", currentMetaSystemMonitor.getHostname());
            context.put("systemMonitorType", currentMetaSystemMonitor.getType());

            String systemMonitorMessage = new String("");
            systemMonitorMessage = createSystemMonitorMessage(context, this.alarmTemplateName);

            if (systemMonitorMessage.equalsIgnoreCase("")) {
              this.log.log(Level.SEVERE,
                  "Creating systemMonitorMessage message failed. Empty system monitor message was not created.");
              return;
            }

            // Save the file to outputfolder.
            BufferedWriter out = null;
            try {
              final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");

              String filename = this.alarmFilenamePattern;
              // Replace the "$"-character with parsed datestring.
              if (filename.indexOf("$") >= 0) {
                final String dateString = dateFormat.format(new Date());
                filename = filename.replaceFirst("\\$", dateString); // $ is a special character in regexp.
              }
              // Replace the "@"-character with host name.
              if (filename.indexOf("@") >= 0) {
                filename = filename.replaceFirst("@", currentMetaSystemMonitor.getHostname());
              }
              // Replace the "#"-character with system monitor name.
              if (filename.indexOf("#") >= 0) {
                filename = filename.replaceFirst("#", currentMetaSystemMonitor.getMonitor());
              }

              if (!this.alarmFileOutputDirectory.endsWith(File.separator)) {
                this.alarmFileOutputDirectory += File.separator;
              }

              final String filepath = this.alarmFileOutputDirectory + filename;
              out = new BufferedWriter(new FileWriter(filepath));
              // Write the file to outputpath.
              out.write(systemMonitorMessage);
              this.log.fine("SystemMonitorAction.execute: System Monitor alarm file " + filename
                  + " written succesfully.");
              out.close();
            } catch (Exception e) {
              this.log.log(Level.WARNING, "SystemMonitorAction.execute: System Monitor alarm file write error.", e);
            } finally {
              if (out != null) {
                try {
                  out.close();
                } catch (IOException e) {
                  this.log.log(Level.SEVERE, "SystemMonitorAction.execute: IOException", e);
                }
              }
            }
          }

          // Check if the syslog needs to be sent.
          syslogHost = configuration.getProperty("syslogHost");
          if (syslogHost != null && !syslogHost.equalsIgnoreCase("")) {
            this.syslogPriority = this.configuration.getProperty("syslogPriority");
            this.syslogTemplateName = this.configuration.getProperty("syslogTemplate");

            this.log.log(Level.INFO, "Trying to send syslog message.");
            final org.apache.log4j.Level syslogLevel = org.apache.log4j.Level.toLevel(syslogPriority);

            // Send the syslog of this changed status of this system monitor.
            // First create the layout used in syslog messages.
            // %-9p = Syslog priority with 9 characters and left positioning.
            // %t = name of the thread that generated the logging event.
            // %d{} = used date format for the time of event.
            // %m = message provided by the application
            // %n = platform dependent line separator character or characters.
            final PatternLayout usedLayout = new PatternLayout("%-9p [%t]: %d{yyyy-MM-dd HH:mm:ss} %m%n");
            syslogFacilityString = configuration.getProperty("syslogFacility");

            final int sysLogFacilityInt = SystemMonitorAction.convertToFacilityInt(syslogFacilityString);

            // Create syslogAppender which sends the messages to the host.
            final SyslogAppender syslogAppender = new SyslogAppender(usedLayout, syslogHost, sysLogFacilityInt);

            final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("some_logger");

            final Date now = new Date();

            final SimpleDateFormat usedFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            final String executedString = usedFormat.format(currentTime);

            // Create the Velocity context and set it's references.
            final VelocityContext context = new VelocityContext();
            context.put("oldSystemMonitorStatus", oldSystemMonitorStatus);
            context.put("newSystemMonitorStatus", newSystemMonitorStatus);
            context.put("systemMonitor", currentMetaSystemMonitor.getMonitor());
            context.put("systemMonitorExecuted", executedString);
            context.put("systemMonitorHost", currentMetaSystemMonitor.getHostname());
            context.put("systemMonitorType", currentMetaSystemMonitor.getType());

            final String syslogMessage = createSystemMonitorMessage(context, this.syslogTemplateName);
            if (syslogMessage.equalsIgnoreCase("")) {
              this.log.log(Level.SEVERE,
                  "Creating syslog message failed. Empty syslog message was not sent to remote server.");
              return;
            }

            // TODO: What should this be?
            final String fqnOfCategoryClass = "DC5000";

            // Create the LoggingEvent which has details of the syslog event.
            final LoggingEvent loggingEvent = new LoggingEvent(fqnOfCategoryClass, logger, now.getTime(), syslogLevel,
                syslogMessage, null);

            // Send the syslog message.
            syslogAppender.append(loggingEvent);
            this.log.log(Level.INFO, "Syslog message sent to remote server.");
            this.log.log(Level.INFO, "Syslog details: syslogMessage = " + syslogMessage + " || " + "syslogPriority = "
                + syslogPriority + " || " + "syslogFacility = " + syslogFacilityString + " || " + "syslogHost = "
                + syslogHost);
          }

          this.log.log(Level.INFO, "System monitor status changed from " + oldSystemMonitorStatus + " to "
              + newSystemMonitorStatus + ".");
        }

        // Save changes to database.
        currentMetaSystemMonitor.updateDB();

      }

    } catch (Exception ex) {
      log.log(Level.SEVERE, "SystemMonitorAction failed exceptionally", ex);
      throw new EngineException("Exception in SystemMonitorAction", new String[] { "" }, ex, this, this.getClass()
          .getName(), EngineConstants.ERR_TYPE_SYSTEM);

    }
  }

  /**
   * This function executes one of the local system monitors like "CONNECTION", "SQL" etc.
   * @param metaSystemMonitor The target system monitor to execute.
   * @return Returns string containing new status of the system monitor. In case of error "ERROR" status is returned.
   */
  private String executeLocalSystemMonitor(final Meta_system_monitors metaSystemMonitor) {
    String newStatusString = "ERROR";

    final Properties systemMonitorConfiguration = getSystemMonitorConfiguration(metaSystemMonitor);

    if (metaSystemMonitor.getType().equalsIgnoreCase("CONNECTION") == true
        || metaSystemMonitor.getType().equalsIgnoreCase("SQL") == true) {
      // System monitor is db connection type.
      final String connectionName = systemMonitorConfiguration.getProperty("connection_name");
      this.log.info("Trying to establish a connection to " + connectionName);
      String query = new String("select getdate();");
      if (metaSystemMonitor.getType().equalsIgnoreCase("SQL")) {
        query = systemMonitorConfiguration.getProperty("query");
        if (query == null) {
          this.log.log(Level.SEVERE, "System monitor " + metaSystemMonitor.getMonitor() + " of type "
              + metaSystemMonitor.getType() + " has no required parameter \"query\". Exiting...");
          return "ERROR";
        }
      } else {
        if (systemMonitorConfiguration.getProperty("query") != null) {
          query = systemMonitorConfiguration.getProperty("query");
        }
      }

      final Meta_databases whereMetaDatabases = new Meta_databases(this.rockFactory);
      whereMetaDatabases.setConnection_name(connectionName);
      whereMetaDatabases.setType_name("USER");
      try {
        final Meta_databasesFactory metaDatabasesFactory = new Meta_databasesFactory(this.rockFactory, whereMetaDatabases);
        final Vector metaDatabases = metaDatabasesFactory.get();
        final Iterator metaDatabasesIterator = metaDatabases.iterator();
        if (metaDatabasesIterator.hasNext()) {
          final Meta_databases targetMetaDatabase = (Meta_databases) metaDatabasesIterator.next();
          Connection connection = null;
          Statement statement = null;
          try {
            Class.forName(targetMetaDatabase.getDriver_name());
            connection = DriverManager.getConnection(targetMetaDatabase.getConnection_string(), targetMetaDatabase
                .getUsername(), targetMetaDatabase.getPassword());
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            this.log.log(Level.INFO, "Executing query: " + query);
            final ResultSet resultSet = statement.executeQuery(query);

            if (metaSystemMonitor.getType().equalsIgnoreCase("SQL") == true) {
              if (resultSet.next()) {
                if (resultSet.getInt(1) != 0) {
                  newStatusString = "ACTIVE";
                } else {
                  newStatusString = "ERROR";
                }
              }
            } else if (metaSystemMonitor.getType().equalsIgnoreCase("CONNECTION")) {
              if (resultSet.next()) {
                // The sql query returned at least something, so return active status.
                newStatusString = "ACTIVE";
              }
            }

            statement.close();
            connection.commit();
            connection.setAutoCommit(true);
          } catch (SQLException se) {
            this.log.log(Level.INFO, "System monitor " + metaSystemMonitor.getMonitor() + " of type "
                + metaSystemMonitor.getType() + " failed to execute sql query. " + query, se);
            if (connection != null) {
              try {
                connection.rollback();
              } catch (SQLException ex) {
                this.log.log(Level.SEVERE, "Failed to execute connection.rollback()", ex);
              }
            }

          } catch (final Exception e) {
            this.log.log(Level.INFO, "System monitor " + metaSystemMonitor.getMonitor() + " of type "
                + metaSystemMonitor.getType() + " failed to execute.", e);
            if (connection != null) {
              try {
                connection.rollback();
              } catch (SQLException ex) {
                this.log.log(Level.SEVERE, "Failed to execute connection.rollback()", ex);
              }
            }
          } finally {
            if (statement != null) {
              try {
                statement.close();
              } catch (Exception e) {
              }
            }
            if (connection != null) {
              try {
                connection.close();
              } catch (Exception e) {
              }
            }
          }
        }
      } catch (Exception e) {
        this.log.log(Level.SEVERE, "Failed to create Meta_databasesFactory", e);
      }

    } else {
      this.log.severe("SystemMonitorAction.executeLocalSystemMonitor encountered unknown system monitor type "
          + metaSystemMonitor.getType() + ".");
      return "error";
    }

    this.log.log(Level.INFO, "SystemMonitorAction.executeLocalSystemMonitor returned newStatusString "
        + newStatusString);
    return newStatusString;
  }

  private String executeRemoteSystemMonitor(final Meta_system_monitors metaSystemMonitor) {
    String newStatusString = "ERROR";
    this.log.info("Starting to execute remote system monitor " + metaSystemMonitor.getMonitor());
    try {
      if (metaSystemMonitor.getType().equalsIgnoreCase("SERVER") == true
          || metaSystemMonitor.getType().equalsIgnoreCase("DISK") == true) {
        final Meta_servers whereMetaServer = new Meta_servers(this.rockFactory);
        String metaServerHostName = new String("");
        // Get the server of this monitor. 
        if (metaSystemMonitor.getType().equalsIgnoreCase("SERVER") == true) {
          // If the system monitor is of type "SERVER" the server name is specified in the configuration parameter server.
          final Properties configuration = getSystemMonitorConfiguration(metaSystemMonitor);
          metaServerHostName = configuration.getProperty("server");
        } else if (metaSystemMonitor.getType().equalsIgnoreCase("DISK") == true) {
          metaServerHostName = metaSystemMonitor.getHostname();
        } else {
          // Server hostname not found. Set the variable as empty.
          metaServerHostName = "";
        }

        if (metaServerHostName != null && !metaServerHostName.equalsIgnoreCase("")) {
          whereMetaServer.setHostname(metaServerHostName);
        } else {
          this.log.log(Level.SEVERE, "Meta server hostname not found for system monitor "
              + metaSystemMonitor.getMonitor());
          this.log.log(Level.INFO, "SystemMonitorAction.executeRemoteSystemMonitor returned newStatusString ERROR");
          return "ERROR";
        }

        final Meta_serversFactory metaServersFactory = new Meta_serversFactory(this.rockFactory, whereMetaServer);
        final Vector metaServers = metaServersFactory.get();
        final Iterator metaServersIterator = metaServers.iterator();
        String statusUrl = new String("");

        if (metaServersIterator.hasNext()) {
          final Meta_servers targetMetaServer = (Meta_servers) metaServersIterator.next();
          statusUrl = targetMetaServer.getStatus_url();
          // System monitor of type DISK needs to send parameters in http request to the ServerStatusServlet.
          if (metaSystemMonitor.getType().equalsIgnoreCase("DISK") == true) {
            statusUrl += "?a=1"; // Add a dummy parameter, so that the rest of the request parameters can use the &-character as parameter separators. 
            statusUrl += this.getServerMonitorsQueryString(targetMetaServer);
          }
        }

        // Get the content of the ServerStatusServlet.
        String content = new String("");
        try {
          final URL url = new URL(statusUrl);
          this.log.log(Level.INFO, "Getting status of server from status URL: " + statusUrl);
          // Read all the text returned by the server
          final BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
          String line = new String("");
          while ((line = in.readLine()) != null) {
            content += line;
          }
          in.close();
          //return content;
        } catch (Exception e) {
          this.log.log(Level.SEVERE,
              "Error in SystemMonitorAction.executeRemoteSystemMonitor. Returning empty content.", e);
          content = "";
        }

        if (metaSystemMonitor.getType().equalsIgnoreCase("SERVER")) {
          final String serverStatusColor = parseServerStatusColorFromHtmlSource(content);
          if (serverStatusColor.equalsIgnoreCase("GREEN") == true) {
            newStatusString = "ACTIVE";
          } else {
            newStatusString = "ERROR";
          }

        } else if (metaSystemMonitor.getType().equalsIgnoreCase("DISK")) {
          final String monitorStatusColor = parseSystemMonitorStatusColorFromHtmlSource(content, metaSystemMonitor);
          if (monitorStatusColor.equalsIgnoreCase("GREEN") == true) {
            newStatusString = "ACTIVE";
          } else {
            newStatusString = "ERROR";
          }
        }

      } else {
        this.log.severe("SystemMonitorAction.executeRemoteSystemMonitor encountered unknown system monitor type "
            + metaSystemMonitor.getType() + ".");
        return "error";
      }
    } catch (Exception e) {

    }

    this.log.log(Level.INFO, "SystemMonitorAction.executeRemoteSystemMonitor returned newStatusString "
        + newStatusString);
    return newStatusString;
  }

  /**
   * This function creates the end of the query string to be sent to the ServerStatus servlet (ServerStatus.java).
   * @param targetMetaServer The server where monitors query is sent.
   * @return Returns the string to add to the query string.
   */
  private String getServerMonitorsQueryString(final Meta_servers targetMetaServer) {
    String serverMonitorsQueryString = new String("");
    try {
      final Meta_system_monitors whereMetaSystemMonitor = new Meta_system_monitors(this.rockFactory);
      whereMetaSystemMonitor.setHostname(targetMetaServer.getHostname());
      final Meta_system_monitorsFactory metaSystemMonitorsFactory = new Meta_system_monitorsFactory(this.rockFactory,
          whereMetaSystemMonitor);
      final Vector metaSystemMonitors = metaSystemMonitorsFactory.get();
      final Iterator metaSystemMonitorsIterator = metaSystemMonitors.iterator();
      while (metaSystemMonitorsIterator.hasNext()) {
        final Meta_system_monitors currentSystemMonitor = (Meta_system_monitors) metaSystemMonitorsIterator.next();
        this.log.log(Level.INFO, "getServerMonitorsQueryString iterating through system monitor "
            + currentSystemMonitor.getMonitor() + " of type " + currentSystemMonitor.getType());
        final Properties systemMonitorConfiguration = getSystemMonitorConfiguration(currentSystemMonitor);
        final Enumeration systemMonitorConfigurations = systemMonitorConfiguration.propertyNames();
        while (systemMonitorConfigurations.hasMoreElements()) {
          // Check for the type of the system monitor.
          final String propertyName = (String) systemMonitorConfigurations.nextElement();
          if (currentSystemMonitor.getType().equalsIgnoreCase("DISK") == true) {
            serverMonitorsQueryString += "&d."; // d = DISK
            serverMonitorsQueryString += currentSystemMonitor.getMonitor();
            serverMonitorsQueryString += "." + propertyName + "="
                + systemMonitorConfiguration.getProperty(propertyName);
          } else if (currentSystemMonitor.getType().equalsIgnoreCase("SERVER") == true) {
            // No additional parameters are needed for the type SERVER system monitor.
            // Do nothing here.
          } else {
            // No need to add parameters for other types of system monitors.
            this.log.log(Level.INFO, "Skipping system monitor " + currentSystemMonitor.getMonitor()
                + " because it's not remote server system monitor.");
            //return "";
          }
        }
      }

      this.log.log(Level.INFO, "serverMonitorsQueryString = " + serverMonitorsQueryString);
      return serverMonitorsQueryString;

    } catch (Exception e) {
      this.log.log(Level.SEVERE, "Error in SystemMonitorAction.getServerMonitorsQueryString", e);
      return "";

    }
  }

  /**
   * This function gets the configuration of a system monitor from the database.
   * @param targetSystemMonitor is the system monitor of which configuration is to be loaded.
   */
  private Properties getSystemMonitorConfiguration(final Meta_system_monitors targetSystemMonitor) {
    final Properties configuration = new Properties();

    final String configurationString = targetSystemMonitor.getConfiguration();

    if (configurationString != null && configurationString.length() > 0) {
      try {
        final ByteArrayInputStream bais = new ByteArrayInputStream(configurationString.getBytes());
        configuration.load(bais);
        bais.close();
        this.log.info("Configuration read");
      } catch (Exception e) {
        this.log.log(Level.SEVERE, "SystemMonitors.getSystemMonitorConfiguration. Error reading configuration.", e);
        return new Properties();
      }

    }

    return configuration;

  }

  /**
   * This function parses the status color of the server from the html source returned from the ServerStatusServlet.
   * @param htmlSource HTML format source returned from the ServerStatusServlet.
   * @return Returns the status color of the server. In case of error, "RED" is returned.
   */
  private String parseServerStatusColorFromHtmlSource(final String htmlSource) {
    String statusColor = new String("");
    final String start = "<!-- SERVER STATUS=\"";

    if (htmlSource.indexOf(start) == -1) {
      this.log
          .log(
              Level.INFO,
              "SystemMonitorAction.parseServerStatusColorFromHtmlSource could not found the start of the standard response of ServerStatusServlet. Returned RED as statuscolor.");
      return "RED";
    }

    final String tempString = htmlSource.substring(htmlSource.indexOf(start) + start.length(), htmlSource.length());
    statusColor = tempString.substring(0, tempString.indexOf("\""));
    this.log.log(Level.INFO, "SystemMonitorAction.parseServerStatusColorFromHtmlSource parsed color " + statusColor
        + " from the html source");
    return statusColor;
  }

  /**
   * This function parses the status color of a system monitor from the html source returned from the ServerStatusServlet.
   * @param htmlSource HTML format source returned from the ServerStatusServlet.
   * @param metaSystemMonitor The target system monitor to be parsed.
   * @return Returns the status color of system monitor. In case of error, "RED" is returned. 
   */
  private String parseSystemMonitorStatusColorFromHtmlSource(final String htmlSource, final Meta_system_monitors metaSystemMonitor) {
    String statusColor = new String("");
    final String start = "<!-- MONITOR=\"" + metaSystemMonitor.getMonitor() + "\" STATUS=\"";

    this.log.log(Level.INFO, "Trying to find start string <code>" + HtmlEntities.createHtmlEntities(start)
        + "</code> in server status html source.");
    this.log.log(Level.INFO, "Server status html source is:<br/> <code>" + HtmlEntities.createHtmlEntities(htmlSource)
        + "</code>");

    if (htmlSource.indexOf(start) == -1) {
      this.log
          .log(
              Level.INFO,
              "SystemMonitorAction.parseSystemMonitorStatusColorFromHtmlSource could not found the start of the system monitor tag from the response of ServerStatusServlet. Returned RED as statuscolor.");
      return "RED";
    }

    final String tempString = htmlSource.substring(htmlSource.indexOf(start) + start.length(), htmlSource.length());
    statusColor = tempString.substring(0, tempString.indexOf("\""));
    this.log.log(Level.INFO, "SystemMonitorAction.parseSystemMonitorStatusColorFromHtmlSource parsed color "
        + statusColor + " from the html source");
    return statusColor;
  }

  /**
   * This function converts the facility from string format to int format.
   * @param facilityString Facility in string format.
   * @return Returns the int value of facility.
   */
  static int convertToFacilityInt(final String facilityString) {
    Integer facilityInteger = new Integer(-1);
    if (facilityString.equalsIgnoreCase("USER") == true) {
      facilityInteger = new Integer(SyslogAppender.LOG_USER);
    } else if (facilityString.equalsIgnoreCase("KERN") == true) {
      facilityInteger = new Integer(SyslogAppender.LOG_KERN);
    } else if (facilityString.equalsIgnoreCase("MAIL") == true) {
      facilityInteger = new Integer(SyslogAppender.LOG_MAIL);
    } else if (facilityString.equalsIgnoreCase("DAEMON") == true) {
      facilityInteger = new Integer(SyslogAppender.LOG_DAEMON);
    } else if (facilityString.equalsIgnoreCase("AUTH") == true) {
      facilityInteger = new Integer(SyslogAppender.LOG_AUTH);
    } else if (facilityString.equalsIgnoreCase("SYSLOG") == true) {
      facilityInteger = new Integer(SyslogAppender.LOG_SYSLOG);
    } else if (facilityString.equalsIgnoreCase("LPR") == true) {
      facilityInteger = new Integer(SyslogAppender.LOG_LPR);
    } else if (facilityString.equalsIgnoreCase("NEWS") == true) {
      facilityInteger = new Integer(SyslogAppender.LOG_NEWS);
    } else if (facilityString.equalsIgnoreCase("UUCP") == true) {
      facilityInteger = new Integer(SyslogAppender.LOG_UUCP);
    } else if (facilityString.equalsIgnoreCase("CRON") == true) {
      facilityInteger = new Integer(SyslogAppender.LOG_CRON);
    } else if (facilityString.equalsIgnoreCase("AUTHPRIV") == true) {
      facilityInteger = new Integer(SyslogAppender.LOG_AUTHPRIV);
    } else if (facilityString.equalsIgnoreCase("FTP") == true) {
      facilityInteger = new Integer(SyslogAppender.LOG_FTP);
    }

    return facilityInteger.intValue();
  }

  /**
   * This function converts the facility int to string format.
   * @param facilityInt facility int.
   * @return Returns the facility in string format.
   */
  static String convertToFacilityString(final int facilityInt) {
    String facilityString = new String("");
    if (facilityInt == SyslogAppender.LOG_USER) {
      facilityString = "USER";
    } else if (facilityInt == SyslogAppender.LOG_KERN) {
      facilityString = "KERN";
    } else if (facilityInt == SyslogAppender.LOG_MAIL) {
      facilityString = "MAIL";
    } else if (facilityInt == SyslogAppender.LOG_DAEMON) {
      facilityString = "DAEMON";
    } else if (facilityInt == SyslogAppender.LOG_AUTH) {
      facilityString = "AUTH";
    } else if (facilityInt == SyslogAppender.LOG_SYSLOG) {
      facilityString = "SYSLOG";
    } else if (facilityInt == SyslogAppender.LOG_LPR) {
      facilityString = "LPR";
    } else if (facilityInt == SyslogAppender.LOG_NEWS) {
      facilityString = "NEWS";
    } else if (facilityInt == SyslogAppender.LOG_UUCP) {
      facilityString = "UUCP";
    } else if (facilityInt == SyslogAppender.LOG_CRON) {
      facilityString = "CRON";
    } else if (facilityInt == SyslogAppender.LOG_AUTHPRIV) {
      facilityString = "AUTHPRIV";
    } else if (facilityInt == SyslogAppender.LOG_FTP) {
      facilityString = "FTP";
    }

    return facilityString;
  }

  /**
   * This function creates the system monitor message by using the selected template.
   * The system monitor message is returned.
   * @param context is an instance of VelocityContext, which contains the variables and their variables to be used in Velocitytemplate.
   * @param templateName is the name of the template used by Velocity.
   * @return Returns the syslog message.
   */
  private String createSystemMonitorMessage(final VelocityContext context, final String templateName) {
    String syslogMessage = new String("");

    VelocityEngine velocityEngine = null;
    
    try {
      final Properties velocityProperties = new Properties();
      velocityProperties.put("file.resource.loader.path", this.alarmTemplatePath);
      
      velocityEngine = VelocityPool.reserveEngine();
      
      try {
        velocityEngine.init(velocityProperties);
      } catch (Exception e) {
        this.log.log(Level.WARNING, "Velocity initialization failed.", e);
      }
      final StringWriter writer = new StringWriter();
      velocityEngine.mergeTemplate(templateName, Velocity.ENCODING_DEFAULT, context, writer);
      syslogMessage = writer.toString();
      return syslogMessage;

    } catch (Exception e) {
      this.log.log(Level.SEVERE,
          "SystemMonitorAction.createSyslogMessage failed. Returning empty syslog message. Exception: ", e);
      return "";
    } finally {
    	VelocityPool.releaseEngine(velocityEngine);
    }
  }

}
