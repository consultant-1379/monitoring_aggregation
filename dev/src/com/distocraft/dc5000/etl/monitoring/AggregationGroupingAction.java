package com.distocraft.dc5000.etl.monitoring;

import com.distocraft.dc5000.etl.engine.sql.SQLActionExecute;
import com.distocraft.dc5000.etl.engine.connect.ConnectionPool;
import com.distocraft.dc5000.etl.engine.common.EngineMetaDataException;
import com.distocraft.dc5000.etl.engine.common.EngineException;
import com.distocraft.dc5000.etl.engine.common.EngineConstants;
import com.distocraft.dc5000.etl.rock.Meta_versions;
import com.distocraft.dc5000.etl.rock.Meta_collections;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actions;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.etl.rock.Meta_databasesFactory;
import com.distocraft.dc5000.etl.rock.Meta_transfer_actionsFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementcolumn;
import com.distocraft.dc5000.repository.dwhrep.MeasurementcolumnFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementcounter;
import com.distocraft.dc5000.repository.dwhrep.MeasurementcounterFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhtype;
import com.distocraft.dc5000.repository.dwhrep.DwhtypeFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementtype;
import com.distocraft.dc5000.repository.dwhrep.MeasurementtypeFactory;
import com.distocraft.dc5000.repository.dwhrep.Dwhcolumn;
import com.distocraft.dc5000.repository.dwhrep.DwhcolumnFactory;
import com.distocraft.dc5000.repository.dwhrep.Busyhour;
import com.distocraft.dc5000.repository.dwhrep.BusyhourFactory;
import com.distocraft.dc5000.repository.dwhrep.Measurementkey;
import com.distocraft.dc5000.repository.dwhrep.MeasurementkeyFactory;
import com.distocraft.dc5000.repository.cache.PhysicalTableCache;
import com.distocraft.dc5000.common.StaticProperties;
import com.ericsson.eniq.common.VelocityPool;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

import java.util.GregorianCalendar;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.Date;
import java.util.Calendar;
import java.util.Locale;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

/**
 * This Action is used to perform the Busy Hour Group By Time/Node functionality
 * 
 * @deprecated: 20110809 eanguan :: This class is no more useful after implementation of CR : Remove Grouping :: CR 52/109 18-FCP 103 8147/14
 * 
 */
@Deprecated
public class AggregationGroupingAction extends SQLActionExecute {
	
	
    final SimpleDateFormat dateTimeFormatter = new SimpleDateFormat(DATETIME_FORMAT_DEF, Locale.getDefault());
    final SimpleDateFormat dateFormatter = new SimpleDateFormat(DATE_FORMAT_DEF, Locale.getDefault());
	
	public static final String GROUPPUBLICTIME =     "MIN_ID=OFFSET;DATE_ID=min(DATE_ID);HOUR_ID=min(HOUR_ID);DAY_ID=min(DAY_ID);WEEK_ID=min(WEEK_ID);MONTH_ID=min(MONTH_ID);YEAR_ID=min(YEAR_ID);BUSYHOUR=MIN(datepart(hour,CAST($CALCTABLE.DATE_ID || ' ' || $CALCTABLE.BUSYHOUR || ':' || $CALCTABLE.MIN_ID AS TIMESTAMP))) as BUSYHOUR";	
	public static final String GROUPPUBLICTIMENODE = "MIN_ID=OFFSET;DATE_ID=min(DATE_ID);HOUR_ID=min(HOUR_ID);DAY_ID=min(DAY_ID);WEEK_ID=min(WEEK_ID);MONTH_ID=min(MONTH_ID);YEAR_ID=min(YEAR_ID);BUSYHOUR=MIN(datepart(hour,CAST($CALCTABLE.DATE_ID || ' ' || $CALCTABLE.BUSYHOUR || ':' || $CALCTABLE.MIN_ID AS TIMESTAMP))) as BUSYHOUR";
	public static final String GROUPPUBLICNODE =                   "DATE_ID=min(DATE_ID);HOUR_ID=min(HOUR_ID);DAY_ID=min(DAY_ID);WEEK_ID=min(WEEK_ID);MONTH_ID=min(MONTH_ID);YEAR_ID=min(YEAR_ID);BUSYHOUR=MIN(datepart(hour,CAST($CALCTABLE.DATE_ID || ' ' || $CALCTABLE.BUSYHOUR || ':' || $CALCTABLE.MIN_ID AS TIMESTAMP))) as BUSYHOUR";
	
	public static final String GROUPKEYTIME = "OSS_ID;DCVECTOR_INDEX;Service;VectorColumn";		
	public static final String GROUPKEYNODE = "OSS_ID;DCVECTOR_INDEX;Service;VectorColumn";	
	public static final String GROUPKEYTIMENODE = "OSS_ID;DCVECTOR_INDEX;Service;VectorColumn";	
	
	public static final String GROUP_BY = "GROUP BY";
	
	public static final String EXTRASELECTNAME = "tt";
	
	private List<String> groupPublicTimeList = null;
	private List<String> groupPublicNodeList = null;
	private List<String> groupKeyNodeList = null;
	private List<String> groupPublicTimeNodeList = null;
	private List<String> groupKeyTimeNodeList = null;

	private static final String groupTemplate = "DELETE FROM $targetTable $where_delete; INSERT INTO $targetTable ($keys_insert $publicColumns_insert $counters_insert) SELECT $keys_select $publicColumns_select $counters_select FROM $sourceTable $extra_select $where_select $groupby $keys_group $publicColumns_group";
	
    private static final String GROUP_PUBLIC_TIME_KEY = "group_public_time" ;
    private static final String GROUP_PUBLIC_NODE_KEY = "group_public_node";
    private static final String GROUP_PUBLIC_TIME_NODE_KEY = "group_public_time_node";
    private static final String GROUP_KEY_NODE_KEY = "group_key_node";
    private static final String GROUP_KEY_TIME_NODE_KEY = "group_key_time_node";
	
    private static final String _DAYBH = "_DAYBH";
    /**
     * The ending of the cal table name
     */
    private static final String _CALC_TABLE_POSTFIX = "_CALC";
    /**
     * Default Aggregation date format
     */
    private static final String DATETIME_FORMAT_DEF = "yyyy-MM-dd HH:mm:ss";
    private static final String DATE_FORMAT_DEF = "yyyy-MM-dd";

    /**
     * Key to get the type name that this grouping is being executed on, read from the meta_transfer_action's
     * where clause
     */
    public static final String KEY_TYPENAME = "typename";
    /**
     * Key to get the type name that this grouping is being executed on, read from the meta_transfer_action's
     * where clause
     */
    private static final String KEY_TABLETYPE = "tabletype";
    /**
     * Key to get the versionid
     */
    private static final String KEY_VERSIONID = "versionid";
    /**
     * Used to get the grouping aggregation date, if not set the day today is used
     */
    public static final String KEY_AGGDATE = "aggDate";

    /**
     * Connection to dwhrep
     */
    protected RockFactory dwhrepRock = null;
    /**
     * The logger
     */
    private Logger log;
    /**
     * The action properties from the meta_transfer_action's wheer clause
     */
    protected Properties actionProperties = null;
    
    protected Properties schedulingProperties = null;
    /**
     * Table cache used to get the proper partition to store the grouped results
     */
    private static PhysicalTableCache tableCache = null;

    /**
     * Default constructor, not used.
     */
    protected AggregationGroupingAction(){//NOPMD
    }

    /**
     * Constructor used be Engine
     * @param version THe version of the action
     * @param collectionSetId The actions collection_set_id
     * @param collection The actions collection_id
     * @param transferActionId The id of the transfer action
     * @param transferBatchId The batch ID
     * @param connectId The connection ID to use, dwhdh
     * @param etlrep The telrep connection from engine
     * @param connectionPool A connection pool
     * @param trActions The meta_transfer_action (includes the where clause)
     * @param batchColumnName not used
     * @param clog The parent logger
     * @throws Exception If the action details can be converted to a Properties set.
     */
    public AggregationGroupingAction(final Meta_versions version, final Long collectionSetId,//NOPMD
                                     final Meta_collections collection, final Long transferActionId,
                                     final Long transferBatchId, final Long connectId, final RockFactory etlrep,
                                     final ConnectionPool connectionPool, final Meta_transfer_actions trActions,
                                     final Logger clog) throws Exception {
    	
        super(version, collectionSetId, collection, transferActionId, transferBatchId,
                connectId, etlrep, connectionPool, trActions);
        final String actionContents = trActions.getAction_contents();

        getActionProperties(actionContents);//NOPMD
        log = Logger.getLogger(clog.getName() + ".AggregationGrouping");
        
    	groupPublicTimeList = toListArray(groupPublic_Time());  
    	groupPublicNodeList = toListArray(groupPublic_Node());	
    	groupPublicTimeNodeList = toListArray(groupPublic_TimeNode());	
    	groupKeyNodeList = toListArray(groupKey_Node());   	
    	groupKeyTimeNodeList = toListArray(groupKey_TimeNode());
    }

    /**
     * Get the temp daybh table, format is targetType + "_DAYBH_CALC"
     * @return the temporary daybh table
     * @throws Exception If typename isnt in hte meta transfer actions procperties
     */
    protected String getTempDayBhTable() throws Exception {
        return getTableType()+ _DAYBH + _CALC_TABLE_POSTFIX;
    }

    private String getVersionId() throws EngineMetaDataException {
        assert actionProperties != null;
        final String versionId = actionProperties.getProperty(KEY_VERSIONID); // e.g. DC_E_RBS:((56))
        if(versionId == null || versionId.length() == 0){
            throw new EngineMetaDataException("No versionid meta info found", null, getClass().getName() + ".getVersionId");
        }
        return versionId;
    }

    private String getTypeName() throws EngineMetaDataException {
        assert actionProperties != null;
        final String typeName = actionProperties.getProperty(KEY_TYPENAME); // e.g. DC_E_RBS_IUBBH
        if(typeName == null || typeName.length() == 0){
            throw new EngineMetaDataException("No versionid meta info found", null, getClass().getName() + ".getTypeName");
        }
        return typeName;
    }

    /**
     * Get the target type
     * @return The target type of hte daybh aggragation
     * @throws EngineMetaDataException If 'typename' cant be read from the transfer action
     */
    protected String getBhLevel() throws EngineMetaDataException {
        assert actionProperties != null;
        final String type_name = actionProperties.getProperty(KEY_TYPENAME); // e.g. DC_E_RBS_AICH
        if(type_name == null || type_name.length() == 0){
            throw new EngineMetaDataException("No typename meta info found", null, getClass().getName() + ".getTargetType");
        }
        return type_name;
    }
    /**
     * Get the target type
     * @return The target type of hte daybh aggragation
     * @throws EngineMetaDataException If 'typename' cant be read from the transfer action
     */
    private String getTableType() throws EngineMetaDataException {
        assert actionProperties != null;
        String tabletype = actionProperties.getProperty(KEY_TABLETYPE);
        if(tabletype == null){
            tabletype = getBhLevel();
        }
        return tabletype;
    }

    protected String groupPublic_Time(){
        return StaticProperties.getProperty(GROUP_PUBLIC_TIME_KEY, GROUPPUBLICTIME);
    }
    
    protected String groupPublic_TimeNode(){
        return StaticProperties.getProperty(GROUP_PUBLIC_TIME_NODE_KEY, GROUPPUBLICTIMENODE);
    }
    
    protected String groupPublic_Node(){
        return StaticProperties.getProperty(GROUP_PUBLIC_NODE_KEY, GROUPPUBLICNODE);
    }
    
    protected String groupKey_Node(){
        return StaticProperties.getProperty(GROUP_KEY_NODE_KEY, GROUPKEYNODE);
    }
    
    protected String groupKey_TimeNode(){
        return StaticProperties.getProperty(GROUP_KEY_TIME_NODE_KEY, GROUPKEYTIMENODE);
    }

    /**
     * Get the aggregation date in string format, If aggDate is set that is converted, otherwise the day
     * today is used as a default
     * @return the aggregation date in milliseconds
     */
    protected long getAggregationDate(){
        assert schedulingProperties != null;
        final String dateString = schedulingProperties.getProperty(KEY_AGGDATE);

        long longDate = -1;
        
		try {
			// try to parse from yyyy-MM-dd HH:mm:ss format
			longDate = dateTimeFormatter.parse(dateString).getTime();
		} catch (final Exception e) {
			// not in yyyy-MM-dd HH:mm:ss format
			longDate = -1;
		}

		try {
			// try to parse from yyyy-MM-dd format
			final Date date = dateFormatter.parse(dateString);
			longDate = date.getTime();
		} catch (final Exception e) {
			// not in yyyy-MM-dd HH:mm:ss format
			longDate = -1;
		}

		if (longDate == -1) {
			try {
				// try to parse from long format
				final GregorianCalendar curCal = new GregorianCalendar();
				curCal.setTimeInMillis(Long.parseLong(dateString));
				longDate = Long.parseLong(dateString);
			} catch (final Exception e) {
				// not in long format
				// return default
		        return getDefaultAggregationDate().getTime();
			}
		}

		
		 return longDate;
    }

    /**
     * Get the default aggregation date i.e. today
     * @return Todate in a Date object
     */
    protected Date getDefaultAggregationDate(){
        final Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_MONTH, -1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        return cal.getTime();
    }

    /**
     * Convert the meta transfer action contents to a properties set
     * @param actionDetails Taken from meta_transfer_action.getWhereClause()
     */
    protected void getActionProperties(final String actionDetails) {
        actionProperties = new Properties();
        if (actionDetails != null){
        try {
          final ByteArrayInputStream bais = new ByteArrayInputStream(actionDetails.getBytes());
          actionProperties.load(bais);
          bais.close();
        } catch (Exception e) {
            log.warning("Couldnt convert action details to a property set : " + e.toString());
            //
        }
        }
    }

    /**
     * Convert the meta transfer action contents to a properties set
     * @param schedulingDetails Taken from meta_transfer_action.getWhereClause()
     */
    protected void getSchedulingProperties(final String schedulingDetails) {
    	schedulingProperties = new Properties();
        if (schedulingDetails != null){
        try {
          final ByteArrayInputStream bais = new ByteArrayInputStream(schedulingDetails.getBytes());
          schedulingProperties.load(bais);
          bais.close();
        } catch (Exception e) {
            log.warning("Couldnt convert action details to a property set : " + e.toString());
            //
        }
        }
    }
    
    /**
     * Get the table cache, used to figure out the correct partition to write the gouped results too
     * @return the correct partition to use
     */
    private PhysicalTableCache getTableCache(){
        if (tableCache == null) {
            tableCache = PhysicalTableCache.getCache();
        }
        return tableCache;
    }

    private Map<GroupType, List<String>> getTypeGroups(final List<String> bhTypes) throws Exception {
        final Map<GroupType, List<String>> typeGroupMap = new HashMap<GroupType, List<String>>();
        final String bhLevel = getBhLevel();
        for(String bhType : bhTypes){
            final GroupType gType = getGroupingType(bhType, bhLevel);
            final List<String> bhtypeList;
            if(typeGroupMap.containsKey(gType)){
                bhtypeList = typeGroupMap.get(gType);
            } else {
                bhtypeList = new ArrayList<String>();
                typeGroupMap.put(gType, bhtypeList);
            }
            bhtypeList.add(bhType);
        }
        return typeGroupMap;
    }
    /**
     * Execute the Grouping Action
     * @throws Exception
     */
    @Override
    public void execute() throws EngineException {
        
    	try{
        	
            getSchedulingProperties(collection.getScheduling_info());
            
            dwhrepRock = getDwhrepRock();
            final String tableToGroup = getTempDayBhTable();
            final List<String> bhTypes = getBhTypesToGroup(tableToGroup);
            if(bhTypes.isEmpty()){
                log.info("No BHTYPE's found in calc table, nothing to do....");
                return;
            }
            final Map<GroupType, List<String>> typeGroupMap = getTypeGroups(bhTypes);
            final String typeName = getTableType();
            final String storageId = getStorageId(getTableType()); // e.g. DC_E_RBS_AICH:DAYBH
            final String mTableId = getVersionId()+":"+getStorageId(getTableType()); // e.g. DC_E_RBS((1)):DC_E_RBS_AICH:DAYBH

            final List<Measurementcounter> countersToGroup = getCountersToGroup(typeName);
            if(countersToGroup.isEmpty()){ // no counters??
                log.fine("No Meas Counters found for type name " + typeName);
                return;
            }

            final List<Dwhcolumn> partitionColList = getPartitionColumnList(storageId);
            if(typeGroupMap.containsKey(GroupType.None)){
                final List<String> noGroupings = typeGroupMap.get(GroupType.None);
                noGrouping(countersToGroup, tableToGroup, partitionColList, storageId, noGroupings, mTableId);
            }
            if(typeGroupMap.containsKey(GroupType.Time)){
                final List<String> groupTypes = typeGroupMap.get(GroupType.Time);
                groupByTime(countersToGroup, tableToGroup, partitionColList, storageId, groupTypes, mTableId);
            }
            if(typeGroupMap.containsKey(GroupType.Node)){
                final List<String> groupTypes = typeGroupMap.get(GroupType.Node);
                groupByNode(countersToGroup, tableToGroup, partitionColList, storageId, groupTypes, mTableId);
            }
            if(typeGroupMap.containsKey(GroupType.NodeTime)){
                final List<String> groupTypes = typeGroupMap.get(GroupType.NodeTime);
                groupByNodeAndTime(countersToGroup, tableToGroup, partitionColList, storageId, groupTypes, mTableId);
            }
        } catch (EngineMetaDataException e){
            log.log(Level.SEVERE, "Error while executing groupings ", e);
            throw new EngineException(e.getMessage(), e, this,
                    getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
        } catch (EngineException e){
            log.log(Level.SEVERE, "Error while executing groupings ", e);
            throw e;
        } catch (Exception e){
            String msg = "Unknown error while executing groupings ";
            if(e.getCause() != null){
                msg = "Grouping Error : " + e.getCause().getMessage();
            }
            log.log(Level.SEVERE, msg, e);
            throw new EngineException(msg, e, this,
                    getClass().getName(), EngineConstants.ERR_TYPE_EXECUTION);
        } finally {
            close();
        }
    }

    private List<String> getBhTypesToGroup(final String inputTable) throws Exception {
    	final String sql = "select distinct(BHTYPE) from " + inputTable;
    	final List<String> list = new ArrayList<String>();
    	
    	Statement stmt = null;
    	ResultSet rs = null;
    	
    	try {
    		stmt = this.getConnection().getConnection().createStatement();
    		stmt.getConnection().commit();
    		rs = stmt.executeQuery(sql);
    		stmt.getConnection().commit();
    		
    		while(rs.next()){
    			list.add(rs.getString("BHTYPE"));
    		}
    		
    	} finally {
    		try {
    		  rs.close();
    		} catch (Exception e) {}
    		
    		try {
    			stmt.close();
    		} catch (Exception e) {}
    	}
    	
    	return list;
    }

    /**
     * Close the connection to dwhrep if its open
     */
    protected void close(){
        if(dwhrepRock != null){
            try{
                dwhrepRock.getConnection().close();
            } catch (Throwable t){/**/}//NOPMD
        }
    }

 
    /**
     * Get the columns used in the partitions for the storage id
     * @param storageId The storage id to search
     * @return A list of columns that make up the partitions
     * @throws Exception If there are any errors
     */
    protected List<Dwhcolumn> getPartitionColumnList(final String storageId) throws Exception {
        final Dwhcolumn where = new Dwhcolumn(dwhrepRock);
        where.setStorageid(storageId);
        final DwhcolumnFactory fac = new DwhcolumnFactory(dwhrepRock, where, " ORDER BY COLNUMBER");
        return fac.get();
    }

    /**
     * The real partition the group results shoule be written too.
     *
     * @param storageId The storage ID to search for
     * @param dateId The date the partition coverd
     * @return The partition that will contain the results of the grouping
     * @throws Exception If there are any errors
     */
    protected String getStoragePartition(final String storageId, final long dateId) throws Exception {
        return getTableCache().getTableName(storageId, dateId);
    }

    /**
     * Get the storage id for the target type
     * @param typeName The target type
     * @return The storage id
     * @throws Exception If there are any errors.
     */
    private String getStorageId(final String typeName) throws Exception {
        final Dwhtype where = new Dwhtype(dwhrepRock);
        where.setTypename(typeName);
        where.setTablelevel("DAYBH");
        final DwhtypeFactory fac1 = new DwhtypeFactory(dwhrepRock, where);
        final List<Dwhtype> list = fac1.get();
        final Dwhtype type = list.get(0);
        return type.getStorageid();
    }

    /**
     * Get the counters being groups and their assiciated grouping function e.g. SUM, AVG etc.
     * @param typeName the measurement type e.g. DC_E_RBS_AICH
     * @return List of counters (table columns) associated wit the meas type
     * @throws Exception If there are any errors
     */
    protected List<Measurementcounter> getCountersToGroup(final String typeName) throws Exception {
        final Measurementtype where = new Measurementtype(dwhrepRock);
        //20110126,eeoidiv,HN27667 change added VersionId to where clause, but not all have versionid property.
        //Set Version id if specified in Meta_Transfer_Action
        try {
        	final String versionId = getVersionId();
        	where.setVersionid(versionId);
        } catch(Exception e) { 
        	//getVersionId() throws an Exception if no id found, which we will ignore.
        }
        where.setTypename(typeName);
        final MeasurementtypeFactory fac = new MeasurementtypeFactory(dwhrepRock, where);
        final List<Measurementtype> mts = fac.get();
        final String typeId = mts.get(0).getTypeid(); // DC_E_RBS:((x)):DC_E_RBS_AICH
        final Measurementcounter msWhere = new Measurementcounter(dwhrepRock);
        msWhere.setTypeid(typeId);
        
        final MeasurementcounterFactory facc = new MeasurementcounterFactory(dwhrepRock, msWhere);
        final List<Measurementcounter> mcList = new ArrayList<Measurementcounter>();
        final List<Measurementcounter> measurementCounterList = facc.get();
        for(Measurementcounter mc: measurementCounterList){
        	if(mc.getFollowjohn() == null){
        		mcList.add(mc);
        	}
        	else{
        		log.fine("Ignoring the Follow John Counter:"+mc.getDataname()+ " from grouping.");
        	}
        	
        }
        return mcList;
    }



    /**
     * Is Grouping Enabled
     * @return TRUE is grouping is enabled, FALSE otherwise
     * @throws Exception If there are any errors.
     */
    public boolean isGroupingEnabled() throws Exception {
        final Meta_transfer_actions where = new Meta_transfer_actions(getRockFact());
        where.setCollection_id(getCollectionId());
        where.setCollection_set_id(getCollectionSetId());
        where.setTransfer_action_id(getTransferActionId());
        final Meta_transfer_actionsFactory fac = new Meta_transfer_actionsFactory(getRockFact(), where);
        final List actionList = fac.get();
        boolean isEnabled = false;
        if(!actionList.isEmpty()){
            final Meta_transfer_actions meta = (Meta_transfer_actions)actionList.get(0);
            isEnabled = meta.getEnabled_flag().equalsIgnoreCase("Y");
        }
        return isEnabled;
    }

    /**
     * Get the Group Type i.e. the placeholder name
     * @param bhType the placeholder ID/name
     * @param bhLevel Get busy hour level
     * @return The Grouping Type to use o nthe DAYBH table, can be Time, Node or NodeTime
     * @throws Exception If there were read errors
     */
    protected GroupType getGroupingType(final String bhType, final String bhLevel) throws Exception {
        final Busyhour where = new Busyhour(dwhrepRock);
        final String shortBhType = bhType.substring(bhType.indexOf('_')+1);
        where.setBhtype(shortBhType);
        where.setVersionid(getVersionId());
        where.setBhlevel(bhLevel);
        final BusyhourFactory bhFac = new BusyhourFactory(dwhrepRock, where);
        final List<Busyhour> types = bhFac.get();
        if(types.isEmpty()){
            return GroupType.None;
        } else {
            final String gType = types.get(0).getGrouping();
            return GroupType.getType(gType);
        }
    }
 
	private List<String> getElementColumns(final List<String> add) throws Exception {
		final Measurementkey where = new Measurementkey(dwhrepRock);
		where.setIselement(1);
		final String typeId = getVersionId() +":" + getTypeName();
		where.setTypeid(typeId);
		final MeasurementkeyFactory fac = new MeasurementkeyFactory(dwhrepRock, where);
		final List<Measurementkey> keys = fac.get();
		final List<String> result = new ArrayList<String> ();
		final Iterator<Measurementkey> iter = keys.iterator();
		while(iter.hasNext()){
			final String dName = iter.next().getDataname();
			result.add(dName);
		}
		
		result.addAll(add);
		return result;
	}
	
	  /**
     * If grouping is disabled, copy the contents of hte temp table to the real partition.
     * @param fromTable The aggragation temp table
     * @param storageId The daybh types storage id
     * @param bhTypes the placeholder ids/names
     * @throws Exception If there are any errors executing the sql.
     */
    private void noGrouping(final List<Measurementcounter> counterNames,
            final String tableName, final List<Dwhcolumn> partitionColList, final String storageId,
            final List<String> bhTypes, final String mTableId) throws Exception {
    	
        final long groupDateLong = getAggregationDate();
        final long start = System.currentTimeMillis();
        final String storagePartition = getStoragePartition(storageId, groupDateLong);
        
        //EEIKBE: Need to use the AGG_DATE column when performing the DELETE but it may not be 
        //there in older TechPacks. This checks to see if the column exists: 
        //YES, use AGG_DATE.
        //NO,  use DATE_ID.
        String colName = "DATE_ID";
        //Need to determine which column to use when deleting the data from the CALC table.
        if(isColumnExists(partitionColList, "AGG_DATE", storageId)){
        	colName = "AGG_DATE";
        }
        
        final VelocityContext context = new VelocityContext();
        context.put("dateid", dateFormatter.format(groupDateLong));
        context.put("targetTable", storagePartition);
        
        context.put("keys_insert", columnsToString(getKeyColumns(mTableId)));
        context.put("keys_select", columnsToString(getKeyColumns(mTableId), tableName));
        context.put("keys_group", "");

        context.put("publicColumns_insert", ", " + columnsToString(getPublicColumns(mTableId)));
        context.put("publicColumns_select", ", " + columnsToString(getPublicColumns(mTableId), tableName));
        context.put("publicColumns_group", "");
       
        context.put("counters_insert", ", " + getMeasCounters(counterNames));
        context.put("counters_select", ", " + getMeasCounters(counterNames, tableName));
      
       
        context.put("sourceTable", tableName);
        context.put("groupby", "");
       
        populateContext(tableName, bhTypes, mTableId, groupDateLong, colName,
				context);

		final String totalSQL = generateGroupSql(context);    
        final int rowsUpdated = executeSQLUpdate(totalSQL);
        
        log.finer("Copied on table " + tableName + " SQL:");
        log.finer(totalSQL);
        
        final long stop = System.currentTimeMillis();
        final String msg = "Copied "+storageId+":"+bhTypes+" data to " + rowsUpdated + " new rows in " + storagePartition + " in " + (stop - start)+"mSec";

        log.info(msg);
    }

	protected void populateContext(final String tableName,
			final List<String> bhTypes, final String mTableId,
			final long groupDateLong, final String colName,
			final VelocityContext context) throws Exception {
		final long startTime = System.currentTimeMillis();
		final String bhTypeFilter = generateTypeFilter(bhTypes);
        if(bhTypeFilter != null){
        	
        	final String tmp = "CAST("+tableName+".DATE_ID || ' ' || "+tableName+".BUSYHOUR || ':' || "+tableName+".MIN_ID AS TIMESTAMP)";
        	final String newDateCalc = " AND "+tmp+" >= start_timestamp AND "+tmp+" < end_timestamp";
        	final String dateCalc = " AND "+tableName+".DATE_ID='"+dateFormatter.format(groupDateLong)+"'";   	
            context.put("where_select", "WHERE " + generateTypeFilter(bhTypes, tableName) + newDateCalc + " AND "+tableName+".BHTYPE = "+EXTRASELECTNAME+".BHTYPE AND "+columnsToStringWithAnd(timeCalculationsWithNullCheck(getKeyNameAndDataType(mTableId), tableName, EXTRASELECTNAME)));
            context.put("where_delete", "WHERE " + bhTypeFilter + " AND "+colName+"='"+dateFormatter.format(groupDateLong)+"'");
            context.put("extra_select", ",( SELECT DISTINCT "+columnsToString(getKeyColumns(mTableId))+", BHTYPE, CAST(DATE_ID || ' ' || BUSYHOUR || ':' || OFFSET AS TIMESTAMP) AS start_timestamp, DATEADD(MINUTE, 60, DATE_ID || ' ' || BUSYHOUR || ':' || OFFSET) AS end_timestamp FROM "+tableName+" WHERE " + bhTypeFilter + dateCalc+") as "+EXTRASELECTNAME);

        } else {
            context.put("where_select", "");  
            context.put("where_delete", ""); 
            context.put("extra_select", ",( SELECT DISTINCT "+columnsToString(getKeyColumns(mTableId))+", BHTYPE, CAST(DATE_ID || ' ' || BUSYHOUR || ':' || OFFSET AS TIMESTAMP) AS start_timestamp, DATEADD(MINUTE, 60, DATE_ID || ' ' || BUSYHOUR || ':' || OFFSET) AS end_timestamp FROM "+tableName+") as "+EXTRASELECTNAME);
        }
        final long endTime = System.currentTimeMillis();
		final long timeTaken = (endTime-startTime);
        final StringBuffer msg = new StringBuffer();
        msg.append("AggregationGroupingAction.populateContext took(ms):"); msg.append(timeTaken);msg.append("\n");
        msg.append("mTableId="); msg.append(mTableId);msg.append("\n");
        msg.append("where_select="); msg.append(context.get("where_select"));msg.append("\n");
        msg.append("where_delete="); msg.append(context.get("where_delete"));msg.append("\n");
        msg.append("extra_select="); msg.append(context.get("extra_select"));msg.append("\n");
        if(log!=null) { //TODO: REMOVE extra logging
        	log.info(msg.toString());
        } else {
        	System.out.println(msg.toString());
        }
	}

	
    /**
     * Do the Group By Node on a table
     * @param counterNames The Coutners to group
     * @param tableName The table name the grouping is being performed on
     * @param partitionColList Table definition
     * @param storageId The storage ID of the BH target type
     * @param bhTypes the ID's of the placeholders being grouped
     * @throws Exception If there are any errors
     */
    private void groupByNode(final List<Measurementcounter> counterNames,
                             final String tableName, final List<Dwhcolumn> partitionColList, final String storageId,
                             final List<String> bhTypes, final String mTableId) throws Exception {
        final long start = System.currentTimeMillis();
        final long groupDateLong = getAggregationDate();
        final String storagePartition = getStoragePartition(storageId, groupDateLong);
        
        //EEIKBE: Need to use the AGG_DATE column when performing the DELETE but it may not be 
        //there in older TechPacks. This checks to see if the column exists: 
        //YES, use AGG_DATE.
        //NO,  use DATE_ID.
        String colName = "DATE_ID";
        //Need to determine which column to use when deleting the data from the CALC table.
        if(isColumnExists(partitionColList, "AGG_DATE", storageId)){
        	colName = "AGG_DATE";
        }
        
        final VelocityContext context = new VelocityContext();
        context.put("dateid", dateFormatter.format(groupDateLong));
        context.put("targetTable", storagePartition);
        
        context.put("keys_insert", columnsToString(joinColumns(getKeyColumns(mTableId),getElementColumns(groupKeyNodeList))));
        context.put("keys_select", columnsToString(joinColumns(getKeyColumns(mTableId),getElementColumns(groupKeyNodeList)), tableName));
        context.put("keys_group", columnsToString(joinColumns(getKeyColumns(mTableId),getElementColumns(groupKeyNodeList)), tableName));

        context.put("publicColumns_insert",  ", " + columnsToString(getPublicColumns(mTableId), tableName));
        context.put("publicColumns_select",  ", " + columnsToString(getPublicColumns(mTableId), tableName));
        context.put("publicColumns_group",  ", " + columnsToString(getPublicColumns(mTableId), tableName));
       
        context.put("counters_insert",  ", " + getMeasCounters(counterNames));
        context.put("counters_select",  ", " + generateCounterFunctionSql(counterNames, tableName));
                
        context.put("sourceTable", tableName);      
        context.put("groupby", GROUP_BY);
        
        populateContext(tableName, bhTypes, mTableId, groupDateLong, colName,
				context);

        
		final String totalSQL = generateGroupSql(context);
	    final int rowsUpdated = executeSQLUpdate(totalSQL);
        log.finer("Group By Node on table " + tableName + " SQL:");
        log.finer(totalSQL);
        final long stop = System.currentTimeMillis();
        final String msg = "Grouped By Node "+storageId+":"+bhTypes+" data to " + rowsUpdated + " new rows in " + storagePartition + " in " + (stop - start)+"mSec";
        log.info(msg);
    }

   
    /**
     * Do the Group By Time on a table
     * This works be getting all the distinct DATE_ID values from the tables, then for each DATE_ID build up an sql
     * statement that will group over time for each unique CELL e.g.
     * SELECT RNC, CELL, DATE_ID, HOUR_ID, SUM(C1) as C1, AVG(C2) as C2 from TEST_DAHBH where DATE_ID = '2009-10-13' GROUP BY RNC, CELL, DATE_ID, HOUR_ID
     * RNC        CELL       DATE_ID    HOUR_ID    C1         C2
     * 1          1          2009-10-13 16         2512       720
     * 1          2          2009-10-13 16         1375       517
     * 1          3          2009-10-13 16         1863       514
     * 2          4          2009-10-13 16         2712       623
     * 2          5          2009-10-13 16         2123       243
     * 2          6          2009-10-13 16         1600       204
     * @param counterNames The Coutners to group
     * @param tableName The table name the grouping is being performed on
     * @param partitionColList Table definition
     * @param storageId The storage ID of the BH target type
     * @param bhTypes the ID's of the placeholders being grouped
     * @throws Exception If there are any errors
     */
    private void groupByTime(final List<Measurementcounter> counterNames,
                             final String tableName, final List<Dwhcolumn> partitionColList, final String storageId,
                             final List<String> bhTypes, final String mTableId) throws Exception {
        final long start = System.currentTimeMillis();
        final long groupDateLong = getAggregationDate();
        final String storagePartition = getStoragePartition(storageId, groupDateLong);
        
        //EEIKBE: Need to use the AGG_DATE column when performing the DELETE but it may not be 
        //there in older TechPacks. This checks to see if the column exists: 
        //YES, use AGG_DATE.
        //NO,  use DATE_ID.
        String colName = "DATE_ID";
        //Need to determine which column to use when deleting the data from the CALC table.
        if(isColumnExists(partitionColList, "AGG_DATE", storageId)){
        	colName = "AGG_DATE";
        }

        final VelocityContext context = new VelocityContext();
        context.put("dateid", dateFormatter.format(groupDateLong));
        context.put("targetTable", storagePartition);
        
        context.put("keys_insert", columnsToString(getKeyColumns(mTableId)));
        context.put("keys_select", columnsToString(getKeyColumns(mTableId), tableName));
        context.put("keys_group", columnsToString(getKeyColumns(mTableId), tableName));
        
        context.put("publicColumns_insert",  ", " + columnsToString(getPublicColumns(mTableId)));
        context.put("publicColumns_select",  ", " + columnsToString(replaceColumns(getPublicColumns(mTableId), groupPublicTimeList), tableName));
        context.put("publicColumns_group",  ", " + columnsToString(filterColumns(getPublicColumns(mTableId), groupPublicTimeList), tableName));
        
        context.put("counters_insert",  ", " + getMeasCounters(counterNames));
        context.put("counters_select",  ", " + generateCounterFunctionSql(counterNames, tableName));
                
        context.put("sourceTable", tableName);
        context.put("groupby", GROUP_BY);
         
        populateContext(tableName, bhTypes, mTableId, groupDateLong, colName,
				context);

        
		final String totalSQL = generateGroupSql(context);
        final int rowsUpdated = executeSQLUpdate(totalSQL);
        log.finer("Group By Time on table " + tableName + " SQL:");
        log.finer(totalSQL);
        final long stop = System.currentTimeMillis();
        final String msg = "Grouped By Time "+storageId+":"+bhTypes+" data to " + rowsUpdated + " new rows in " + storagePartition + " in " + (stop - start)+"mSec";
        log.info(msg);
    }

    

	private void groupByNodeAndTime(
			final List<Measurementcounter> counterNames,
			final String tableName, final List<Dwhcolumn> partitionColList,
			final String storageId, final List<String> bhTypes, final String mTableId)
			throws Exception {
		
		final long start = System.currentTimeMillis();
		final long groupDateLong = getAggregationDate();
		final String storagePartition = getStoragePartition(storageId,
				groupDateLong);
		
        //EEIKBE: Need to use the AGG_DATE column when performing the DELETE but it may not be 
        //there in older TechPacks. This checks to see if the column exists: 
        //YES, use AGG_DATE.
        //NO,  use DATE_ID.
        String colName = "DATE_ID";
        //Need to determine which column to use when deleting the data from the CALC table.
        if(isColumnExists(partitionColList, "AGG_DATE", storageId)){
        	colName = "AGG_DATE";
        }
		
        final VelocityContext context = new VelocityContext();
        context.put("dateid", dateFormatter.format(groupDateLong));
        context.put("targetTable", storagePartition);

        context.put("keys_insert", columnsToString(joinColumns(getKeyColumns(mTableId),getElementColumns(groupKeyTimeNodeList))));
        context.put("keys_select", columnsToString(joinColumns(getKeyColumns(mTableId),getElementColumns(groupKeyTimeNodeList)), tableName));
        context.put("keys_group", columnsToString(joinColumns(getKeyColumns(mTableId),getElementColumns(groupKeyTimeNodeList)), tableName));
        
        context.put("publicColumns_insert",  ", " + columnsToString(getPublicColumns(mTableId)));
        context.put("publicColumns_select",  ", " + columnsToString(replaceColumns(getPublicColumns(mTableId), groupPublicTimeList), tableName));
        context.put("publicColumns_group",  ", " + columnsToString(filterColumns(getPublicColumns(mTableId), groupPublicTimeList), tableName));
        
        context.put("counters_insert",  ", " + getMeasCounters(counterNames));
        context.put("counters_select",  ", " + generateCounterFunctionSql(counterNames, tableName));
                
        context.put("sourceTable", tableName);
        context.put("groupby", GROUP_BY);
        
        populateContext(tableName, bhTypes, mTableId, groupDateLong, colName,
				context);

		
		final String totalSQL = generateGroupSql(context);
		final int rowsUpdated = executeSQLUpdate(totalSQL);
		log.finer("Group By Time + Node on table " + tableName + " SQL:");
		log.finer(totalSQL);
		final long stop = System.currentTimeMillis();
		final String msg = "Grouped By Time + Node " + storageId + ":" + bhTypes
				+ " data to " + rowsUpdated + " new rows in "
				+ storagePartition + " in " + (stop - start) + "mSec";
		log.info(msg);
	}
    
    /**
     * Executes a SQL query
     */
    @Override
    protected int executeSQLUpdate(final String sqlClause) throws SQLException {
        return super.executeSQLUpdate(sqlClause);
    }

    protected String generateGroupSql(final VelocityContext context) throws Exception {
        		
        final StringWriter writer = new StringWriter();       
        VelocityEngine ve = null;     
        try {
        	
            ve = VelocityPool.reserveEngine();          
        	ve.evaluate(context, writer, "", groupTemplate);
        
        } finally {
          VelocityPool.releaseEngine(ve);
        }

        return writer.toString();
    }
       
    private String generateTypeFilter(final List<String> types){
    	return generateTypeFilter(types, null);
    }
    
    private String generateTypeFilter(final List<String> types, String table){
        if(types == null || types.isEmpty()){
            return null;
        }
        
		if (table != null && table.length()>0){
			table += ".";
		} else {
			table = "";
		}
        
        final StringBuilder bhTypeFilter = new StringBuilder("(");
        final Iterator<String> iter = types.iterator();
        while(iter.hasNext()){
            final String hType = iter.next();
            bhTypeFilter.append(table+"BHTYPE = '").append(hType).append("'");
            if(iter.hasNext()){
                bhTypeFilter.append(" or ");
            }
        }
        bhTypeFilter.append(")");
        return bhTypeFilter.toString();
    }

    /**
     * Generate the sql section to calculate the pm counters grouping formulas.
     * TR:HL92898
     * This method should use the timeAggregation column as the formula and not 
     * the groupAggregation, as this is only used in Reports. 
     * @param countersToGroup The mp counters being grouped
     * @return The sql that performs the counter grouping function
     * @throws Exception If there are any errors
     */
    private String generateCounterFunctionSql(final List<Measurementcounter> countersToGroup, String table) throws Exception {
        final StringBuilder buffer = new StringBuilder();
        
		if (table != null && table.length()>0){
			table += ".";
		} else {
			table = "";
		}
        
        final Iterator<Measurementcounter> cInterator = countersToGroup.iterator();
        while(cInterator.hasNext()){
            final Measurementcounter counter = cInterator.next();
            final String timeFunction = counter.getTimeaggregation();
            final String cName = counter.getDataname();
            if(timeFunction == null){
                throw new EngineMetaDataException("No Time Aggregation Function defined for counter '"+cName+"'",
                        null, "generateCounterFunctionSql");
            }
            buffer.append(timeFunction).append("(").append(table+cName).append(") as ").append(cName);
            if(cInterator.hasNext()){
                buffer.append(", ");
            }
        }
        return buffer.toString();
    }

    /**
     * Get a connection to dwhrep
     * @return RockFactory connected to dwhrep
     * @throws Exception If there are any errors
     */
    protected RockFactory getDwhrepRock() throws Exception {
        final RockFactory etlrep = getRockFact();
        final Meta_databases metaDbs = new Meta_databases(etlrep);
        metaDbs.setType_name("USER");
        metaDbs.setConnection_name("dwhrep");
        final Meta_databasesFactory fac = new Meta_databasesFactory(etlrep, metaDbs);
        final List links = fac.get();
        if(links.isEmpty()){
             throw new EngineMetaDataException(
                     "Unable to connect metadata database (No dwhrep or multiple dwhreps defined in Meta_databases..)",
                     null,
                     "getDwhrepRock");
        }
        final Meta_databases dwh = (Meta_databases)links.get(0);
        return new RockFactory(
                dwh.getConnection_string(),
                dwh.getUsername(),
                dwh.getPassword(),
                dwh.getDriver_name(),
                getClass().getName(), true);
    }

    /**
     * Supported group types
     */
    public enum GroupType {
        Time, Node, NodeTime, None;
        public static GroupType getType(final String type){
            if(type.equalsIgnoreCase("node")){
                return Node;
            } else if(type.equalsIgnoreCase("time")){
                return Time;
            } else if(type.equalsIgnoreCase("Time + Node")){
                return NodeTime;
            }
            return None;
        }
    }
    
    
    protected List<String> getKeyColumns(final String mTableId) throws Exception {
    	
        final Measurementcolumn where = new Measurementcolumn(dwhrepRock);
        where.setMtableid(mTableId);
        where.setColtype("KEY");
        final MeasurementcolumnFactory fac = new MeasurementcolumnFactory(dwhrepRock, where, " ORDER BY COLNUMBER");
               
    	final List<String> result = new ArrayList<String>();	
		for (Measurementcolumn mcol : fac.get()) {
			
			final String col = mcol.getDataname();
			result.add(col);
		}
        
        return result;
    }
    
    protected List<String> getPublicColumns(final String mTableId) throws Exception {
    	
        final Measurementcolumn where = new Measurementcolumn(dwhrepRock);
        where.setMtableid(mTableId);
        where.setColtype("PUBLICKEY");
        final MeasurementcolumnFactory fac = new MeasurementcolumnFactory(dwhrepRock, where, " ORDER BY COLNUMBER");
               
    	final List<String> result = new ArrayList<String>();	
		for (Measurementcolumn mcol : fac.get()) {
			
			final String col = mcol.getDataname();
			result.add(col);
		}
        
        return result;
    }
         
    
    protected List<String> filterColumns(final List<String> columns, final List<String> ignoredColumnList) throws Exception {
    	    	
    	final List<String> result = new ArrayList<String>();
    	
    	final Map<String,String> ignoredColumns = new HashMap<String,String>();
    	for (String r : ignoredColumnList) {
    		final String[] tmp = r.split("=");
    		ignoredColumns.put(tmp[0], tmp[1]);
    	}
    	
		for (String mcol : columns) {			
			// skip the unwanted columns
			if (ignoredColumns == null || !ignoredColumns.keySet().contains(mcol)){
	        	result.add(mcol);
			}
		}		
        return result;
    }
 
    protected List<String> replaceColumns(final List<String> columns, final List<String> replaceColumnList) throws Exception {
    	
    	final List<String> result = new ArrayList<String>(); 
  
    	
    	final Map<String,String> replaceColumns = new HashMap<String,String>();
    	for (String r : replaceColumnList) {
    		final String[] tmp = r.split("=");
    		replaceColumns.put(tmp[0], tmp[1]);
    	}
    	
    	
		for (String mcol : columns) {			
			// skip the unwanted columns
			if (replaceColumns != null && replaceColumns.keySet().contains(mcol)){
				final String str = replaceColumns.get(mcol);
				if (str != null && str.length()>0){
					result.add(str);
				}
			} else {
				result.add(mcol);
			}
		}		
        return result;
    }
    
	protected String columnsToString(final List<String> columns) throws Exception {
		return columnsToString(columns, null);
	}

    
	protected String columnsToString(final List<String> columns, String table) throws Exception {
		
		final StringBuffer result = new StringBuffer();

		if (table != null && table.length()>0){
			table += ".";
		} else {
			table = "";
		}
		
		for (String mcol : columns) {
			
			String col = mcol;
			
			col = col.replace("$CALCTABLE.", table);
			
			if (col.indexOf("(")>=0){
				// contains sql command, no table added
			} else {
				col = table+col;
			}
			
			if (result.length() == 0) {
				result.append(col);
			} else {
				result.append(", ").append(col);
			}
		}
		
		return result.toString();
	}    
    
	protected String columnsToStringWithAnd(final List<String> columns) throws Exception {
		
		final StringBuffer result = new StringBuffer();
		
		for (String mcol : columns) {
			
			final String col = mcol;
			
			if (result.length() == 0) {
				result.append(col);
			} else {
				result.append(" AND ").append(col);
			}
		}
		
		return result.toString();
	}
  
	protected List<String> groupColumns(final List<String> columns, final Map<String,String> groupedColumns) throws Exception {

		final List<String> result = new ArrayList<String>();	
		for (String mcol : columns) {
			
			String col = mcol;
			
			if (groupedColumns != null && groupedColumns.keySet().contains(mcol)){
				col = groupedColumns.get(mcol) + "(" +mcol+ ") as " + mcol;
			}

			result.add(col);

		}		
		return result;
	}
    
	
	protected List<String> trimColumns(final List<String> columns, final Map<String,String> trimedColumns) throws Exception {

		final List<String> result = new ArrayList<String>();	
		for (String mcol : columns) {
			
			final String col = mcol;
			
			if (trimedColumns == null || trimedColumns.size() == 0 || !trimedColumns.keySet().contains(mcol)){
				result.add(col);
			}
		}		
		return result;
	}
		
	protected List<String> timeCalculations(final List<String> columns, String table, String tt) throws Exception {
		
		if (table != null && table.length()>0){
			table += ".";
		} else {
			table = "";
		}
		
		if (tt != null && tt.length()>0){
			tt += ".";
		} else {
			tt = "";
		}
		
		final List<String> result = new ArrayList<String>();	
		for (String mcol : columns) {		
			result.add(table+mcol+"="+tt+mcol);
		}		
		return result;
	}
	
	protected List<String> joinColumns(final List<String> columns, final List<String> columns2) throws Exception {

		final List<String> result = new ArrayList<String>();	
		for (String mcol : columns) {		
			final String col = mcol;		
			if (columns2 != null && columns2.contains(mcol)){
				result.add(col);
			}
		}		
		return result;
	}
	
	protected List<String> toListArray(final String str){
		final List<String> tmp = new ArrayList<String>();
		final String[] array = str.split(";");
		for(int i = 0 ; i < array.length ; i++){
			tmp.add(array[i]);
		}		
		return tmp;
	}
		
	protected String getMeasCounters(final List<Measurementcounter> counterNames){
		return getMeasCounters(counterNames,null);
	}

	
	protected String getMeasCounters(final List<Measurementcounter> counterNames, String table){
    	
		final StringBuffer result = new StringBuffer();
		
		if (table != null && table.length()>0){
			table += ".";
		} else {
			table = "";
		}
		
        for(Measurementcounter c : counterNames){
        	if (result.length()==0){
        		result.append(table+c.getDataname());
        	} else {
        		result.append(", ").append(table+c.getDataname());
        	}
        }
        
        return result.toString();
	}

	public boolean isColumnExists(final List<Dwhcolumn> columnList, final String columnName, final String storageId) {
		boolean result = false;
		final Iterator<Dwhcolumn> iterator = columnList.iterator();
		while(iterator.hasNext()){
			if(iterator.next().getDataname().equals(columnName)){
				result = true;
				break;
			}
		}
		return result;
	}

	protected Map<String, String> getKeyNameAndDataType(final String mTableId) throws SQLException, RockException {
		final Map<String, String> result = new HashMap<String, String>();

        final Measurementcolumn where = new Measurementcolumn(dwhrepRock);
        where.setMtableid(mTableId);
        where.setColtype("KEY");
        final MeasurementcolumnFactory fac = new MeasurementcolumnFactory(dwhrepRock, where, " ORDER BY COLNUMBER");
//               System.out.println("SELECT * FROM Measurementcolumn WHERE Coltype='KEY' AND Mtableid='"+mTableId+"'"); //TODO: REMOVE
		for (Measurementcolumn mcol : fac.get()) {
			
			final String colName = mcol.getDataname();
			final String colDataType = mcol.getDatatype();
			result.put(colName, colDataType);
//			System.out.println("colName:"+colName+"="+colDataType);
		}
		return result;
	}
/**
 * This Method Adds each key value to the where clause. I also safeguards against 
 * the possibility of the value of the Cell being null. If the cell is null and this 
 * method is not used then the result SQL cannot compare null against null and will 
 * return an empty result set. If this method is used a check is performed to 
 * determine if the value is null, if it is then it's set to a default value and 
 * the comparison is overlooked.
 * ISNULL(tableName.colName, defaultValue_for_dataType) = ISNULL(tt.colName, defaultValue_for_dataType)
 * 
 * @param columns
 * @param table
 * @param tt
 * @return
 */
	protected List<String> timeCalculationsWithNullCheck(
			final Map<String, String> columns, String table, String tt) {
		if (table != null && table.length()>0){
			table += ".";
		} else {
			table = "";
		}
		
		if (tt != null && tt.length()>0){
			tt += ".";
		} else {
			tt = "";
		}
		
		final List<String> result = new ArrayList<String>();	
		String dataType = "";
		String defaultValue = ""; 
		for (String key : columns.keySet()) {	
			dataType = columns.get(key);
			
			if(dataType.equalsIgnoreCase("varchar")){
				defaultValue = "''";
			}else if(dataType.equalsIgnoreCase("numberic") || dataType.equalsIgnoreCase("int")){
				defaultValue = "0";
			}else if(dataType.equalsIgnoreCase("date")){
				defaultValue="0001-01-01 00:00:00";
			}else{
				defaultValue = "0";
			}
			
			result.add("ISNULL("+table+key+ ","+defaultValue+")=ISNULL("+tt+key+","+defaultValue+")");
		}		
		return result;
	}
	
}
