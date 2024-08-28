insert into Meta_transfer_actions (VERSION_NUMBER, TRANSFER_ACTION_ID, COLLECTION_ID, COLLECTION_SET_ID, ACTION_TYPE, TRANSFER_ACTION_NAME, ORDER_BY_NO, WHERE_CLAUSE_01, ACTION_CONTENTS_01, ENABLED_FLAG, CONNECTION_ID, WHERE_CLAUSE_02, WHERE_CLAUSE_03, ACTION_CONTENTS_02, ACTION_CONTENTS_03) values ('((6))', '10252', '2071', '61', 'GateKeeper', 'GateKeeper', '0', '#
#Fri Jul 31 10:22:13 EEST 2009
aggregation=DC_E_MGW_ATMPORT_DAYBH_Atmport
', 'SELECT count(*) result FROM LOG_AGGREGATIONSTATUS WHERE TYPENAME = ''DC_E_MGW_ATMPORT'' AND TIMELEVEL = ''DAYBH'' AND DATADATE = $date AND AGGREGATIONSCOPE = ''DAY'' AND STATUS NOT IN (''AGGREGATED'')
UNION ALL
select count(*) from (SELECT count(distinct aggregation) c  FROM LOG_AGGREGATIONSTATUS   WHERE typename = ''DC_E_MGW_ATMPORTBH'' AND timelevel = ''RANKBH'' AND DATADATE = $date and AGGREGATIONSCOPE = ''DAY'' AND STATUS IN (''AGGREGATED'') ) as a ,(select  count(distinct aggregation) c  from LOG_AggregationRules where target_type = ''DC_E_MGW_ATMPORTBH'' AND target_level = ''RANKBH''  and aggregationscope = ''DAY'') as b where a.c>=b.c and b.c <> 0 
UNION ALL
select count(*) from (SELECT count(distinct aggregation) c  FROM LOG_AGGREGATIONSTATUS   WHERE typename = ''DC_E_MGW_ATMPORT'' AND timelevel = ''COUNT'' AND DATADATE = $date and AGGREGATIONSCOPE = ''DAY'' AND STATUS IN (''AGGREGATED'') ) as a ,(select  count(distinct aggregation) c  from LOG_AggregationRules where target_type = ''DC_E_MGW_ATMPORT'' AND target_level = ''COUNT''  and aggregationscope = ''DAY'') as b where a.c>=b.c and b.c <> 0 ', 'Y', '2', '', '', '', '');
insert into Meta_transfer_actions (VERSION_NUMBER, TRANSFER_ACTION_ID, COLLECTION_ID, COLLECTION_SET_ID, ACTION_TYPE, TRANSFER_ACTION_NAME, ORDER_BY_NO, DESCRIPTION, WHERE_CLAUSE_01, ACTION_CONTENTS_01, ENABLED_FLAG, CONNECTION_ID, WHERE_CLAUSE_02, WHERE_CLAUSE_03, ACTION_CONTENTS_02, ACTION_CONTENTS_03) values ('((6))', '10253', '2071', '61', 'Aggregation', 'Aggregator_DC_E_MGW_ATMPORT_DAYBH_Atmport', '1', '', '#
#Wed Nov 04 10:54:24 GMT 2009
tablename=DC_E_MGW_ATMPORT_DAYBH
', 'DELETE $targetDerivedTable.get("RANKSRC")
WHERE DATE_ID=$dateid
AND BHTYPE in (select distinct BHTYPE from $sourceDerivedTable.get("RANKSRC"))


Insert into $targetDerivedTable.get("RANKSRC")
(
 AtmPort
,MGW
,MOID
,NEDN
,NESW
,NEUN
,OSS_ID
,SN
,TransportNetwork
,userLabel
,DATE_ID
,YEAR_ID
,MONTH_ID
,DAY_ID
,MIN_ID
,BHTYPE
,BUSYHOUR
,BHCLASS
,TIMELEVEL
,SESSION_ID
,BATCH_ID
,PERIOD_DURATION
,ROWSTATUS
,DC_RELEASE
,DC_SOURCE
,DC_TIMEZONE
,pmReceivedAtmCells
,pmSecondsWithUnexp
,pmTransmittedAtmCells
)
select
 raw.AtmPort
,raw.MGW
,raw.MOID
,raw.NEDN
,raw.NESW
,raw.NEUN
,raw.OSS_ID
,raw.SN
,raw.TransportNetwork
,raw.userLabel
,raw.DATE_ID
,raw.YEAR_ID
,raw.MONTH_ID
,raw.DAY_ID
,raw.MIN_ID
,rankbh.BHTYPE
,rankbh.BUSYHOUR
,rankbh.BHCLASS
,''DAYBH''
,$sessionid
,$batchid
,raw.PERIOD_DURATION
,''AGGREGATED''
,raw.DC_RELEASE
,raw.DC_SOURCE
,raw.DC_TIMEZONE
,raw.pmReceivedAtmCells
,raw.pmSecondsWithUnexp
,raw.pmTransmittedAtmCells
from $sourceDerivedTable.get("RANKSRC") as rankbh, $sourceDerivedTable.get("BHSRC") as raw
where raw.hour_id = rankbh.BUSYHOUR
and raw.ROWSTATUS NOT IN (''DUPLICATE'',''SUSPECTED'')

and rankbh.AtmPort = raw.AtmPort
and rankbh.MGW = raw.MGW
and rankbh.OSS_ID = raw.OSS_ID
and rankbh.TransportNetwork = raw.TransportNetwork

and rankbh.date_id = raw.date_id
and rankbh.date_id = $dateid', 'Y', '2', '', '', '', '');
