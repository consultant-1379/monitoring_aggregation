insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ATMPORTBH', 'PP0', 'cast((sum(ifnull(pmReceivedAtmCells,0,pmReceivedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'Atmport Rx maximum hour.', 'DC_E_MGW:((7))', 'ATMPORT', '0', '1', 'RANKBH_TIMELIMITED', 'PP', 'time', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ATMPORTBH', 'PP1', 'cast((sum(ifnull(pmReceivedAtmCells,0,pmReceivedAtmCells)) + sum(ifnull(pmTransmittedAtmCells,0,pmTransmittedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'Atmport RxTx maximum hour.', 'DC_E_MGW:((7))', 'ATMPORT', '0', '1', 'RANKBH_TIMELIMITED', 'PP', 'time', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ATMPORTBH', 'PP2', 'cast((sum(ifnull(pmTransmittedAtmCells,0,pmTransmittedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'Atmport Tx maximum hour.', 'DC_E_MGW:((7))', 'ATMPORT', '0', '1', 'RANKBH_TIMELIMITED', 'PP', 'time', '0');

insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, AGGREGATIONTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((6))', 'DC_E_MGW_ATMPORTBH', 'RxTxAtmP', 'cast((sum(ifnull(pmReceivedAtmCells,0,pmReceivedAtmCells)) + sum(ifnull(pmTransmittedAtmCells,0,pmTransmittedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'Atmport RxTx maximum hour.', 'DC_E_MGW:((6))', 'AtmPort', '0', 'RANKBH_TIMELIMITED', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, AGGREGATIONTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((6))', 'DC_E_MGW_ATMPORTBH', 'TxAtmP', 'cast((sum(ifnull(pmTransmittedAtmCells,0,pmTransmittedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'Atmport Tx maximum hour.', 'DC_E_MGW:((6))', 'AtmPort', '0', 'RANKBH_TIMELIMITED', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, AGGREGATIONTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((6))', 'DC_E_MGW_ATMPORTBH', 'RxAtmP', 'cast((sum(ifnull(pmReceivedAtmCells,0,pmReceivedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'Atmport Rx maximum hour.', 'DC_E_MGW:((6))', 'AtmPort', '0', 'RANKBH_TIMELIMITED', 'None', '0');

insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, AGGREGATIONTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((6))', 'DC_E_MGW_AAL2APBH', 'Aal2Ap', 'cast((sum(ifnull(pmExisOrigConns,0,pmExisOrigConns)) + sum(ifnull(pmExisTermConns,0,pmExisTermConns)) + sum(ifnull(pmExisTransConns,0,pmExisTransConns))) as numeric(18,8))', 'ROWSTATUS NOT IN (''DUPLICATE'',''SUSPECTED'') ', 'Aal2Ap maximum hour.', 'DC_E_MGW:((6))', 'Aal2Ap', '0', 'RANKBH_TIMELIMITED', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, AGGREGATIONTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((6))', 'DC_E_MGW_JHSERVICEBH', 'JHServ', 'cast((avg(ifnull(pmBusyInstances,0,pmBusyInstances))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'JitterHandling Service maximum hour.', 'DC_E_MGW:((6))', 'JHService', '0', 'RANKBH_TIMELIMITED', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, AGGREGATIONTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((6))', 'DC_E_MGW_SERVICEBH', 'Service', 'cast((avg(ifnull(pmBusyInstances,0,pmBusyInstances))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'ServiceService maximum hour.', 'DC_E_MGW:((6))', 'Service', '0', 'RANKBH_TIMELIMITED', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, AGGREGATIONTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((6))', 'DC_E_MGW_VCLTPBH', 'RxTxVclTp', 'cast((sum(ifnull(pmReceivedAtmCells,0,pmReceivedAtmCells)) + sum(ifnull(pmTransmittedAtmCells,0,pmTransmittedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'VclTp RxTx maximum hour.', 'DC_E_MGW:((6))', 'VclTp', '0', 'RANKBH_TIMELIMITED', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, AGGREGATIONTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((6))', 'DC_E_MGW_VCLTPBH', 'RxVclTp', 'cast((sum(ifnull(pmReceivedAtmCells,0,pmReceivedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'VclTp Rx maximum hour.', 'DC_E_MGW:((6))', 'VclTp', '0', 'RANKBH_TIMELIMITED', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, AGGREGATIONTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((6))', 'DC_E_MGW_VCLTPBH', 'TxVclTp', 'cast((sum(ifnull(pmTransmittedAtmCells,0,pmTransmittedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'VclTp Tx maximum hour.', 'DC_E_MGW:((6))', 'VclTp', '0', 'RANKBH_TIMELIMITED', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, AGGREGATIONTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((6))', 'DC_E_MGW_VPLTPBH', 'RxTxVplTp', 'cast((sum(ifnull(pmReceivedAtmCells,0,pmReceivedAtmCells)) + sum(ifnull(pmTransmittedAtmCells,0,pmTransmittedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'VplTp RxTx maximum hour.', 'DC_E_MGW:((6))', 'VplTp', '0', 'RANKBH_TIMELIMITED', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, AGGREGATIONTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((6))', 'DC_E_MGW_VPLTPBH', 'RxVplTp', 'cast((sum(ifnull(pmReceivedAtmCells,0,pmReceivedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'VplTp Rx maximum hour.', 'DC_E_MGW:((6))', 'VplTp', '0', 'RANKBH_TIMELIMITED', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, AGGREGATIONTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((6))', 'DC_E_MGW_VPLTPBH', 'TxVplTp', 'cast((sum(ifnull(pmTransmittedAtmCells,0,pmTransmittedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'VplTp Tx maximum hour.', 'DC_E_MGW:((6))', 'VplTp', '0', 'RANKBH_TIMELIMITED', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_AAL2APBH', 'PP0', 'cast((sum(ifnull(pmExisOrigConns,0,pmExisOrigConns)) + sum(ifnull(pmExisTermConns,0,pmExisTermConns)) + sum(ifnull(pmExisTransConns,0,pmExisTransConns))) as numeric(18,8))', 'ROWSTATUS NOT IN (''DUPLICATE'',''SUSPECTED'') ', 'Aal2Ap maximum hour.', 'DC_E_MGW:((7))', 'AAL2AP', '0', '1', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_JHSERVICEBH', 'PP0', 'cast((avg(ifnull(pmBusyInstances,0,pmBusyInstances))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'JitterHandling Service maximum hour.', 'DC_E_MGW:((7))', 'JHSERVICE', '0', '1', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_SERVICEBH', 'PP0', 'cast((avg(ifnull(pmBusyInstances,0,pmBusyInstances))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'ServiceService maximum hour.', 'DC_E_MGW:((7))', 'SERVICE', '0', '1', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VCLTPBH', 'PP0', 'cast((sum(ifnull(pmReceivedAtmCells,0,pmReceivedAtmCells)) + sum(ifnull(pmTransmittedAtmCells,0,pmTransmittedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'VclTp RxTx maximum hour.', 'DC_E_MGW:((7))', 'VCLTP', '0', '1', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VCLTPBH', 'PP1', 'cast((sum(ifnull(pmReceivedAtmCells,0,pmReceivedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'VclTp Rx maximum hour.', 'DC_E_MGW:((7))', 'VCLTP', '0', '1', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VCLTPBH', 'PP2', 'cast((sum(ifnull(pmTransmittedAtmCells,0,pmTransmittedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'VclTp Tx maximum hour.', 'DC_E_MGW:((7))', 'VCLTP', '0', '1', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VPLTPBH', 'PP0', 'cast((sum(ifnull(pmReceivedAtmCells,0,pmReceivedAtmCells)) + sum(ifnull(pmTransmittedAtmCells,0,pmTransmittedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'VplTp RxTx maximum hour.', 'DC_E_MGW:((7))', 'VPLTP', '0', '1', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VPLTPBH', 'PP1', 'cast((sum(ifnull(pmReceivedAtmCells,0,pmReceivedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'VplTp Rx maximum hour.', 'DC_E_MGW:((7))', 'VPLTP', '0', '1', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, BHCRITERIA, WHERECLAUSE, DESCRIPTION, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VPLTPBH', 'PP2', 'cast((sum(ifnull(pmTransmittedAtmCells,0,pmTransmittedAtmCells))) as numeric(18,8))', 'ROWSTATUS NOT IN (''SUSPECTED'') ', 'VplTp Tx maximum hour.', 'DC_E_MGW:((7))', 'VPLTP', '0', '1', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_AAL2APBH', 'PP1', 'DC_E_MGW:((7))', 'AAL2AP', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_AAL2APBH', 'PP2', 'DC_E_MGW:((7))', 'AAL2AP', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_AAL2APBH', 'PP3', 'DC_E_MGW:((7))', 'AAL2AP', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_AAL2APBH', 'PP4', 'DC_E_MGW:((7))', 'AAL2AP', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_AAL2APBH', 'CP0', 'DC_E_MGW:((7))', 'AAL2AP', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_AAL2APBH', 'CP1', 'DC_E_MGW:((7))', 'AAL2AP', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_AAL2APBH', 'CP2', 'DC_E_MGW:((7))', 'AAL2AP', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_AAL2APBH', 'CP3', 'DC_E_MGW:((7))', 'AAL2AP', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_AAL2APBH', 'CP4', 'DC_E_MGW:((7))', 'AAL2AP', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ELEMBH', 'PP0', 'DC_E_MGW:((7))', 'ELEM', '1', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ELEMBH', 'PP1', 'DC_E_MGW:((7))', 'ELEM', '1', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ELEMBH', 'PP2', 'DC_E_MGW:((7))', 'ELEM', '1', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ELEMBH', 'PP3', 'DC_E_MGW:((7))', 'ELEM', '1', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ELEMBH', 'PP4', 'DC_E_MGW:((7))', 'ELEM', '1', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ELEMBH', 'CP0', 'DC_E_MGW:((7))', 'ELEM', '1', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ELEMBH', 'CP1', 'DC_E_MGW:((7))', 'ELEM', '1', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ELEMBH', 'CP2', 'DC_E_MGW:((7))', 'ELEM', '1', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ELEMBH', 'CP3', 'DC_E_MGW:((7))', 'ELEM', '1', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ELEMBH', 'CP4', 'DC_E_MGW:((7))', 'ELEM', '1', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_JHSERVICEBH', 'PP1', 'DC_E_MGW:((7))', 'JHSERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_JHSERVICEBH', 'PP2', 'DC_E_MGW:((7))', 'JHSERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_JHSERVICEBH', 'PP3', 'DC_E_MGW:((7))', 'JHSERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_JHSERVICEBH', 'PP4', 'DC_E_MGW:((7))', 'JHSERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_JHSERVICEBH', 'CP0', 'DC_E_MGW:((7))', 'JHSERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_JHSERVICEBH', 'CP1', 'DC_E_MGW:((7))', 'JHSERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_JHSERVICEBH', 'CP2', 'DC_E_MGW:((7))', 'JHSERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_JHSERVICEBH', 'CP3', 'DC_E_MGW:((7))', 'JHSERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_JHSERVICEBH', 'CP4', 'DC_E_MGW:((7))', 'JHSERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_SERVICEBH', 'PP1', 'DC_E_MGW:((7))', 'SERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_SERVICEBH', 'PP2', 'DC_E_MGW:((7))', 'SERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_SERVICEBH', 'PP3', 'DC_E_MGW:((7))', 'SERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_SERVICEBH', 'PP4', 'DC_E_MGW:((7))', 'SERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_SERVICEBH', 'CP0', 'DC_E_MGW:((7))', 'SERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_SERVICEBH', 'CP1', 'DC_E_MGW:((7))', 'SERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_SERVICEBH', 'CP2', 'DC_E_MGW:((7))', 'SERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_SERVICEBH', 'CP3', 'DC_E_MGW:((7))', 'SERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_SERVICEBH', 'CP4', 'DC_E_MGW:((7))', 'SERVICE', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ATMPORTBH', 'PP3', 'DC_E_MGW:((7))', 'ATMPORT', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ATMPORTBH', 'PP4', 'DC_E_MGW:((7))', 'ATMPORT', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ATMPORTBH', 'CP0', 'DC_E_MGW:((7))', 'ATMPORT', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ATMPORTBH', 'CP1', 'DC_E_MGW:((7))', 'ATMPORT', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ATMPORTBH', 'CP2', 'DC_E_MGW:((7))', 'ATMPORT', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ATMPORTBH', 'CP3', 'DC_E_MGW:((7))', 'ATMPORT', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_ATMPORTBH', 'CP4', 'DC_E_MGW:((7))', 'ATMPORT', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VCLTPBH', 'PP3', 'DC_E_MGW:((7))', 'VCLTP', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VCLTPBH', 'PP4', 'DC_E_MGW:((7))', 'VCLTP', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VCLTPBH', 'CP0', 'DC_E_MGW:((7))', 'VCLTP', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VCLTPBH', 'CP1', 'DC_E_MGW:((7))', 'VCLTP', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VCLTPBH', 'CP2', 'DC_E_MGW:((7))', 'VCLTP', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VCLTPBH', 'CP3', 'DC_E_MGW:((7))', 'VCLTP', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VCLTPBH', 'CP4', 'DC_E_MGW:((7))', 'VCLTP', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VPLTPBH', 'PP3', 'DC_E_MGW:((7))', 'VPLTP', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VPLTPBH', 'PP4', 'DC_E_MGW:((7))', 'VPLTP', '0', '0', 'RANKBH_TIMELIMITED', 'PP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VPLTPBH', 'CP0', 'DC_E_MGW:((7))', 'VPLTP', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VPLTPBH', 'CP1', 'DC_E_MGW:((7))', 'VPLTP', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VPLTPBH', 'CP2', 'DC_E_MGW:((7))', 'VPLTP', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VPLTPBH', 'CP3', 'DC_E_MGW:((7))', 'VPLTP', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
insert into Busyhour (VERSIONID, BHLEVEL, BHTYPE, TARGETVERSIONID, BHOBJECT, BHELEMENT, ENABLE, AGGREGATIONTYPE, PLACEHOLDERTYPE, GROUPING, REACTIVATEVIEWS) values ('DC_E_MGW:((7))', 'DC_E_MGW_VPLTPBH', 'CP4', 'DC_E_MGW:((7))', 'VPLTP', '0', '0', 'RANKBH_TIMELIMITED', 'CP', 'None', '0');
