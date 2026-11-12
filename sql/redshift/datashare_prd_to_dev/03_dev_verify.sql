/*
=======================================================================
AMAZON REDSHIFT - PRD DATA SHARING NAAR DEV / UAT
03 - DEV: CONTROLE VAN RECHTEN EN TEST VAN DE DATASHARE
=======================================================================

Uitvoeren op:   Redshift DEV (consumer)
Host:           rsc-ter-dev.c7s4bhsdkddw.eu-west-1.redshift.amazonaws.com
Database:       terdwh_uat
Uitgevoerd:     2026-10-15 door nicolas_vilain via DBeaver

Read-only: dit script mag opnieuw uitgevoerd worden.

Volgorde:
    01_prd_create_datashare.sql   (PRD)
    02_dev_consume_datashare.sql  (DEV)
    03_dev_verify.sql             (DEV)          <- dit script

Resultaat op 2026-10-15:
    count(*) terdwh_prd_datashare.ingest.bc_api_general_ledger_entries
        = 36.312.414  (tegenover 21.973.007 in DEV terdwh)

=======================================================================
*/


/*
=======================================================================
BLOK 15 - REDSHIFT DEV: CONTROLE VAN RECHTEN
=======================================================================
Waarom:
Controleren of de rollen en privileges correct zijn ingesteld.
*/

SELECT *
FROM svv_role_grants
ORDER BY role_name;

SELECT *
FROM svv_database_privileges
WHERE database_name IN (
    'terdwh',
    'terdwh_uat',
    'terdwh_prd_datashare'
);

SELECT *
FROM svv_schema_privileges
WHERE database_name = 'terdwh_uat';


/*
=======================================================================
BLOK 16 - REDSHIFT DEV: TEST DATASHARE READ
=======================================================================
Waarom:
Controleren dat de PRD-data effectief leesbaar is op DEV via
driedelige namen (terdwh_prd_datashare.<schema>.<tabel>).
*/

SHOW SCHEMAS FROM DATABASE terdwh_prd_datashare;

SHOW TABLES FROM SCHEMA terdwh_prd_datashare.ingest;

select count(*) -- 36312414
from terdwh_prd_datashare.ingest.bc_api_general_ledger_entries
limit 10;

select *
from terdwh_prd_datashare.ingest.bc_api_general_ledger_entries
where systemcreatedat >= '2026-10-14'
