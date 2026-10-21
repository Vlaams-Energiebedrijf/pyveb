/*
=======================================================================
AMAZON REDSHIFT - PRD DATA SHARING NAAR DEV / UAT
01 - PRD: DATASHARE AANMAKEN
=======================================================================

Uitvoeren op:   Redshift PRD (producer)
Host:           rsc-ter-prd.c7s4bhsdkddw.eu-west-1.redshift.amazonaws.com
Database:       terdwh
Uitgevoerd:     2026-10-15 door nicolas_vilain via DBeaver

HISTORISCH LOGBOEK: dit is de SQL zoals ze effectief uitgevoerd werd.
Het script is niet idempotent; niet opnieuw blind uitvoeren.

Volgorde:
    01_prd_create_datashare.sql   (PRD)          <- dit script
    02_dev_consume_datashare.sql  (DEV)
    03_dev_verify.sql             (DEV)

PRD namespace:          b5a395bb-921e-478c-ad78-31d32c702ef6
DEV namespace:          f8729010-ec91-457c-a851-5c9cb80b8fbf
PRD Datashare:          prd_to_dev_datashare

Gedeelde PRD-schemas (11):
    ingest, ingest_terra,
    rep_fin, rep_lev, rep_veb, rep_terra, uat_rep_terra,
    trans_fin, trans_lev, trans_veb,
    vastgoed

-----------------------------------------------------------------------
VERBETERPUNTEN / GENOMEN SHORTCUTS
-----------------------------------------------------------------------
- Governance: alle tabellen in de 11 schemas worden gedeeld, inclusief
  ingest en ingest_terra (ruwe brondata, mogelijk persoonsgegevens).
  Nagaan of dat nodig is, of beperken tot rep_* / trans_*.
- INCLUDENEW = TRUE geldt enkel voor nieuwe TABELLEN in reeds gedeelde
  schemas. Een nieuw PRD-schema moet expliciet toegevoegd worden:
      ALTER DATASHARE prd_to_dev_datashare ADD SCHEMA <schema>;
      ALTER DATASHARE prd_to_dev_datashare ADD ALL TABLES IN SCHEMA <schema>;
      ALTER DATASHARE prd_to_dev_datashare SET INCLUDENEW = TRUE FOR SCHEMA <schema>;
- Controleren welke objecten (tabellen EN views) effectief gedeeld zijn:
      SELECT object_type, object_name
      FROM svv_datashare_objects
      WHERE share_name = 'prd_to_dev_datashare'
      ORDER BY object_name;
- Er is geen rollback-script. Terugdraaien op PRD:
      REVOKE USAGE ON DATASHARE prd_to_dev_datashare
      FROM NAMESPACE 'f8729010-ec91-457c-a851-5c9cb80b8fbf';
      DROP DATASHARE prd_to_dev_datashare;
  (eerst op DEV: DROP DATABASE terdwh_prd_datashare;)
=======================================================================
*/


/*
=======================================================================
BLOK 1 - REDSHIFT PRD: CONTROLE
=======================================================================
Waarom:
Controleren of we verbonden zijn met de juiste PRD-brondatabase
en of de elf schemas bestaan.
*/

SELECT
    current_database() AS database_name,
    current_user AS user_name;

SELECT
    nspname AS schema_name
FROM pg_namespace
WHERE nspname IN (
    'ingest',
    'ingest_terra',
    'rep_fin',
    'rep_lev',
    'rep_veb',
    'rep_terra',
    'trans_fin',
    'trans_lev',
    'trans_veb',
    'vastgoed',
'uat_rep_terra'
)
ORDER BY nspname;


/*
=======================================================================
BLOK 2 - REDSHIFT PRD: DATASHARE AANMAKEN
=======================================================================
*/

CREATE DATASHARE prd_to_dev_datashare;


/*
=======================================================================
BLOK 3 - REDSHIFT PRD: SCHEMAS TOEVOEGEN
=======================================================================
Waarom/wat: We delen alleen de elf expliciet geselecteerde schemas.
*/

ALTER DATASHARE prd_to_dev_datashare ADD SCHEMA ingest;
ALTER DATASHARE prd_to_dev_datashare ADD SCHEMA ingest_terra;
ALTER DATASHARE prd_to_dev_datashare ADD SCHEMA rep_fin;
ALTER DATASHARE prd_to_dev_datashare ADD SCHEMA rep_lev;
ALTER DATASHARE prd_to_dev_datashare ADD SCHEMA rep_veb;
ALTER DATASHARE prd_to_dev_datashare ADD SCHEMA rep_terra;
ALTER DATASHARE prd_to_dev_datashare ADD SCHEMA trans_fin;
ALTER DATASHARE prd_to_dev_datashare ADD SCHEMA trans_lev;
ALTER DATASHARE prd_to_dev_datashare ADD SCHEMA trans_veb;
ALTER DATASHARE prd_to_dev_datashare ADD SCHEMA vastgoed;
ALTER DATASHARE prd_to_dev_datashare ADD SCHEMA uat_rep_terra;


/*
=======================================================================
BLOK 4 - REDSHIFT PRD: BESTAANDE TABELLEN DELEN
=======================================================================
Waarom/wat: Een schema toevoegen betekent niet dat de tabellen automatisch
gedeeld zijn. Daarom voegen we de bestaande tabellen toe.
*/

ALTER DATASHARE prd_to_dev_datashare
ADD ALL TABLES IN SCHEMA ingest;

ALTER DATASHARE prd_to_dev_datashare
ADD ALL TABLES IN SCHEMA ingest_terra;

ALTER DATASHARE prd_to_dev_datashare
ADD ALL TABLES IN SCHEMA rep_fin;

ALTER DATASHARE prd_to_dev_datashare
ADD ALL TABLES IN SCHEMA rep_lev;

ALTER DATASHARE prd_to_dev_datashare
ADD ALL TABLES IN SCHEMA rep_veb;

ALTER DATASHARE prd_to_dev_datashare
ADD ALL TABLES IN SCHEMA rep_terra;

ALTER DATASHARE prd_to_dev_datashare
ADD ALL TABLES IN SCHEMA trans_fin;

ALTER DATASHARE prd_to_dev_datashare
ADD ALL TABLES IN SCHEMA trans_lev;

ALTER DATASHARE prd_to_dev_datashare
ADD ALL TABLES IN SCHEMA trans_veb;

ALTER DATASHARE prd_to_dev_datashare
ADD ALL TABLES IN SCHEMA vastgoed;

ALTER DATASHARE prd_to_dev_datashare
ADD ALL TABLES IN SCHEMA uat_rep_terra;


/*
=======================================================================
BLOK 5 - REDSHIFT PRD: TOEKOMSTIGE TABELLEN AUTOMATISCH DELEN
=======================================================================
Waarom:
Nieuwe tabellen in deze schemas moeten later automatisch
beschikbaar worden in de datashare.
*/

ALTER DATASHARE prd_to_dev_datashare
SET INCLUDENEW = TRUE FOR SCHEMA ingest;

ALTER DATASHARE prd_to_dev_datashare
SET INCLUDENEW = TRUE FOR SCHEMA ingest_terra;

ALTER DATASHARE prd_to_dev_datashare
SET INCLUDENEW = TRUE FOR SCHEMA rep_fin;

ALTER DATASHARE prd_to_dev_datashare
SET INCLUDENEW = TRUE FOR SCHEMA rep_lev;

ALTER DATASHARE prd_to_dev_datashare
SET INCLUDENEW = TRUE FOR SCHEMA rep_veb;

ALTER DATASHARE prd_to_dev_datashare
SET INCLUDENEW = TRUE FOR SCHEMA rep_terra;

ALTER DATASHARE prd_to_dev_datashare
SET INCLUDENEW = TRUE FOR SCHEMA trans_fin;

ALTER DATASHARE prd_to_dev_datashare
SET INCLUDENEW = TRUE FOR SCHEMA trans_lev;

ALTER DATASHARE prd_to_dev_datashare
SET INCLUDENEW = TRUE FOR SCHEMA trans_veb;

ALTER DATASHARE prd_to_dev_datashare
SET INCLUDENEW = TRUE FOR SCHEMA vastgoed;

ALTER DATASHARE prd_to_dev_datashare
SET INCLUDENEW = TRUE FOR SCHEMA uat_rep_terra;


/*
=======================================================================
BLOK 6 - REDSHIFT PRD: DEV NAMESPACE AUTORISEREN
=======================================================================
Waarom/wat: Redshift DEV moet expliciet toestemming krijgen om de
PRD-datashare te consumeren.
*/

GRANT USAGE ON DATASHARE prd_to_dev_datashare
TO NAMESPACE 'f8729010-ec91-457c-a851-5c9cb80b8fbf';
