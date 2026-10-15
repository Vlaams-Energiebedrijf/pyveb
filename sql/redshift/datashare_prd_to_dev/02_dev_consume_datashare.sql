/*
=======================================================================
AMAZON REDSHIFT - PRD DATA SHARING NAAR DEV / UAT
02 - DEV: SHARED DATABASE, UAT-DATABASE, ROLES EN RECHTEN
=======================================================================

Uitvoeren op:   Redshift DEV (consumer)
Host:           rsc-ter-dev.c7s4bhsdkddw.eu-west-1.redshift.amazonaws.com
Database:       terdwh      voor BLOK 7 - 8
                terdwh_uat  voor BLOK 9 - 14  (connectie wisselen in DBeaver!)
Uitgevoerd:     2026-10-15 door nicolas_vilain via DBeaver

HISTORISCH LOGBOEK: dit is de SQL zoals ze effectief uitgevoerd werd.
Het script is niet idempotent; niet opnieuw blind uitvoeren.

Volgorde:
    01_prd_create_datashare.sql   (PRD)
    02_dev_consume_datashare.sql  (DEV)          <- dit script
    03_dev_verify.sql             (DEV)

DEV Shared database:    terdwh_prd_datashare  (READ ONLY, live PRD-data)
DEV Lokale database:    terdwh_uat            (READ / WRITE)
DEV Lokale schemas:     rep_fin, rep_lev, rep_veb, rep_terra, uat_rep_terra,
                        trans_fin, trans_lev, trans_veb, vastgoed
                        (geen ingest / ingest_terra: die lees je uit de share)
Roles:                  terdwh_uat_reader  -> lezen
                        terdwh_uat_writer  -> lezen + schrijven
Gebruikers (writer):    nicolas_vilain, jan_van_der_sypt

-----------------------------------------------------------------------
GELEERDE LESSEN (eerste poging die faalde)
-----------------------------------------------------------------------
- GRANT USAGE ON DATABASE terdwh_uat faalt: dat privilege bestaat enkel
  voor shared databases, niet voor lokale. Weggelaten.
- GRANT ... FOR TABLES IN SCHEMA a, b, c werkte niet zoals verwacht;
  vervangen door GRANT ... FOR TABLES IN DATABASE terdwh_uat.

-----------------------------------------------------------------------
VERBETERPUNTEN / GENOMEN SHORTCUTS
-----------------------------------------------------------------------
- Shared database aangemaakt zonder WITH PERMISSIONS: USAGE op de
  database geeft alles-of-niets leesrecht op alle 11 schemas. Voor
  per-schema/per-tabel rechten op DEV moet de database opnieuw  aangemaakt worden met WITH PERMISSIONS.
- Rechten enkel toegekend aan 2 persoonlijke gebruikers, er is nog geen  service-user (bv. voor dbt Cloud).
- De schemas in terdwh_uat zijn eigendom van de admin die ze aanmaakte.
  GRANT ALL laat writers elkaars tabellen droppen/truncaten.
- Geen IF NOT EXISTS / geen rollback. Terugdraaien op DEV:
      DROP DATABASE terdwh_prd_datashare;
      DROP DATABASE terdwh_uat;
      DROP ROLE terdwh_uat_writer FORCE;
      DROP ROLE terdwh_uat_reader FORCE;
- Geen opkuis- of retentiebeleid voor PRD-kopieen in terdwh_uat.
=======================================================================
*/


/*
=======================================================================
BLOK 7 - REDSHIFT DEV: SHARED DATABASE AANMAKEN
=======================================================================
BELANGRIJK: DIT MOET UITGEVOERD WORDEN OP DEV (database terdwh)

Waarom/Wat: De shared database maakt de PRD-tabellen op DEV beschikbaar.
Deze database is read-only.
*/

SELECT
    current_database() AS database_name,
    current_user AS user_name;

CREATE DATABASE terdwh_prd_datashare
FROM DATASHARE prd_to_dev_datashare
OF NAMESPACE 'b5a395bb-921e-478c-ad78-31d32c702ef6';


/*
=======================================================================
BLOK 8 - REDSHIFT DEV: LOKALE UAT-DATABASE AANMAKEN
=======================================================================
Waarom:
In deze database slaan we de resultaten van onze queries op.
De data wordt fysiek opgeslagen op Redshift DEV.
*/

CREATE DATABASE terdwh_uat;

SELECT database_name, database_type
FROM svv_redshift_databases
WHERE database_name IN ('terdwh_uat', 'terdwh_prd_datashare');


/*
=======================================================================
BLOK 9 - REDSHIFT DEV: LOKAAL SCHEMA AANMAKEN
=======================================================================
BELANGRIJK:
WISSEL NU IN DBEAVER NAAR DATABASE terdwh_uat OP REDSHIFT DEV.

Waarom:
De lokale schemas worden gebruikt om nieuwe tabellen
en queryresultaten lokaal op te slaan.
*/

SELECT current_database();
CREATE SCHEMA rep_fin;
CREATE SCHEMA rep_lev;
CREATE SCHEMA rep_veb;
CREATE SCHEMA rep_terra;
CREATE SCHEMA trans_fin;
CREATE SCHEMA trans_lev;
CREATE SCHEMA trans_veb;
CREATE SCHEMA vastgoed;
CREATE SCHEMA uat_rep_terra;


/*
=======================================================================
BLOK 10 - REDSHIFT DEV: ROLES AANMAKEN
=======================================================================
Waarom:
Leesrechten en schrijfrechten worden gescheiden beheerd.
Een writer krijgt automatisch ook de reader-rechten.
*/

CREATE ROLE terdwh_uat_reader;

CREATE ROLE terdwh_uat_writer;

GRANT ROLE terdwh_uat_reader
TO ROLE terdwh_uat_writer;


/*
=======================================================================
BLOK 11 - REDSHIFT DEV: READER-RECHTEN OP PRD-DATASHARE
=======================================================================
Waarom:
Gebruikers met de reader-role mogen de gedeelde PRD-data lezen.

De shared database werd zonder WITH PERMISSIONS aangemaakt.
Daarom gebruiken we USAGE op de shared database.
*/

GRANT USAGE ON DATABASE terdwh_prd_datashare
TO ROLE terdwh_uat_reader;


/*
=======================================================================
BLOK 12 - REDSHIFT DEV: READER-RECHTEN OP LOKALE UAT
=======================================================================
Waarom:
Readers moeten ook de lokaal opgeslagen UAT-tabellen kunnen lezen.
*/

GRANT USAGE ON SCHEMA rep_fin, rep_lev, rep_veb, rep_terra,
    trans_fin, trans_lev, trans_veb, vastgoed, uat_rep_terra
TO ROLE terdwh_uat_reader;

-- Leesrechten op bestaande tabellen.
GRANT SELECT ON ALL TABLES IN SCHEMA rep_fin, rep_lev, rep_veb, rep_terra,
    trans_fin, trans_lev, trans_veb, vastgoed, uat_rep_terra
TO ROLE terdwh_uat_reader;

-- Leesrechten op huidige en toekomstige tabellen.
GRANT SELECT FOR TABLES IN DATABASE terdwh_uat
TO ROLE terdwh_uat_reader;


/*
=======================================================================
BLOK 13 - REDSHIFT DEV: WRITER-RECHTEN OP LOKALE UAT
=======================================================================
Waarom:
Writers mogen tabellen aanmaken, data toevoegen en
bestaande tabellen aanpassen in de lokale schemas.
*/

-- Alle beschikbare schemarechten, inclusief USAGE en CREATE.
GRANT ALL ON SCHEMA rep_fin, rep_lev, rep_veb, rep_terra,
    trans_fin, trans_lev, trans_veb, vastgoed, uat_rep_terra
TO ROLE terdwh_uat_writer;

-- Alle beschikbare rechten op bestaande tabellen.
GRANT ALL ON ALL TABLES IN SCHEMA rep_fin, rep_lev, rep_veb, rep_terra,
    trans_fin, trans_lev, trans_veb, vastgoed, uat_rep_terra
TO ROLE terdwh_uat_writer;

-- Alle beschikbare rechten op huidige en toekomstige tabellen.
GRANT ALL FOR TABLES IN DATABASE terdwh_uat
TO ROLE terdwh_uat_writer;


/*
=======================================================================
BLOK 14 - REDSHIFT DEV: ROLE AAN GEBRUIKER TOEKENNEN
=======================================================================
Waarom:
De gebruiker krijgt de rechten van de writer-role,
en via role-overerving ook de reader-rechten.
*/

GRANT ROLE terdwh_uat_writer
TO "nicolas_vilain";

GRANT ROLE terdwh_uat_writer
TO "jan_van_der_sypt";
