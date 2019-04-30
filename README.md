# Cichlid_database
range of scripts to populate or update R. Durbin's group cichlid database
- 'Cichlid_Population_dbv5.py' was used to initially populate the MySQL database with data from different sources
- 'Get_taxonomy_from_ENA.py' and 'Get_taxonomy_from_NCBI.py' are scripts to query ENA and NCBI taxonomy database, respectively. They are used in the population and update scripts. Species_name, taxon_id, rank_order and common_name are returned using species_name or taxon_id as input for the query (Note: ENA script only provide name and taxon_id).
- 'Cichlid_Population_db.json' file providing the database connection details
- 'Populating the Cichlid_database using Cichlid_Population_dbv5.py' describes the steps to initially populate the cichlid database (some steps have to be executed on the MySQL instance).
- 'Cichlid_db_update_v1.py' is a script to insert new records in or update/overwrite existing ones in the cichlid database using data from a Google spreadsheet template with validation fields (https://docs.google.com/spreadsheets/d/1eoVGpkX--R5FvFzj9jic8uiDoS-i9K6m3dY3_C4X96Y/edit?ts=5bc7080f#gid=1978536442).
- dao: directory with utilities to execute the MySQL statements
