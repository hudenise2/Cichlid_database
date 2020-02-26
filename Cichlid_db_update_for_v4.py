#!/Library/Frameworks/Python.framework/Versions/3.7/bin/python3

import json
import collections
import pymysql
import logging
import argparse
import time
import sys
import re
import datetime
from urllib.request import urlopen
from dao.mysql.mysql_db_access_object import MySQLDataAccessObject
from dao.mysql import StudyDAO
import Get_taxonomy_from_NCBI as TaxUtils
import copy

__author__ = 'Hubert Denise, Feb. 2020'

'''Cichlid_db_updateV4.py automates entry of data from input template available on Google spreadsheets into the Cichlid db. Type Cichlid_db_update.py -h for help.
    Usage: Cichlid_db_updateV4.py (-o) (-v) (-c) (-sp <'input'>)
    The script will import data from input template spreadsheet. See spreadsheets for format.
    They will be parsed as a dictionary with 'individual_name' as keys and table dictionaries as values.
    {'individual_name':{'table1':{'field1':value, 'field2': value...}, 'table2':{'fielda':value, 'fieldb': value...}, 'individual_name2': {...} }}
'''

req_version = (3, 4)
cur_version = sys.version_info

# check python version
if cur_version < req_version:
    print("Your Python interpreter is too old. You need version %d.%d.x" % req_version)
    sys.exit()

'''
usage: Cichlid_db_update.py automates entry of data from input template spreadsheets into the Cichlid db. Type Cichlid_db_update.py -h for help.
    Usage: Cichlid_db_update (-o) (-v) (-c) (-sp <'input'>)
'''

def adjust_header_list(line):
    '''To cope with the fact that not all fields will be present in data to upload '''
    full_header_list=['record-option','individual-name','individual-alias', 'individual-sex', 'species-name','species-taxon_id','species-common_name','species-taxon_position',
     'individual-date_collected', 'individual-collection_method', 'individual-collection_details','provider-provider_name', 'location-country_of_origin', 'location-location',
     'location-sub_location','location-latitude','location-longitude','developmental_stage-name', 'individual_data-weight', 'individual_data-unit', 'organism_part-name',
     'individual-comment', 'image-filename','image-filepath','image-comment', 'image-licence','material-name','material-accession','material-type', 'material-date_received',
     'material-storage_condition', 'material-storage_location', 'material-amount','material-unit', 'material-comment', 'mat_provider-provider_name','project-name',
     'project-alias', 'project-ssid','project-accession', 'sample-name', 'sample-accession','sample-ssid','sample-comment', 'lane-name','lane-accession', 'library_type-name',
     'library-ssid','file-name','file-accession','file-format', 'file-type','file-md5', 'file-location','file-comment', 'file-nber_reads', 'seq_centre-name','seq_tech-name']
    full_column_list=['option', 'individual_name', 'alias', 'sex', 'species_name', 'taxon_id', 'common_name', 'taxon_position', 'date_collected', 'collection_method', 'collection_details',
     'collector_name', 'country', 'location', 'location_details', 'latitude', 'longitude', 'developmental_name', 'individual_weight', 'unit', 'organism_part', 'individual_comment', 'image_name',
     'image_path', 'image_comment', 'image_licence', 'material_name', 'material_accession', 'material_type', 'date_received', 'storage_condition', 'material_location',
     'material_amount', 'material_unit', 'material_comment', 'material_provider_name', 'project_name', 'project_alias', 'project_ssid', 'project_accession', 'sample_name', 'sample_accession',
     'sample_ssid', 'sample_comment', 'lane_name', 'lane_accession', 'library_name', 'library_ssid', 'file_name', 'file_accession', 'format', 'paired-end', 'md5', 'filepath', 'file_comment',
     'nber_reads', 'seq_centre', 'seq_tech']
    #create list of indexes for the column present in the spreadsheet
    if line.startswith('HEADER'):
        LINE=line.split("\t")[1:]
        index_list=[full_column_list.index(k) for k in LINE if k != "''"]
    else:
        index_list=[full_column_list.index(k) for k in line.split("\t") if k != "''"]
    #create list of corresponding headers
    header_list=[full_header_list[k] for k in index_list]
    if verbose and len(full_header_list) != len(header_list): logging.info(" header adjusted from "+str(len(full_header_list)) +" columns to "+str(len(header_list))+" columns.")
    return header_list

def compare_and_overwrite_data(table, new_data, database_data, index_dic, cv_id, studyDAO):
    '''Function to compare database data and new data and decide if overwrite has to take place and carried it'''
    update_flag=0
    dependent_table =  ['developmental_stage', 'organism_part', 'individual', 'image', 'material', 'sample', 'lane', 'library', 'individual_data', 'file', 'table']
    dependent_dic={'developmental_stage':['ontology'], 'organism_part':['ontology'], 'individual' : ['species', 'location', 'provider'],
     'material' : ['individual', 'mat_provider', 'developmental_stage', 'organism_part'], 'sample':['material'], 'library' : ['library_type'], 'lane' : ['seq_tech', 'sample','library', 'seq_centre'],
     'file':['lane'], 'image':['individual'], 'individual_data': ['individual', 'cv'], 'table' : ['table_name', 'table_id']}
    identifier_dic = {'individual' : 'name', 'species' : 'name', 'material' : 'name', 'location' : 'location', 'ontology' : 'name', 'lane': 'accession',
      'file' : 'name', 'developmental_stage' : 'name', 'project': 'name', 'sample': 'name', 'image' : 'filename', 'organism_part': 'name', 'provider' : 'provider_name', 'mat_provider' : 'provider_name',
      'cv' : 'attribute', 'seq_centre': 'name', 'seq_tech' : 'name', 'library_type' : 'name', 'library' : 'ssid', 'annotations': 'table+table_id'}
    if verbose: logging.info("  => compare and overwrite data")
    #check if the data are the same between new_data and database_data
    specific_database_data = {k:str(v) for k, v in database_data.items() if k in new_data.keys()}
    #to be able to properly compare the numerical value coming from the database
    if table =='material' and 'amount' in specific_database_data:
        specific_database_data['amount']=float(specific_database_data['amount'])
    differences_dic={k:v for k,v in new_data.items() if v!=specific_database_data[k]}
    overwrite_flag=0
    if table != 'individual_data':
        index_dic[table]=database_data[table+"_id"]
    #if same: no overwrite so return the id
    if len(differences_dic)== 0:
        if verbose: logging.info("  - no differences so no overwrite")
        return overwrite_flag, index_dic
    # if not the same:
    else:
        if verbose: logging.info(" - differences exist")
        #independent_table cases (project, provider, species, location, library_type, seq_tech, seq_centre, cv and ontology)
        if table not in dependent_table:
            update_statement=[k+"='"+str(v)+"'" for k, v in differences_dic.items()]
            #cases where there is changed and latest fields (provider, species, )
            if table in ['provider', 'species']:
                try:
                    studyDAO.update(table, ",".join(update_statement)+", changed = '"+today+"'", table+"_id", database_data[table+"_id"])
                except:
                    logging.info("Could not update the dependent table in the database. Existing now")
                    sys.exit()
                if verbose: logging.info(update_statement)
            #cases where there is no changed and latest fields in table (project, location, library_type, seq_tech, seq_centre, cv and ontology)
            else:
                try:
                    studyDAO.update(table, ",".join(update_statement), table+"_id", database_data[table+"_id"])
                except:
                    logging.info("Could not update the dependent table in the database. Existing now")
                    sys.exit()
                #insert in annotations table the date on which data were overwritten (to keep record of changes)
                insert_data("annotations", {"table_name": table, "table_id": database_data[table+"_id"], "cv_id": cv_id, "value": today}, ",","","", studyDAO)

                if verbose: logging.info(field_str+" , "+value_str)
            overwrite_flag+=1
        #dependent_table cases (developmental_stage, organism_part, individual, image, material, sample, lane, library, individual_data, file, table)
        else:
            update_statement=[k+"='"+str(v)+"'" for k, v in differences_dic.items()]
            dependent_statement=[k+"_id ='"+str(v)+"'" for k, v in index_dic.items() if k in dependent_dic[table]]
            TABLE=table
            identifier=table+"_id"
            if table=='individual_data':
                identifiant_statement= str(database_data["individual_id"]) + "' and cv_id='"+str(database_data["cv_id"])
                identifier="individual_id"
            else:
                identifiant_statement=database_data[table+"_id"]
            #annotations table case: data will be overwritten
            if table=='table':
                TABLE='annotations'
                dependent_statement=[]
                identifiant_statement=database_data['table_id'] + "' and table_name ='"+database_data['table_name']
            #cases where there is changed and latest fields
            if table not in ['organism_part', 'developmental_stage', 'table']:
                try:
                    studyDAO.update(TABLE, ",".join(update_statement+dependent_statement)+", changed = '"+today+"'", identifier, identifiant_statement)
                except:
                    logging.info("Could not update the independent table "+table+" in the database. Existing now1")
                    sys.exit()
                if verbose: logging.info(update_statement)
            else:
                try:
                    studyDAO.update(TABLE, ",".join(update_statement+dependent_statement), table+"_id", identifiant_statement)
                except:
                    logging.info("Could not update the independent table "+table+" in the database. Existing now2")
                    sys.exit()
        if cv_id != 6 and table != 'table' and table != 'individual_data':
            insert_data("annotations", {"table_name": table, "table_id": database_data[table+"_id"], "cv_id": cv_id, "value": today}, ",","","", studyDAO)
            if verbose: logging.info(field_str, value_str)
    overwrite_flag+=1
    if verbose: logging.info(" - dictionary of index for the tables")
    if verbose: logging.info(index_dic)
    if verbose: logging.info("\n")
    return overwrite_flag, index_dic

def compare_and_update_data(table, new_data, database_data, studyDAO):
    '''Function to compare database data and new data and decide if update has to take place and carried it'''
    update_flag=0
    return_index=0
    identifier_dic = {'individual' : ['name'], 'species' : ['name'], 'material' : ['name'], 'location' : ['location'], 'ontology' : ['name'], 'lane': ['accession'],
     'file' : ['name'], 'developmental_stage' : ['name'], 'project': ['name'], 'sample': ['name'], 'image' : ['filename'], 'organism_part': ['name'], 'provider' : ['provider_name'],
     'cv' : ['attribute'], 'seq_centre': ['name'], 'seq_tech' : ['name'], 'library_type' : ['name'], 'library' : ['ssid'], 'table': ['table_name', 'table_id']}
    if verbose: logging.info("    - comparing and updating data into database for table "+table)
    #check if the data are the same between new_data and database_data
    specific_database_data = {k:str(v) for k, v in database_data.items() if k in new_data.keys()}
    if table =='material' and 'amount' in specific_database_data:
        specific_database_data['amount']=float(specific_database_data['amount'])
    differences_dic={k:v for k,v in new_data.items() if v!=specific_database_data[k]}
    #if same: no update so return the id
    if len(differences_dic)==0:
        if verbose: logging.info("    - no differences so no update took place")
        return update_flag, database_data[table+"_id"]
    # if not the same:
    else:
        if verbose: logging.info("    - differences exist so update")
        #set latest=0 for current data
        if 'latest' in database_data.keys():
            if verbose: logging.info("    - update latest field")
            if 'row_id' in database_data.keys():
                table_identifier='row_id'
                table_identifiant=database_data['row_id']
                del database_data['row_id']
            else:
                table_identifier=table+'_id'
                table_identifiant=database_data[table+'_id']
            try:
                studyDAO.update(table, "latest=0", table_identifier, table_identifiant)
            except:
                logging.info("Could not update the data in the database. Existing now")
                sys.exit()
        #create new entry with new_data and latest=1
        filtered_database_dic={k:v for k,v in database_data.items() if k not in differences_dic.keys() and k not in ('changed', 'latest')}
        data_to_insert={**differences_dic, **filtered_database_dic}
        if verbose: logging.info("    - insert new data into table "+table+": "+str(data_to_insert))
        if table not in ['project', 'developmental_stage', 'organism_part', 'location', 'seq_centre', 'library_type', 'cv', 'seq_tech', 'table']:
            insert_data(table, data_to_insert, ",", "","", studyDAO)
            new_index= studyDAO.getIndex(table, identifier_dic[table][0], "'"+database_data[identifier_dic[table][0]] +"' and latest=1")
            return_index=new_index[0][table+"_id"]
            if verbose: logging.info("    - new index for table "+table +": "+str(return_index))
        else:
            for identifier in identifier_dic[table]:
                del data_to_insert[identifier]
            #to deal with annotations table case (new data will be appended for this table)
            if table=="table":
                TABLE='annotations'
                data_to_insert['value']=str(database_data['value'])+"; "+str(data_to_insert['value'])
                field_statement=prepare_update(data_to_insert)
                statement_identifiant=statement_identifiant+"' and table_id = '"+str(database_data["table_id"])
            else:
                field_statement=prepare_update(data_to_insert)
                statement_identifier= identifier_dic[table][0]
                statement_identifiant=database_data[statement_identifier]
                TABLE=table
            if verbose: logging.info("    - update statement" + field_statement+" where "+statement_identifier+": "+statement_identifiant)
            try:
                studyDAO.update(TABLE, field_statement, statement_identifier, statement_identifiant)
            except:
                logging.info("Could not update the data in the database. Existing now")
                sys.exit()
            if table != 'table':
                new_index= studyDAO.getIndex(table, identifier_dic[table][0], "'"+database_data[identifier_dic[table][0]]+"'")
                return_index=new_index[0][table+"_id"]
                if verbose: logging.info("    - new index for table "+table +": "+str(return_index))
            else:
                if verbose: logging.info("    - no index for table annotations")
            update_flag+=1
        if verbose: logging.info("    - number of update for table "+table +": "+str(update_flag))
        return update_flag, return_index

def create_new_name(new_dic, db_dic, studyDAO):
    '''To create unique name for individual if there are already individual with same name'''
    existing_names=[v for k,v in db_dic['individual'].items() if k=='name']
    #create suitable suffix to ensure that new entry has unique name. If no previous entry, use the given name
    if len(existing_names)==1:
        new_dic['individual']['name']=existing_names[0]+"[2]"
    elif len(existing_names) > 1:
        suffixes=[int(k[k.index("[")+1:-1]) for k in existing_names if "[" in k]
        max_value=max(suffixes)
        new_dic['individual']['name']=new_dic['individual']["name"]+"["+str(max_value+1)+"]"
        #replace material_name if new individual name was created
        if 'material' in new_dic and previous_name==new_data['material']['name']:
            new_dic['material']['name']=new_dic['individual']['name']
    return new_dic

def dic_to_str(dic, field_attribute="", value_attribute=""):
    '''
    function to transform a dictionary into strings for fields and values to populate the database
    : input dic (dic) dicionary to insert into db
    : input field_attribute (str) string of fields to add to the dictionary ones
    : input value_attribute (str) string of corresponding values to add to the dictionary ones
    : return field_str (str) field list separated by comma
    : return val_str (str) corresponding value list separated by comma and apostrophes if necessary
    '''
    if verbose: logging.info("      - transforming data into string "+str(dic)+" "+field_attribute+" "+str(value_attribute))
    new_list=[]
    field_list = list(dic.keys())
    val_list = list(dic.values())
    #rewrite  the values in mysql compatible format
    for element in val_list:
        if type(element) == str or type(element) == datetime.date:
             element ="'"+str(element)+"'"
        new_list.append(element)
    field_str = (", ".join(field_list) + field_attribute).replace('weight', 'value')
    val_str =  ", ".join(str(x) for x in new_list) + str(value_attribute)
    val_str = val_str.replace('None', 'Null')
    if verbose: logging.info("      - field: "+field_str+", value: "+val_str)
    return field_str, val_str

def dispatch_data(raw_results, annotations_data, entry_name, studyDAO, mydbconn):
    '''
    generic function to call for update/population of the database with data from spreadsheet
    : input raw_results (dic) parsed data from the spreadsheet
    : input entry_name (str) name of the Google spreadsheet tab
    : input studyDAO (connection object) object to connect to the database
    : input mydbconn (database connection_socket) connection to the Cichlid database
    : return none
    '''
    dependent_table =  ['developmental_stage', 'organism_part', 'individual', 'image', 'material', 'sample', 'lane', 'library', 'individual_data', 'file']
    table_eq_Annotations ={'individual' : 'individual_data','material': 'material','morphology': 'individual_data','extraction': 'material',
    'sequencing':'sample','image': 'image', 'files' : 'file', 'lab_note' : 'material','general' : 'individual_data'}
    raw_results_type = 'spreadsheet'
    insert_flag=0
    update_flag=0
    overwrite_flag=0
    for individual_name in raw_results:
        if verbose: logging.info("Dispath data for individual name: "+individual_name)
        #more than one entry per individual could be encountered
        for index in range(0,len(raw_results[individual_name])):
            if 'record' in raw_results[individual_name][index]:
                if raw_results[individual_name][index]['record']['option']=='update':
                    if verbose: logging.info(" UPDATE RECORD FROM DATABASE")
                    update_flag=update_entry(raw_results[individual_name][index], annotations_data[individual_name][index], studyDAO)
                elif raw_results[individual_name][index]['record']['option']=='overwrite':
                    if verbose: logging.info(" OVERWRITE RECORD FROM DATABASE")
                    overwrite_flag=overwrite_entry(raw_results[individual_name][index], annotations_data[individual_name][index], studyDAO)
                else:
                    if verbose: logging.info(" INSERT RECORD INTO DATABASE")
                    insert_flag=insert_entry(raw_results[individual_name][index], annotations_data[individual_name][index], studyDAO)
            else:
                if verbose: logging.info(" INSERT RECORD INTO DATABASE")
                insert_flag=insert_entry(raw_results[individual_name][index], annotations_data[individual_name][index], studyDAO)
            if insert_flag > 0 or update_flag>0 or overwrite_flag>0:
                logging.info("Committing data from the "+raw_results_type+" "+entry_name+" to the database")
                if insert_flag > 0: logging.info(" - "+str(insert_flag)+" insertions took place")
                if update_flag > 0: logging.info(" - "+str(update_flag)+" updates took place")
                if overwrite_flag > 0: logging.info(" - "+str(overwrite_flag)+" overwritting took place")
            if verbose: logging.info("Committing data changes into the database")
            try:
                mydbconn.commit()
            except:
                logging.error("Rolling back database changes...")
                mydbconn.rollback()
                logging.error("The program failed to import data from the "+raw_results_type+" "+entry_name+" with the below exception:")
                raise

def format_date(entry_date):
    '''
    function to re-format the date fields from the data fromm the spreadsheet in mysql compatible format (YYYY-MM-DD)
    : input entry_date (str) date as provided in the spreadsheet
    : return return_date (date) date formatted as YYYY-MM-DD (only numeric and separated by '-')
    '''
    pattern_date=re.compile(r'^[0-3][0-9]/[0-1][0-9]/2[0-1][0-3][0-9]')
    return_date = entry_date
    if pattern_date.match(entry_date):
        date_field_part = entry_date.split("/")
        date_part = date_field_part[2]+"-"+date_field_part[1]+"-"+date_field_part[0]
        return_date=datetime.datetime.strptime(date_part, "%Y-%m-%d").date()
    if verbose: logging.info("      + format data from "+str(entry_date) +" to "+str(return_date))
    return str(return_date)

def format_to_compare(dic):
    '''
    Function to reformat the latitude and longitude entries to compare them (ensure it is a string and that it does not end by '0')
    : input dic (dic) dictionary with location_field:value where location_fields are the fields from the location entry from the spreadsheet
    : return dic (dic) updated location directory
    '''
    previous_data=""
    for field in ['latitude', 'longitude']:
        previous_data+=", "+str(dic[field])
        dic[field] = str(dic[field])
        #make sure that last digit is not considered if equal to 0
        if dic[field][-1] =='0':
            dic[field] = dic[field][:-1]
    if verbose: logging.info("      + reformat latitude and longitude from "+previous_data[2:]+" to "+str(dic['latitude'])+", "+str(dic['longitude']))
    return dic

def get_cv_id(studyDAO):
    '''query the database to get the relevant cv_id'''
    cv_dic={}
    cv_data = studyDAO.getTableData("cv", "cv_id, comment", "attribute = 'notes' and comment like 'entry for%'")
    if len(cv_data) > 0:
        for entry in cv_data:
            table=entry['comment'].replace('entry for table ','')
            cv_dic[table]=entry['cv_id']
        if verbose: logging.info("      + get cv_id(s) from table cv: "+str(cv_dic))
    return cv_dic

def get_database_data(data, studyDAO, flag):
    '''query the database to get all data linked to an individual name'''
    if verbose: logging.info("      + getting database data")
    database_dic={}
    identifier_dic = {'individual' : 'name', 'species' : 'name', 'material' : 'name', 'location' : 'location', 'ontology' : 'name', 'lane': 'accession',
     'file' : 'name', 'developmental_stage' : 'name', 'project': 'name', 'sample': 'name', 'image' : 'filename', 'organism_part': 'name', 'provider' : 'provider_name',
     'cv' : 'attribute', 'seq_centre': 'name', 'seq_tech' : 'name', 'library_type' : 'name', 'library' : 'ssid' }#, 'annotations': 'table+table_id'}
    table_list= (list(data.keys()))
    table_list.remove('record')
    #ensure that individual table is treated at the end as it has most dependencies
    table_list.remove('individual')
    table_list.append('individual')
    #remove and individual_data table to treat them separately
    if 'individual_data' in table_list: table_list.remove('individual_data')
    #deal with case where provider is provider for both individual and material table (if last case, 'mat_provider' will be present in table_list)
    if 'mat_provider' in table_list:
        material_provider=data["mat_provider"]
        table_list.remove("mat_provider")
        mat_provider=studyDAO.getIndex("provider", "provider_name", "'"+material_provider["provider_name"]+"'")
        #if provider_name is present into the db, add index to database_dic otherwise, insert name into the provider table before geting the index
        if len(mat_provider) == 0:
            studyDAO.populate_table("provider", "(provider_name, changed, latest)", "('"+material_provider["provider_name"]+"', '"+today+"', 1)")
            mat_provider=studyDAO.getIndex("provider", "provider_name", "'"+material_provider["provider_name"]+"'")
        database_dic["mat_provider"]=mat_provider[0]["provider_id"]
    asso_dic={}
    insert_dic={}
    #go through the tables refered on the spreadsheet
    for table in table_list:
        #get all data  for the entry using the identifier (latest entry if the field 'latest' is present)|
        if table not in ['project', 'developmental_stage', 'organism_part', 'location', 'seq_centre', 'library_type', 'seq_tech', 'individual']:
            if identifier_dic[table] in data[table]:
                db_table_data = studyDAO.getStudyData(table, "latest = 1 and "+identifier_dic[table], data[table][identifier_dic[table]])
        elif table=='individual':
            #if there is several individual with the same name, check which one need to be evaluated and eventually updated
            #alias has to be used also to define individual; if no alias, then species_id & sex could be used
            if 'alias' in data[table]:
                db_table_data = studyDAO.getStudyData(table, "latest = 1 and "+identifier_dic[table]+" like '" +data[table][identifier_dic[table]]+"%' and alias", str(data[table]['alias']))
            else:
                db_table_data = studyDAO.getStudyData(table, identifier_dic[table]+" like '" +data[table][identifier_dic[table]]+"%' and latest", '1')
        else:
            identifier=identifier_dic[table]
            if table == 'location' and 'location' in data[table]:
                value = data[table]['location']
                data_location = dict(data[table])
                del data_location['location']
                identifier_value = value + "' and " + str(data_location)[2:-2].replace("':","=").replace(", '", " and ")
            else:
                identifier_value = data[table][identifier_dic[table]]
            db_table_data = studyDAO.getStudyData(table, identifier, identifier_value)
        #if data are already present in the database  and return dic with id or data depending on the flag
        if len(db_table_data) > 0:
            if flag=='I':
                if table+"_id" in db_table_data[0]:
                    database_dic[table]=db_table_data[0][table+"_id"]
            elif flag=='U' or flag=='O':
                database_dic[table]=db_table_data[0]
    if verbose: logging.info("      + "+str(database_dic))
    return database_dic

def insert_data(table, table_data, index, field_attribute, data_attribute, studyDAO):
    '''function to insert new data for every table into the database'''
    if verbose: logging.info("      + inserting data into database for table "+table)
    #prepare data to insert depending the nature of the table
    if table not in ['project', 'developmental_stage', 'organism_part', 'location', 'seq_centre', 'library_type', 'cv', 'seq_tech', 'allocation', 'annotations']:
        field_str, value_str = dic_to_str(table_data, field_attribute+", changed, latest ", data_attribute+", '"+today+"', 1")
    else:
        if isinstance(index, int):
            field_str, value_str = dic_to_str(table_data, ","+table+"_id", ","+str(index+1))
        else:
            field_str, value_str = dic_to_str(table_data)
    try:
        studyDAO.populate_table(table, "("+field_str+")", "("+value_str+")")
    except:
        logging.info("Could not insert the new dependent data in table "+table+" the database. Existing now")
        sys.exit()
    if verbose: logging.info("      + data: "+field_str+", "+value_str)

def insert_entry(new_data, annotations_data, studyDAO):
    '''manage the fate of the data if 'new_record' flag has been provided'''
    dependent_table =  ['developmental_stage', 'organism_part', 'individual', 'image', 'material', 'sample', 'lane', 'library', 'individual_data', 'file']
    independent_table =['species', 'provider', 'location',  'project', 'library_type', 'seq_centre', 'seq_tech']
    #check if entry is in the database
    insert_flag=0
    database_data = get_database_data(new_data, studyDAO, 'I')
    #independent table do not have dependencies so can be inserted directly
    for table in independent_table:
        #not already in db: insert has then to take place
        if table in new_data and table not in database_data:
            if verbose: logging.info("C-INSERT ENTRY")
            previous_index=studyDAO.getmaxIndex(table)[0]['max('+table+'_id)']
            insert_data(table, new_data[table], previous_index, ","+table+"_id", ","+ str(previous_index+1), studyDAO)
            #update the flag value
            insert_flag+=1
            #update database_data with new index for entry
            database_data[table]=previous_index+1
    #dependent table have dependencies so this has to be taken into account
    for table in dependent_table:
        if verbose: logging.info("  C1 - inserting entry for dependent_table: "+table)
        #dictionary of table and their dependencies
        dependent_dic={'developmental_stage':['ontology'], 'organism_part':['ontology'], 'individual' : ['species', 'location', 'provider'],
        'material' : ['individual', 'provider', 'developmental_stage', 'organism_part'], 'sample':['material'], 'library' : ['library_type'], 'lane' : ['seq_tech', 'sample','library', 'seq_centre'],
        'file':['lane'], 'image':['individual'], 'individual_data': ['individual', 'cv']}
        input_name=spath.split("/")[-1]
        if table in new_data:
            #individual name has to be unique. Therefore suffix has to be added if same name when flag is new_record
            if table=='individual':
                new_name_flag=0
                field_list=[x for x,y in new_data[table].items() if y != "" and x != 'name']
                previous_name=new_data[table]['name']
                original_new_individual_data=dict(new_data[table])
                del original_new_individual_data['name']
                check_statement = prepare_update(original_new_individual_data)
                #get data already in database
                # check if there is entry with all parameters from submitted data
                param_name=studyDAO.getStudyData(table, "latest", "1' and "+ check_statement.replace(", "," and ")+" and name like '"+previous_name+"%")
                # if there is no results with all parameters so check if alias can be used for identity
                if len(param_name)==0 and 'alias' in new_data[table]:
                        param_name=studyDAO.getStudyData(table, "latest", "1' and alias ='"+new_data[table]['alias']+"' and  name like '"+previous_name+"%")
                # there is no results with all parameters nor alias only: get data with name only
                if len(param_name)==0:
                        names=studyDAO.getStudyData(table, "latest", "1' and name like '"+previous_name+"%")
                # there is results with all parameters or alias so use the individual_id found in the database
                if len(param_name) > 0:
                    new_data[table]['name']=param_name[0]['name']
                    database_data["individual"]=param_name[0]['individual_id']
                else:
                    # there is no results with all parameters or alias so check if there is ground to create new entry in individual table
                    if len(names) > 0:
                        #get fields from the new_data and compare to the database data
                        new_data_filtered={x:y for x,y in new_data[table].items() if x != 'name' and y !=""}
                        db_data={x:y for x,y in names[0].items() if x in new_data_filtered.keys() and y is not None}
                        diff_dic={x:y for x,y in new_data_filtered.items() if str(new_data_filtered[x]) != str(db_data[x])}
                        # if there is no difference then use the individual_id found in the database
                        if len(diff_dic) == 0:
                            new_data[table]['name']=names[0]['name']
                            database_data["individual"]=names[0]['individual_id']
                        else:
                            # there is differences so create new name and insert into database
                            new_data=create_new_name(new_data, {"individual":names[0]}, studyDAO)
                            new_name_flag+=1
                    else:
                        # there is no entry in database so create new entry
                        new_name_flag+=1
                if new_name_flag !=0:
                    if verbose: logging.info("  C2 -individual name changed to "+new_data['individual']['name'])
                    #insert the new individual data
                    linked_dic={k+"_id":v for k,v in database_data.items() if k in dependent_dic[table]}
                    link_attr, val_attr = dic_to_str(linked_dic)
                    previous_index=studyDAO.getmaxIndex(table)[0]['max('+table+'_id)']
                    if verbose: logging.info("  C3 - inserting data table: "+table+": "+str(new_data))
                    insert_data(table, new_data[table], previous_index, ", individual_id, "+link_attr, ", "+str(previous_index+1)+", "+val_attr, studyDAO)
                    database_data["individual"]=previous_index+1
                    #populate the allocation table
                    if 'project' in database_data:
                        if verbose: logging.info("  C4 - inserting data table: allocation: "+str({'project_id':str(database_data["project"]), 'individual_id': str(database_data["individual"])}))
                        insert_data("allocation", {'project_id':str(database_data["project"]), 'individual_id': str(database_data["individual"])}, ",", "","", studyDAO)
            #case where data are not already in the database for this table: insert
            if table not in database_data:
                if verbose: logging.info("  C5 - insert into table "+table)
                dep_dic={}
                for sub_table in dependent_dic[table]:
                    if verbose: logging.info("    C5a - get data from sub table: "+sub_table+" for table:"+table)
                    if sub_table=='ontology':
                        database_data['ontology']='NULL'
                        ontology_id=studyDAO.getIndex('ontology', 'name', "'"+new_data[table]['name']+"'")
                        if len(ontology_id) >0:
                            database_data['ontology']=ontology_id[0]['ontology_id']
                    if sub_table=='cv':
                        if 'weight' in new_data[table]:
                            criteria="'weight'"
                            cv_id=studyDAO.getIndex('cv', 'attribute', criteria)
                            database_data['cv']=cv_id[0]['cv_id']
                    #gather the sub_table ids with the others
                    if sub_table in database_data: dep_dic[sub_table+"_id"]= database_data[sub_table]
                if verbose: logging.info("    C5b -sub tables data: "+str(database_data))
                #prepare the insert statement
                dep_field= ",".join(list(dep_dic.keys()))
                dep_data=",".join([str(x) for x in dep_dic.values()])
                #insert data onto table: get index, prepare arguments then insert
                if table !='individual_data':
                    index=","+str(studyDAO.getmaxIndex(table)[0]['max('+table+'_id)'] +1 )+","
                    field=","+table+"_id, "
                else:
                    #if individual_data, there is no 'table_id' so adjust arguments
                    index=","
                    field=", "
                    dep_field+=", comment"
                    dep_data+=", 'extracted the "+today+" from "+input_name+"'"
                if verbose: logging.info("    C5c - inserting data into table: "+table+": "+str(new_data[table])+" "+field+ dep_field+" "+index+dep_data)
                #only enter weight in individual_data if value is provided
                if table =='individual_data':
                    if 'unit' in dep_field and 'weight' in dep_field:
                        insert_data(table, new_data[table], index, field+ dep_field, index+dep_data, studyDAO)
                else:
                    #deal with case where provider name is provided for the material
                    if table =="material" and "material" in new_data:
                        #if provider is provided, it will be added otherwise let blank.
                        prov_index=dep_field.split(",").index('provider_id')
                        list_dep_data=dep_data.split(",")
                        if "mat_provider" in database_data:
                                list_dep_data[prov_index]=str(database_data['mat_provider'])
                        else:
                                list_dep_data[prov_index]='NULL'
                        dep_data=",".join(list_dep_data)
                    insert_data(table, new_data[table], index, field+ dep_field, index+dep_data, studyDAO)
                insert_flag+=1
                #update database_data accordigly
                if table !='individual_data':
                    new_index=studyDAO.getmaxIndex(table)[0]['max('+table+'_id)']
                    database_data[table]=new_index
    #special case for comment fields
    if verbose: logging.info("  C6 - deal with comments and annotations table")
    for table in annotations_data:
        #return a dictionary with the cv_id corresponding to each table
        cv_dic=get_cv_id(studyDAO)
        #check if value is already in 'individual_data' or 'annotations'
        if table != 'individual':
            annotations_query=studyDAO.getTableData("annotations", "cv_id", "table_name = '" +table+"' and table_id= "+str(database_data[table]) +" and value = '"+str(annotations_data[table])+"'")
            if len(annotations_query)==0:
                if verbose: logging.info("  C6a - insert into table annotations: "+str({"table_name":table, "table_id": str(database_data[table]), "cv_id":str(cv_dic[table]), "value":str(annotations_data[table])}))
                insert_data("annotations", {"table_name":table, "table_id": str(database_data[table]), "cv_id":str(cv_dic[table]), "value":str(annotations_data[table])}, ",", "", "", studyDAO)
                insert_flag+=1
        else:
            if verbose: logging.info(" - insert into table individual_data")
            individual_data_query=studyDAO.getTableData("individual_data", "cv_id", "individual_id= "+str(database_data[table])+" and value = '"+str(annotations_data[table])+"'")
            if len(individual_data_query)==0:
                if verbose: logging.info("  C6b - insert into table individual_data: "+str({"individual_id":str(database_data[table]), "cv_id":str(cv_dic[table]), "value":str(annotations_data[table]), "comment": 'extracted the '+today+' from '+input_name}))
                insert_data("individual_data", {"individual_id":str(database_data[table]), "cv_id":str(cv_dic[table]), "value":str(annotations_data[table]), "comment": 'extracted the '+today+' from '+input_name}, ",", "", "", studyDAO)
                insert_flag+=1
    if verbose: logging.info("  - "+str(insert_flag)+" insertion(s) took place")
    return insert_flag

def overwrite_entry(new_data, annotations_data, studyDAO):
    '''manage the fate of the data if 'overwrite' flag has been provided'''
    dependent_table =  ['developmental_stage', 'organism_part', 'individual', 'image', 'material', 'sample', 'lane', 'library', 'individual_data', 'file']
    independent_table =['species', 'provider', 'location',  'project', 'library_type', 'seq_centre', 'seq_tech']
    #check if entry is in the database
    overwrite_flag=0
    database_data = get_database_data(new_data, studyDAO, 'O')
    if 'individual' in database_data:
        new_data['individual']['name']=database_data['individual']['name']
    #get cv_id when the date has to be overwritten (track changes when the tables do not have a 'changed' field)
    cv_id = studyDAO.getIndex("cv", "attribute", "'date overwritten'")[0]["cv_id"]
    novel_database_indexes={}
    database_indexes={}
    if verbose: logging.info("B-OVERWRITE ENTRY")
    for table in independent_table:
        if table in new_data:
            #if not alreay present: insert has then to take place
            if table not in database_data:
                #get the index of last entry in table
                previous_index=studyDAO.getmaxIndex(table)[0]['max('+table+'_id)']
                if verbose: logging.info(" B2 - insert into independent table "+table+": "+str(new_data[table]))
                if table not in ['project', 'location', 'ontology', 'cv', 'library_type']:
                    insert_data(table, new_data[table], previous_index, ","+table+"_id", ","+ str(previous_index+1), studyDAO)
                else:
                    insert_data(table, new_data[table], previous_index, ","+table+"_id, changed, latest", ","+ str(previous_index+1)+","+today+", 1", studyDAO)
                overwrite_flag+=1
                #update database_data with new index for entry
                novel_database_indexes[table]=previous_index+1
                database_indexes[table]=previous_index+1
            else:
                #if present: data have to be compared and overwritten accordingly
                independent_overwrite_flag, database_indexes= compare_and_overwrite_data(table, new_data[table], database_data[table], database_indexes, cv_id, studyDAO)
    overwrite_flag+=independent_overwrite_flag
    for table in dependent_table:
        dependent_dic={'developmental_stage':['ontology'], 'organism_part':['ontology'], 'individual' : ['species', 'location', 'provider'],
        'material' : ['individual', 'provider', 'developmental_stage', 'organism_part'], 'sample':['material'], 'library' : ['library_type'], 'lane' : ['seq_tech', 'sample','library', 'seq_centre'],
        'file':['lane'], 'image':['individual'], 'individual_data': ['individual', 'cv']}
        #if not alreay present: insert has then to take place
        if table not in database_data:
            #special case for individual_data as no individual_data_id
            if table != 'individual_data':
                #get the index of last entry in table
                previous_index=studyDAO.getmaxIndex(table)[0]['max('+table+'_id)']
                dependent_statement_dic={k+"_id":v for k, v in novel_database_indexes.items() if k in dependent_dic[table]}
                field_str, val_str = dic_to_str(dependent_statement_dic)
                database_indexes['individual']=previous_index+1
                #check if there is there is data to insert for the dependent table(s) to the table considered
                if verbose: logging.info(" B3 - insert into dependent table "+table+": "+str(new_data[table]))
                if len(field_str) > 1:
                    insert_data(table, new_data[table], previous_index, ","+table+"_id, " +field_str , ","+ str(previous_index+1)+"," + val_str, studyDAO)
                else:
                    insert_data(table, new_data[table], previous_index, ","+table+"_id" , ","+ str(previous_index+1), studyDAO)
                if verbose: logging.info(field_str +" , "+val_str)
                #update database_data with new index for entry
                novel_database_indexes[table]=previous_index+1
            else:
                individual_id=novel_database_indexes['individual']
                if 'weight' in new_data[table]:
                    cv_id= studyDAO.getIndex("cv", "attribute", "'weight'")[0]["cv_id"]
                    value=new_data[table]['weight']
                    unit=new_data[table]['unit']
                    individual_data_from_db=studyDAO.getStudyData("individual_data", "individual_id", str(novel_database_indexes["individual"]) +"' and cv_id ='"+str(cv_id))
                    if len(individual_data_from_db) == 0:
                        insert_data(table, new_data[table], 0, ", individual_id , cv_id", ","+ str(individual_id)+","+str(cv_id), studyDAO)
                    else:
                        if float(new_data[table]['weight']) != float(individual_data_from_db[0]['value']):
                            #data are present so delete previous entry
                            insert_data(table, new_data[table], 0, ", individual_id , cv_id, comment", ","+ str(individual_id)+","+str(cv_id)+",'"+individual_data_from_db[0]['comment']+"'", studyDAO)
                        try:
                            studyDAO.delete_data(table, "value", individual_data_from_db[0]['value']+" and individual_id =" + str(individual_id)+" and cv_id = "+str(cv_id))
                        except:
                            logging.info("Could not delete the data in the database. Existing now")
                            sys.exit()
                    if verbose: logging.info(" B4 - insert into table individual_data: "+str(new_data[table]))
            overwrite_flag+=1
        else:
            #compare the database data and new data
            dependent_overwrite_flag, novel_database_indexes = compare_and_overwrite_data(table, new_data[table], database_data[table], novel_database_indexes, cv_id, studyDAO)
            overwrite_flag+=dependent_overwrite_flag
    #deal with comments fields for the annotations table
    for table in annotations_data:
        #get the cv_ids for the tables with comment
        cv_id=studyDAO.getIndex('cv', 'comment', "'entry for table "+table+"'")[0]["cv_id"]
        table_to_overwrite="annotations"
        table_to_compare='table'
        annotations_data_from_db=studyDAO.getStudyData(table_to_overwrite, "table_name = '" +table+"' and cv_id= "+str(cv_id)+ " and table_id ", novel_database_indexes[table])
        to_insert_dic={"table_name":table, "table_id": novel_database_indexes[table], "cv_id":cv_id, "value": annotations_data[table]}

        if table=='individual':
            cv_id= studyDAO.getIndex("cv", "attribute", "'notes' and comment = 'entry for table individual'")[0]["cv_id"]
            table_to_overwrite='individual_data'
            table_to_compare='individual_data'
            annotations_data_from_db=studyDAO.getStudyData(table_to_overwrite, "cv_id ="+str(cv_id) +" and individual_id", novel_database_indexes[table])
            to_insert_dic={"individual_id":table, "cv_id":cv_id, "value": annotations_data[table]}
        #if no data in database: insert the comments
        if verbose: logging.info(" B5 - insert into table annotations: "+str(to_insert_dic))
        if len(annotations_data_from_db) == 0:
            insert_data("annotations", to_insert_dic, ",", "", "", studyDAO)
        #if data: compare and update
        else:
            annotations_flag, index = compare_and_overwrite_data(table_to_compare, {"value":annotations_data[table]}, annotations_data_from_db[0], novel_database_indexes, cv_id, studyDAO)
        overwrite_flag+=1
    if verbose: logging.info("  - "+str(overwrite_flag)+" overwrite(s) took place")
    return overwrite_flag

def parse_spreadsheet(spread_path, studyDAO):
    '''
    generic function to open the spreadsheet and reformat the data
    : input spread_path (str) absolute path to the spreadsheet
    : input studyDAO (connection object) object to connect to the database
    : return spread_dic (dic) nested dictionary  with individual_names as key and values in the format [{'table' :['field' : value from datasheet]}]
    : return annotations_dic (dic) nested dictionary with table as key and comments from spreadsheet as values
    : return spreadsheet (str) name of the spreadsheet parsed
    '''
    firstColumn = 0
    individual_name =""
    spread_dic={}
    new_proj={}
    annotations_dic={}
    spreadsheet = 'user spreadsheet'
    if verbose: logging.info("OPENING SPREADSHEET: "+spread_path)
    #define header for each table according to spreadsheet url (create new one if different spreadsheet provided)
    if '1978536442' in spread_path:
        spreadsheet = 'input_template'
        #cases where data are available online (note for Google spreadsheet or else, the data need to be published as csv first)
        try:
            connection_socket = urlopen(spread_path)
            tsv_doc = connection_socket.read()
            res= tsv_doc.decode("utf8")
            connection_socket.close()
        except:
            logging.info("Could not find the spreadsheet at the url indicated. Existing now")
            raise
        lines = res.split("\n")
        header_list=adjust_header_list(lines[0].rstrip())
        start_read = 2
    else:
        spreadsheet = 'input_user'
        file=open(spread_path, 'r', encoding='utf8', errors='replace')
        lines=[]
        for line in file:
            if line.startswith('option') or line.startswith('HEADER'):
                header_list=adjust_header_list(line.rstrip())
                if line.startswith('HEADER'):
                    firstColumn=1
            else:
                if "test_entry" not in line:
                    lines.append(line)
        start_read = 0
    #avoid the spreadsheet header
    for line in lines[start_read:]:
        line=line.rstrip()
        line_dic={}
        annotation_entry_dic={}
        #to avoid issue with apostrophe in field. Need to be updated with a better solution?
        line=line.replace("'","\"")
        if firstColumn==1:
            dataline=line.split("\t")[1:]
        else:
            dataline=line.split("\t")
        #parse the spreadsheet into table and field (after removing leading and trailing space(s))
        for index in range(0,len(dataline)):
            table = header_list[index].split("-")[0]
            field = header_list[index].split("-")[1]
            if table not in line_dic:
                line_dic[table] = {}
            line_dic[table][field] = dataline[index].strip()
        #individual is the main table and name is the identifier. So if no name is present: do not insert
        if 'individual' in line_dic and len(line_dic['individual']['name']) > 0:
            individual_name=line_dic['individual']['name']
            if 'date_collected' in line_dic['individual'] and len(str(line_dic['individual']['date_collected'])) > 0:
                line_dic['individual']['date_collected'] = format_date(str(line_dic['individual']['date_collected']))
            #populate material table
            if 'material' not in line_dic:
                line_dic['material'] ={}
            if 'name' not in line_dic['material'] or len(line_dic['material']['name'])==0:
                line_dic['material']['name'] = line_dic['individual']['name']
            if 'date_received' in line_dic['material'] and len(str(line_dic['material']['date_received'])) > 0:
                line_dic['material']['date_received'] = format_date(str(line_dic['material']['date_received']))
            if 'amount' in line_dic['material'] and len(str(line_dic['material']['amount'])) >0:
                line_dic['material']['amount']=transform_weight_unit({'weight': line_dic['material']['amount'], 'unit': line_dic['material']['unit']})
                line_dic['material']['unit']='g'
            line_dic['individual']['name'] = individual_name
            if verbose: logging.info("  - data for table individual: "+str(line_dic['individual']))
            if 'species' in line_dic:
                #if the taxon_id is not a number, then parse it as a common_name
                if 'taxon_id' in line_dic['species']:
                    try:
                        tax_id = int(line_dic['species']['taxon_id'])
                    except ValueError:
                        line_dic['species']['common_name'] = line_dic['species']['taxon_id']
                        del line_dic['species']['taxon_id']
                #deal with case where name is too short (? or other)
                if 'name' in line_dic['species'] and (len(line_dic['species']['name']) > 0 and  len(line_dic['species']['name'])< 4):
                    del line_dic['species']
                else:
                    line_dic['species'] = update_Species_table(line_dic['species'])
                if verbose: logging.info("  - data for table species: "+str(line_dic['species']))
            if 'file' in line_dic:
                if line_dic['file']['type']:
                    line_dic['file']['type']='PE'
            if 'location' in line_dic and (len(line_dic['location']['latitude']) > 0 and len(line_dic['location']['longitude']) >0):
                if verbose: logging.info("  => data for table location")
                line_dic['location'] = format_to_compare(line_dic['location'])
                if verbose: logging.info("  - data for table location: "+str(line_dic['location']))
            #added this section to cope with the absence of project accession (required for website)
            if 'project' in line_dic and len(line_dic['project']['name']) > 0 and len(line_dic['project']['accession'])==0:
                if line_dic['project']['name'] in new_proj:
                    project_acc=new_proj[line_dic['project']['name']]
                else:
                    proj_acc = studyDAO.getTableData("project", "accession", "name ='"+line_dic['project']['name'] +"';" )
                    if len(proj_acc) >0:
                        project_acc=proj_acc[0]['accession']
                    else:
                        project_acc=""
                if len(project_acc)==0:
                    all_acc = studyDAO.getTableData("project", "accession", "accession like 'NYSUB%';" )
                    if len(all_acc) >0:
                        max_db_acc=max([x['accession'] for x in all_acc])
                    else:
                        max_db_acc=""
                    max_new_acc=[max(x) for x in new_proj.values()]
                    if len(max_db_acc) >0 or len(max_new_acc)>0:
                        max_acc=[max(max_db_acc[0]['accession'], max_new_acc[0])][0][6:]
                    else:
                        max_acc="0"
                    l=len(str(int(max_acc)+1))
                    max_all_acc="0"*(4-l)+str(int(max_acc)+1)
                    #prefix is NYSUB for Not Yet SUBmitted
                    line_dic['project']['accession']="NYSUB"+max_all_acc
                    new_proj[line_dic['project']['name']]=line_dic['project']['accession']
                else:
                    line_dic['project']['accession']=project_acc
                    new_proj[line_dic['project']['name']]=project_acc
                if verbose: logging.info("  - data for table project: "+str(line_dic['project']))
            #ensure that the weight, if provided, is in g unit:
            if 'individual_data' in line_dic:
                if 'weight' in line_dic['individual_data']:
                    line_dic['individual_data']['weight']=transform_weight_unit({'weight': line_dic['individual_data']['weight'], 'unit': line_dic['individual_data']['unit']})
                    line_dic['individual_data']['unit']='g'
                    if verbose: logging.info("  - data for individual weight: "+str(line_dic['individual_data']))
            #full copy of dictionary to be able to keep the newly created dictionary while modifying one copy
            Line_dic=copy.deepcopy(line_dic)
            #go through the content of the dictionary
            for table in line_dic:
                #go through the field for the considered table. Only keep field if there is value associated
                for field in line_dic[table]:
                    if not isinstance(line_dic[table][field], datetime.date) and not isinstance(line_dic[table][field], float):
                        if len(line_dic[table][field]) == 0:
                            del Line_dic[table][field]
                    #deal with comment separately
                    if field == 'comment' and len(line_dic[table][field]) > 0 and table != 'image':
                        annotation_entry_dic[table]=line_dic[table][field]
                        del Line_dic[table][field]
                        if verbose: logging.info("  - comments for annotations table: "+str(annotation_entry_dic[table]))
                #remove table from dic if all is fields have no data associated
                if len(Line_dic[table]) == 0:
                    del Line_dic[table]
            #ensure data are processed if same individual listed twice in spreadsheet
            if individual_name in spread_dic:
                spread_dic[individual_name].append(Line_dic)
                annotations_dic[individual_name].append(annotation_entry_dic)
            else:
                spread_dic[individual_name] =[Line_dic]
                annotations_dic[individual_name]=[annotation_entry_dic]
    if verbose: logging.info("READ DATA FOR "+str(spread_dic.keys()))
    return spread_dic, annotations_dic, spreadsheet

def prepare_update(update_dic):
    '''
    Function to reformat a dictionary into update statement
    : input update_dic (dic) dictionary of fields to update
    : return final_statement (str) <field> = <value>, <field> = <value>.
    '''
    final_statement = ''
    if verbose: logging.info("      - preparing update for data: "+str(update_dic))
    for field in update_dic:
        final_statement += field+" = '" + str(update_dic[field]) +"', "
    return final_statement[:-2]

def transform_weight_unit(data_dic):
    #the dictionary has two keys: unit and weight / value depending of data_dic
    dic_keys=list(data_dic.keys())
    #after removing the unit key, dic_keys will be weight / value depending of data_dic
    dic_keys.remove('unit')
    #if data_dic['unit'].lower() =='g':
    new_weight=data_dic[dic_keys[0]]
    if data_dic['unit'].lower() =='mg':
        new_weight=float(data_dic[dic_keys[0]])/1000
    elif data_dic['unit'].lower() =='ug' or data_dic['unit'].lower() =='µg':
        new_weight=float(data_dic[dic_keys[0]])/1000000
    elif data_dic['unit'].lower() =='kg':
        new_weight=float(data_dic[dic_keys[0]])*1000
    if verbose and new_weight!=data_dic[dic_keys[0]]: logging.info("      - weight changed from "+str(data_dic[dic_keys[0]])+" "+data_dic['unit'].lower()+" to "+str(new_weight)+"g")
    return new_weight

def update_entry(new_data, annotations_data, studyDAO):
    '''manage the fate of the data if 'update' flag has been provided'''
    independent_table =['species', 'provider', 'location',  'project', 'library_type', 'seq_centre', 'seq_tech']
    dependent_table =  ['developmental_stage', 'organism_part', 'individual', 'image', 'material', 'sample', 'lane', 'library', 'individual_data', 'file']
    dependent_dic={'developmental_stage':['ontology'], 'organism_part':['ontology'], 'individual' : ['species', 'location', 'provider'],
     'material' : ['individual', 'provider', 'developmental_stage', 'organism_part'], 'sample':['material'], 'library' : ['library_type'], 'lane' : ['seq_tech', 'sample','library', 'seq_centre'],
     'file':['lane'], 'image':['individual'], 'individual_data': ['individual', 'cv']}
    update_flag=0
    novel_database_indexes={}
    #check if entry is in the database
    database_data = get_database_data(new_data, studyDAO, 'U')
    if 'individual' in database_data:
        new_data['individual']['name']=database_data['individual']['name']
    if verbose: logging.info("A-UPDATE ENTRY")
    for table in independent_table + dependent_table:
        if table in new_data:
            #if not already present : insert has then to take place
            if table not in database_data:
                if table != 'individual_data':
                    if verbose: logging.info(" A1- Insert into table "+table)
                    #get the index of last entry in table
                    previous_index=studyDAO.getmaxIndex(table)[0]['max('+table+'_id)']
                    insert_data(table, new_data[table], previous_index, ","+table+"_id", ","+ str(previous_index+1), studyDAO)
                    update_flag+=1
                    #update database_data with new index for entry
                    novel_database_indexes[table]=previous_index+1
                else:
                    if 'weight' in new_data[table]:
                        if verbose: logging.info(" A2- Insert into table "+table+": weight")
                        cv_id=studyDAO.getIndex('cv', 'attribute', "'weight'")[0]['cv_id']
                        value=new_data[table]['weight']
                        unit=new_data[table]['unit']
                        individual_data_from_db=studyDAO.getStudyData("individual_data", "individual_id", str(novel_database_indexes["individual"]) +"' and cv_id ='"+str(cv_id))
                        #no data in the database so insert into the database
                        if len(individual_data_from_db)==0:
                            if len(str(value))>0:
                                insert_data("individual_data", {"individual_id":str(novel_database_indexes["individual"]), "cv_id":str(cv_id), "value":str(value), "unit":unit}, ",", "", "", studyDAO)
                        else:
                            #get current data in the database
                            data_to_update=studyDAO.getTableData("individual_data", 'value, unit', "individual_id = '"+str(novel_database_indexes["individual"]) +"' and cv_id ='"+str(cv_id)+"'")
                            #ensure that we are comparing the weight value properly
                            if float(new_data[table]['weight']) != float(data_to_update[0]['value']):
                                update_individual_data(individual_data_from_db, new_data[table]['weight'], studyDAO)
            else:
                #compare the database data and new data
                updated_flag, novel_database_indexes[table] = compare_and_update_data(table, new_data[table], database_data[table], studyDAO)
    update_flag+=updated_flag
    #deal with comments fields for the annotations table
    for table in annotations_data:
        #get the cv_ids for the tables with comment
        cv_id=studyDAO.getIndex('cv', 'comment', "'entry for table "+table+"'")[0]['cv_id']
        if table != 'individual':
            annotations_data_from_db=studyDAO.getStudyData("annotations", "table_name = '" +table+"' and cv_id = "+str(cv_id)+" and table_id", novel_database_indexes[table])
            #if no data in database: insert the comments
            if len(annotations_data_from_db) ==0:
                to_insert_dic={"table_name":table, "table_id": novel_database_indexes[table], "cv_id":cv_id, "value": annotations_data[table]}
                insert_data(table, to_insert_dic, ",","","", studyDAO)
            #if data: compare and update
            else:
                annotations_flag, index = compare_and_update_data("table", {"value":annotations_data[table]}, annotations_data_from_db[0], studyDAO)
        else:
            annotations_data_from_db=studyDAO.getStudyData("individual_data", "cv_id = "+str(cv_id)+ " and individual_id ", novel_database_indexes[table])
            update_individual_data(annotations_data_from_db, annotations_data[table], studyDAO)
        update_flag+=1
    if verbose: logging.info(" "+str(update_flag)+" update(s) took place")
    return update_flag

def update_individual_data(db_data, new_data, studyDAO):
    '''function to update data for the individual_table'''
    if verbose: logging.info("    - updating data into database for table individual_data")
    data_updated=list(db_data)
    entry_to_update=data_updated[0]['row_id']
    del data_updated[0]['row_id']
    del data_updated[0]['changed']
    del data_updated[0]['latest']
    data_updated[0]['unit']='g'
    data_updated[0]['value']=new_data
    if verbose: logging.info("      - inserting new data: "+str(data_updated[0]))
    insert_data("individual_data", data_updated[0], ",", "","", studyDAO)
    if verbose: logging.info("      - updating previous data: "+str(list(db_data)))
    try:
        studyDAO.update("individual_data", "changed ='"+today+"', latest =0", "row_id", entry_to_update)
    except:
        logging.info("Could not update the comment in the individual data table from the database. Existing now")
        sys.exit()

def update_Species_table(dic):
    '''Function to populate the species information with data from reference sources'''
    '''
    : input dic (dic) dictionary of ['field' : value] for species table of the spreadsheet
    : return: see the individual function descriptions
    '''
    '''
    #function using ENA taxonomy server:
    #  return dic (dic) updated dictionary with the taxonomy completed i.e. taxon_id or strain if strain or taxon_id are provided, respectively, plus the taxon position.
    if dic['strain'] != "":
        ENA_taxo = TaxUtils.getTaxid(dic['strain'])
        if ENA_taxo:
            dic['taxon_id'] = ENA_taxo[1]
            dic['taxon_position'] = ENA_taxo[0]
    elif 'taxon_id' in dic and dic['taxon_id'] != "":
        ENA_taxo = TaxUtils.getTaxid(dic['taxon_id'])
        if ENA_taxo:
            dic['strain'] = ENA_taxo[2].replace("'","\"")
            dic['taxon_position'] = ENA_taxo[0]
    '''
    old_dic=copy.deepcopy(dic)
    # fonction using NCBI taxonomy server
    #   return dic (dic) updated dictionary with the taxonomy completed i.e. taxon_id or name if strain or taxon_id are provided, respectively, plus the taxon position and common name.
    if 'name' in dic and dic['name'] != "":
        dic['name']=re.sub("\s\s+" , " ", dic['name'].replace("sp.","").replace("c.f.","").replace("cf.",""))
        if len(dic['name']) > 2:
            result_list = TaxUtils.getTaxid(dic['name'])
            if len(result_list) > 0:
                if result_list[1] != "":
                    dic['common_name']=result_list[1].replace("'","\"")
                if result_list[2] !=  "":
                    dic['taxon_position']=result_list[2]
                if result_list[3] != 0 and 'taxon_id' not in dic:
                    dic['taxon_id']=result_list[3]
        else:
            del dic['name']
    elif 'taxon_id' in dic and dic['taxon_id'] != "":
        result_list = TaxUtils.getTaxid(dic['taxon_id'])
        if len(result_list) > 0:
            if result_list[1] != "":
                dic['common_name']=result_list[1].replace("'","\"")
            if result_list[2] !=  "":
                dic['taxon_position']=result_list[2]
            if result_list[0] != "":
                dic['name']=result_list[0].replace("'","\"")
    if verbose: logging.info("      - update species name from "+str(old_dic)+" to  "+str(dic))
    return dic

def main(programSetup):
    configSettings = programSetup.config
    config_file_path = programSetup.config_file_path
    #Not in use but could be used to set up working directory: root_result_dir_path = programSetup.root_result_dir_path
    # Create data access object
    mydbconn = pymysql.connect(user=configSettings["vpUser"], password=configSettings["vpPassword"],
                               host=configSettings["vpHost"], port=int(configSettings["vpPort"]),
                               db=configSettings["vpInstance"], autocommit=False,
                               cursorclass=pymysql.cursors.DictCursor)
    dataAccessObjectForVP = MySQLDataAccessObject(mydbconn)
    studyDAO = StudyDAO.StudyDAO(dataAccessObjectForVP)
    raw_sp_results ={}
    insert_flag =[]
    #call for spreadsheet parsing and inserting/updating
    if spath:
        #open spreadsheet url
        if verbose: logging.info("Opening the spreadsheet")
        raw_sp_results, for_annotations, spreadsheet_name = parse_spreadsheet(spath, studyDAO)
        dispatch_data(raw_sp_results, for_annotations, spreadsheet_name, studyDAO, mydbconn)
    if verbose: logging.info("End of run")

if __name__ == '__main__':
    # Parse script parameters and run program setup
    parser = argparse.ArgumentParser(
        description="Cichlid_db_update import input spreadsheet onto the Cichlid_TRACKING database")
    parser.add_argument("-sp", "--spreadsheet", help="spreadsheet path or input")
    parser.add_argument("-v", "--verbose", help="verbose mode", action = 'store_true')
    parser.add_argument("-c", "--config",
                        type=argparse.FileType('r'),
                        help="path to config file",
                        required=False,
                        metavar="configfile",
                        default="Cichlid_Population_db.json")

    args = vars(parser.parse_args())
    #get today's date as DD-MM-YY
    today=time.strftime("%Y-%m-%d")
    #Load configuration settings for instance database connection details
    config_file_path = args['config']
    configSettings = json.load(config_file_path)
    scriptDir = configSettings["scriptDirectory"]
    root_result_dir_path = configSettings["resultFileDir"]
    SubmissionCheckerSetup = collections.namedtuple('SubmissionCheckerSetup',
                                                    'config, , root_result_dir_path, config_file_path')
    programSetup = SubmissionCheckerSetup(config=configSettings, root_result_dir_path=root_result_dir_path,
                                          config_file_path=config_file_path.name)
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s - %(message)s',
                        datefmt='%Y/%m/%d %H:%M:%S', filename='import_'+today+'.log')
    #deal with other arguments
    end_url =""
    spath =""
    url ='https://docs.google.com/spreadsheets/d/e/2PACX-1vSoiTDfPrIDbMNMqt_0QYY-rLXXofBRaP38nuwcNbvTpwniaYadKLIiRuCwISPT60F4rWSNoY6pO82R/'
    if 'spreadsheet' in args:
        if args['spreadsheet'] == 'input':
            end_url = 'pub?gid=1978536442'
            spath = url+end_url
        else:
            spath = args['spreadsheet']
    verbose=False
    if args['verbose']: verbose= True
    flag=False
    main(programSetup)
