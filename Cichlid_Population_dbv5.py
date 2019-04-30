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


__author__ = 'Hubert Denise, Mar. 2019'

'''Cichlid_Population_db.py automates entry of data from json files and spreadsheets into the Cichlid db. Type Population_db.py -h for help.
    Usage: Population_db.py (-o) (-v) (-c) (-j <path to json file> or -sp <'samples', 'sequenced',  'mlw' or 'images')
    The script will import data from json and spreadsheet. See spreadsheets for format.
    They will be parsed as a dictionary with 'individual_name' as keys and table dictionaries as values.
    {'individual_name':{'table1':{'field1':value, 'field2': value...}, 'table2':{'fielda':value, 'fieldb': value...}, 'individual_name2': {...} }}
    Order of import:    insert data onto ontology_table (see Hubert Notes/Populating Cichlid trackingV3 database),
                        ./Cichlid_Population_db.py -j /Users/hd/Documents/Cichlid_database/data/cichlids_iRods.json -v -o,
                        ./Cichlid_Population_db.py -sp samples -v -o,
                        ./Cichlid_Population_db.py -sp sequenced -v -o,
                        ./Cichlid_Population_db.py -sp mlw -v -o,
                        ./Cichlid_Population_db.py -sp images -v -o,
'''

req_version = (3, 4)
cur_version = sys.version_info

# check python version
if cur_version < req_version:
    print("Your Python interpreter is too old. You need version %d.%d.x" % req_version)
    sys.exit()

'''
usage: Cichlid_Population_db.py automates entry of data from json files and spreadsheets into the Cichlid db. Type Population_db.py -h for help.
    Usage: Population_db.py (-o) (-v) (-c) (-j <path to json file> or -sp <'samples', 'sequenced',  'mlw' or 'images')
'''
def format_display(date_element):
    '''
    fonction for double-digit display when day/month is less than 10. Allow date field to be consistent and compatible with mysql date format (YYYY-MM-DD)
    : input date_element (str) string containing a number indicating the day or month
    : return date_element (str) string containing a double-digit number indicating the day or month
    '''
    if len(date_element)==1:
        date_element="0"+date_element
    return date_element

def prepare_update(update_dic):
    '''
    : input update_dic (dic) dictionary of fields to update
    : return final_statement (str) <field> = <value>, <field> = <value> where <table>_id = <identifier>. No return if there is no field to update
    '''
    #if update take place then update the change field and latest field
    final_statement = ''
    for field in update_dic:
        final_statement += field+" = '" + str(update_dic[field]) +"', "
    return final_statement[:-2]

def dic_to_str(dic, field_attribute, value_attribute):
    '''
    function to transform a dictionary into strings for fields and values to populate the database
    : input dic (dic) dicionary to insert into db
    : input field_attribute (str) string of fields to add to the dictionary ones
    : input value_attribute (str) string of corresponding values to add to the dictionary ones
    : return field_str (str) field list separated by comma
    : return val_str (str) corresponding value list separated by comma and apostrophes if necessary
    '''
    new_list=[]
    field_list = list(dic.keys())
    val_list = list(dic.values())
    #rewrite  the values in mysql compatible format
    for element in val_list:
        if type(element) == str or type(element) == datetime.date:
             element ="'"+str(element)+"'"
        new_list.append(element)
    field_str = ", ".join(field_list) + field_attribute
    val_str =  ", ".join(str(x) for x in new_list) + str(value_attribute)
    val_str = val_str.replace('None', 'Null')
    return field_str, val_str

def updateSpecies_table(dic):
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
    return dic

def update(update_dic, today, studyDAO, flag, verbose, table, identifier, identifiant, db_dic):
    '''
    function to perform the update(s) of the tables present in the json file.
    : input update_dic (dic) for a table with field(s) to update as keys and attribute as values
    : input today_date (str) used to update the 'changed' field if data need changing in a table
    : input studyDAO (connection object) object to connect to the database
    : input flag (boolean) when set to True, data in db will be overwritten otherwise data will not be replaced
    : input verbose (boolean) when set to True, display messages to user about run progress
    : input table (str) table name
    : input identifier (str) field serving as identifier for the table
    : input identifiant (str) value for the identifier
    : input db_dic (dic) dictionary of data already present in the db for the table
    : return none
    '''
    to_update ={}
    if verbose:
        logging.info("Some data already present in the table "+table+" are different from the provided ones.")
    if flag:
        #overwrite option
        if verbose:
            logging.info(" one update for table "+ table+" could take place but no update flag. Replacing instead")
            logging.info(update_dic)
            logging.info(db_dic)
        update_statement = prepare_update(update_dic)
        studyDAO.update(table, update_statement, identifier, identifiant)
    else:
        #create a new record and update the current one [changed from latest =1 to 0]
        if verbose:
            logging.info("Updating table " + table+" now")
        #create a new record with current date and updated data if applies
        if table not in ('library_type','project', 'location'):
            #make copy and update the dictionary for the table
            db_dic_copy=db_dic.copy()
            db_dic_copy.update(update_dic)
            #insert data into a new row
            del db_dic_copy['row_id']
            record_update_dic = {k:v for k,v in db_dic_copy.items() if v is not None and k != 'row_id'}
            #update the previous record
            to_update['changed'] = today
            to_update['latest'] = 0
            previous_record_statement = prepare_update(to_update)
            if verbose:
                logging.info("Updating previous record now: latest = 0 and changed to today's date")
            studyDAO.update(table, previous_record_statement, identifier, identifiant)
            if verbose:
                logging.info("Creating new latest record now")
            del record_update_dic['latest']
            field_str, value_str = dic_to_str(record_update_dic, ', latest', "," + str(True))
            studyDAO.populate_table(table, "("+field_str+")", "(" + value_str+ ")")
        else:
            if verbose:
                logging.info("Overwriting record now")
            update_statement = prepare_update(update_dic)
            studyDAO.update(table, update_statement, identifier, identifiant)

def check_for_update(raw_dic, table, studyDAO, verbose):
    '''
    function to decide if insert or update has to be performed from the data present in the iRods json file
    : input raw_dic (dic) for a table
    : input table (str) table name
    : input studyDAO (connection object) object to connect to the database
    : input verbose (boolean) when set to True, display messages to user about run progress
    : return return_flag (str) that take the values "I" for insertion to take place and "U" for update having taking place
    '''
    update_table =[]
    #define the identifier field for each table
    identifier_dic = {'individual' : 'name', 'species' : 'name', 'material' : 'name', 'organism_part' : 'name', 'provider' : 'provider_name', 'location' : 'source_location',
    'file' : 'name', 'project': 'ssid', 'sample' : 'ssid', 'lane': 'accession', 'library' : 'ssid', 'library_type' : 'name', 'seq_tech': 'name', 'developmental_stage': 'name',
    'image' : 'filename'}
    return_flag = ""
    toupdate=[]
    toinsert = []
    table_id = 0
    #check if there is anexisting record in the database
    if identifier_dic[table] in raw_dic:
        db_results=studyDAO.getStudyData(table, identifier_dic[table], raw_dic[identifier_dic[table]])
        #ensure that only field with data are compared
        if len(db_results) > 0:
            table_id =db_results[0][table+"_id"]
            db_dic = {k:v for k,v in db_results[-1].items() if v != None}
            #extract the fields to update
            different_items = {k:v for k,v in raw_dic.items() if k in db_results[0] and str(db_results[0][k]) != str(raw_dic[k])}
            #update table if there is fields to update
            if len(different_items) > 0 :
                if verbose:
                    logging.info("There is fields to update in table "+table)
                different_items[table+"_id"]=table_id
                update(different_items, today, studyDAO, flag, verbose, table, identifier_dic[table], raw_dic[identifier_dic[table]], db_results[0])
                return_flag ="U"
        else:
            #creating new record as not already present in the database
            if verbose:
                logging.info("There is no previous record with the identifier "+identifier_dic[table]+" for the table "+table+". Creating them now")
            toinsert.append({table:raw_dic})
    if len(toinsert) > 0:
        return "I",""
    else:
        return return_flag, table_id

def populate_table(dic, today, studyDAO, flag, verbose, table, table_identifier,  foreign_identifier = "", foreign_id = "", foreign2_identifier = "", foreign2_id = ""):
    '''
    generic function to populate the different tables of the db
    : input dic (dic) dictionary of format 'table'['field' : value] for each sample of the spreadsheet with date reformatted
    : input today_date (str) used to update the 'changed' field if data need changing in a table
    : input studyDAO (connection object) object to connect to the database
    : input flag (boolean) when set to True, data in db will be overwritten otherwise a new record with new data will be created
    : input verbose (boolean) when set to True, display messages to user about run progress
    : input table (str) table name
    : input table_identifier (str) field serving as identifier for the table ('name', <table>'_id' or 'name')
    : input foreign_identifier (str) name of the foreign_key to populate the table. Default is "".
    : input foreign_id (str) value of the foreign_identifier. Default is "".
    : return maxid[0][table+'_id'] (int) value of the <table>_id corresponding to the table_identifier
    : return dbflag (charstr) representing the operation that took place: I insert, U update or N not updated
    '''
    #if foreign key exists, insert them
    if len(str(foreign_identifier)) > 0:
        dic[foreign_identifier]=foreign_id
    if len(str(foreign2_identifier)) > 0:
        dic[foreign2_identifier]=foreign2_id
    dbflag = ""
    final_maxid = []
    max_id = 0
    #check if identifier is already in db
    if len(dic) > 0:
        db_results=studyDAO.getStudyData(table, table_identifier, dic[table_identifier])
        #if not already present: insert into db
        if len(db_results) == 0:
            if verbose:
                logging.info("The data are not already present in the table "+table+". Inserting now")
            field_str = ""
            val_str = ""
            #get the latest <table>_id if there is data in db
            if table not in ('annotations', 'individual_data', 'assembly'):
                maxid = studyDAO.getmaxIndex(table)
            #insert into table without chnged and latest fields
            if table not in ('project', 'annotations', 'individual_data', 'assembly'):
                if maxid[0]['max('+table+'_id)'] != None:
                    max_id = int(maxid[0]['max('+table+'_id)'])
                else:
                    max_id = 0
                field_str =  "," + table + "_id"
                val_str = "," + str(max_id +1)
            #only keep valid value(s) to insert
            filtered_dic = {k:v for (k,v) in dic.items() if len(str(v)) > 0}
            if len(filtered_dic) > 0:
                if verbose:
                    logging.info("Preparing insert statement")
                field, val = dic_to_str(filtered_dic, field_str, val_str)
                final_field_str = "(" + field + ")"
                final_val_str = "("+ val +")"
                #insert into table without chnged and latest fields
                if table not in ('library_type', 'project', 'cv', 'location', 'seq_tech', 'organism_part', 'developmental_stage'):
                    final_field_str = "(" + field + ", changed, latest )"
                    final_val_str = "("+ val +", '"+today+"', True )"
                if verbose:
                    logging.info("Populating table "+table+" now")
                studyDAO.populate_table(table, final_field_str, final_val_str)
                dbflag = "I"
        else:
            #if record exists, get the identifier
            if verbose:
                logging.info("The record for table "+table+" already exists. Get identifier now")
            if table == 'location':
                final_maxid =[{'location_id': db_results[0][table+"_id"]}]
        if table not in ['location', 'individual_data']:
            #get the corresponding <table>_id
            final_maxid = studyDAO.getIndex(table, table_identifier, dic[table_identifier])
        #if no previous index, then return 1. To deal with first insertion
        if len(final_maxid) == 0:
            MaxID = max_id +1
        else: MaxID = final_maxid[0][table+'_id']
        #return value and action flag
        return MaxID, dbflag

def format_date(nested_dic):
    '''
    function to re-format the date fields from the data fromm the spreadsheet in mysql compatible format (YYYY-MM-DD)
    : input nested_dic (dic) dictionary with sample_id as key and lidt of nested dictionary [{'table' :['field' : value from datasheet]}] as value
    : return nested_dic (dic) updated nested_dic with date formatted as YYYY-MM-DD (only numeric and separated by '-')
    '''
    date_dic={'jan' : '01','feb' : '02','mar' : '03','apr' : '04','may' : '05','jun' : '06','jul' : '07','aug' : '08','sep' : '09','oct' : '10',
    'nov' : '11','dec' : '12','january' : '01','february' : '02','march' : '03','april' : '04','may' : '05','june' : '06','july' : '07','august' : '08',
    'september' : '09','october' : '10','november' : '11','december' : '12'}
    pattern_date=re.compile(r'^[0-3][0-9]\.[0-1][1-9]\.1[0-9]')
    pattern_date2 =re.compile(r'^2[0-1][0-9]{2}-[0-1][1-9]-[0-3][0-9]')
    #extract dictionary for all the field corresponding to a date for each sample (date_field : date provided)
    for individual in nested_dic:
        print (" --------- "+individual)
        for nested_indiv_dic in nested_dic[individual]:
            print(nested_indiv_dic)
            if 'individual' in nested_indiv_dic and 'date_collected' in nested_indiv_dic['individual']:
                date_field = nested_indiv_dic['individual']['date_collected']
                #generally the first part of a date field is the date itself while the second part, if present, is the time so we'll keep the first part
                if len(date_field) > 0:
                    datefield = date_field.lower().split()[0]
                    #ensure there is only numeric month value by replacing month name if present by month number
                    for month in date_dic:
                        datefield = datefield.replace(month, date_dic[month])
                    #note that the last part it always the year so need to compare the 2 first section to deduce which is day/month
                    #in the spreadsheet: when sep by "/" : MM-DD-YY (sp spreadsheet) or (sa spreadsheet) while if sep by ".": DD-MM-YY
                    date_part=re.split(r"[^0-9]", datefield)
                    print(datefield)
                    if "." in datefield:
                        if pattern_date.match(datefield):
                            date_part="20"+date_part[2]+"-"+format_display(date_part[1])+"-"+format_display(date_part[0])
                    elif "/" in datefield:
                        if len(date_part[2])==4:
                            date_part=date_part[2] +"-" + format_display(date_part[1])+"-"+format_display(date_part[0])
                        else:
                            date_part="20"+date_part[2] +"-" + format_display(date_part[0])+"-"+format_display(date_part[1])
                    elif pattern_date2.match(datefield):
                            date_part = datefield
                    #update the input nested_dic with reformatted date
                    print (date_part)
                    nested_indiv_dic['individual']['date_collected']=datetime.datetime.strptime(date_part, "%Y-%m-%d").date()
    return nested_dic

def extract_individual_name(original_name, studyDAO):
    '''
    function to parse the individual_name from json/spreadsheet and extract organism_part name if present(*)
    : input original_name (str) individual_name as given in the spreadsheet or json files
    : input studyDAO (connection object) object to connect to the database
    : return name <original_name || new_name> (str) individual_name or new individual_name that does not contain the organism_part name
    : return name <organism_part_name || ''> (str) organism_part_name, if present, with an ontology_id in the ontology table
    (*: not needed with the current data but could be useful in the future. Used for VGP)
    '''
    #extract the organism_part_name already present in ontology table
    part_name_list  = studyDAO.getTableData('ontology', 'name', 'name is not Null')
    part_list = [v['name'].lower() for v in part_name_list]
    #remove space in original_name
    original_name = original_name.replace(" ","")
    #deal with particular case
    if 'gDNA' in original_name:
        organism_part_name = 'DNA'
        new_name= original_name[:-4]
    #most of organism_part_name are separated by a dot from the individual_name
    if "." in original_name:
        new_name = original_name.split(".")[0]
        organism_part_name = original_name.split(".")[1]
        #deal with particular case
        if organism_part_name[-1] == "?":
            organism_part_name= organism_part_name[:-1]
        #extract number part
        numerics = [s for s in organism_part_name if s.isdigit()]
        num = "".join(numerics)
        if len(num) >0 :
            #extract string part
            organism_part_name = organism_part_name[:-len(numerics)]
        #deal with particular cases
        if organism_part_name.lower() == 'wholeblood':
            organism_part_name = 'blood'
        if organism_part_name.lower() in ['middle', 'rest']:
            organism_part_name = 'body'
        if organism_part_name.lower() == 'plugs':
            organism_part_name = 'dna'
        if organism_part_name.lower() == 'ova':
            organism_part_name = 'ovary'
        if organism_part_name.lower() == 'fry':
            original_name = new_name
    #check if the organism_part_name has an ontology. Otherwise, ignore
    if organism_part_name.lower() in part_list or organism_part_name.lower()+"s" in part_list:
        return new_name, organism_part_name.lower()
    else:
        return original_name, ""

def parse_spreadsheet(spread_path, studyDAO):
    '''
    generic function to open the spreadsheet and reformat the data
    : input spread_path (str) absolute path to the spreadsheet
    : input studyDAO (connection object) object to connect to the database
    : return spread_dic (dic) nested dictionary  with individual_names as key and values in the format [{'table' :['field' : value from datasheet]}]
    : return spreadsheet (str) name of the spreadsheet parsed
    '''
    countIndividual = 0
    individual_name =""
    spreadsheet = 'user spreadsheet'
    start_read = 1
    #define header for each table according to spreadsheet url (create new one if different spreadsheet provided)
    if '911220955' in spread_path:
        eq_list=['sample-name', 'individual-name', 'individual-alias', 'species-name', 'location-geographical_region', 'location-source_location', 'individual-date_collected', 'species-common_name', '', '', '','', 'location-latitude','',
        'location-longitude', '', '', 'individual_data-value', 'individual_data-value3', '', '','individual-collection_details', 'individual-collection_method', 'project-alias2', 'individual-sex', 'individual-alias2', '', '', '','individual_data-value4' ]
        spreadsheet = 'Sequenced_master'
    if '386735776' in spread_path:
        eq_list=['individual-name', '','','','individual_data-value','individual-sex', 'location-geographical_region', 'location-source_location', 'individual-collection_method', 'individual-collection_details', 'individual-date_collected', '',
        '','', 'individual_data-value4', 'species-common_name', 'location-latitude', '','location-longitude', '', 'species-name', 'individual_data-value3', '', 'project-alias2', 'individual-alias']
        spreadsheet = 'Sample_master'
    if 'mlw' in spread_path:
        eq_list=['individual-name', 'sample-name', 'project-ssid', 'project-alias', 'project-name', 'project-accession', 'sample-name', 'material-name',	'sample-name',	'sample-name', 'individual-sex', 'species-taxon_id', 'individual-geographical_region']
        spreadsheet = 'mlw_db'
    if 'tsv' in spread_path:
        eq_list=['individual-name', 'image-name']
        spreadsheet = 'Cichlid.org'
    #note: data_value is the field called 'morph' in spreadsheets, data_value3: note, data_value4 is weight, individual-alias2 ius the name given to the individual by the supplier like 'G10J04' and project-alias2 is the study short-name like 'milan_201706'
    #cases where the files are accessed on disc/file_system
    if 'mlw' in spread_path or 'tsv' in spread_path:
        res=""
        for tab_line in open(spread_path, "r"):
            res+=tab_line
    else:
        #cases where data are available online (note for Google spreadsheet or else, the data need to be published as csv first)
        try:
            connection_socket = urlopen(spread_path+'&output=tsv')
            tsv_doc = connection_socket.read()
            res= tsv_doc.decode("utf8")
            connection_socket.close()
        except:
            logging.info("Could not find the spreadsheet at the url indicated. Existing now")
            raise
    spread_dic={}
    lines = res.split("\n")
    #avoid the first line (spreadsheet header)
    for line in lines[start_read:]:
        line=line.rstrip()
        line_dic={}
        #to avoid issue with apostrophe in field. Need to be updated with a better solution?
        line=line.replace("'","\"")
        if 'tsv' in spread_path:
            dataline=line.split("\t")[:2]
        else:
            dataline=line.split("\t")
        #parse the spreadsheet into table and field
        for index in range(0,len(dataline)):
            if eq_list[index] != "" and dataline[index] != "":
                table = eq_list[index].split("-")[0]
                field = eq_list[index].split("-")[1]
                if table not in line_dic:
                    line_dic[table] = {}
                line_dic[table][field] = dataline[index].strip()
        #special cases for different tables (spreadsheets are not consistent)
        if 'species' in line_dic:
            #if the taxon_id is not a number, then parse it as a common_name
            if 'taxon_id' in line_dic['species']:
                try:
                    tax_id = int(line_dic['species']['taxon_id'])
                except ValueError:
                    line_dic['species']['common_name'] = line_dic['species']['taxon_id']
                    del line_dic['species']['taxon_id']
            #deal with case where name is too short (? or other)
            if 'name' in line_dic['species'] and len(line_dic['species']['name']) < 3:
                del line_dic['species']
            else:
                line_dic['species'] = updateSpecies_table(line_dic['species'])
        if 'location' in line_dic:
            if 'source_location' in line_dic['location'] and ";" in line_dic['location']['source_location']:
                location_list = line_dic['location']['source_location'].split(";")
                if 'previously labelled' in location_list[1]:
                    line_dic['location']['source_location']=location_list[0]
                    line_dic['individual_data'] ={}
                    line_dic['individual_data']['value'] = location_list[1]
        if 'image' in line_dic:
            if 'name' in line_dic['image']:
                image_data =line_dic['image']['name'].split("/")
                image_filepath = "/".join(image_data[:-1])
                image_filename= image_data[-1]
                line_dic['image']['filename'] = image_filename
                line_dic['image']['filepath'] = image_filepath
                line_dic['image']['comment'] = "from https://cambridgecichlids.org"
                provider_data = studyDAO.getTableData("provider", "provider_id", "provider_name = 'Hannes Svardal'")
                #print(provider_data[0])
                line_dic['image']['provider_id'] = provider_data[0]['provider_id']
                del line_dic['image']['name']
        #individual is the main table and name is the identifier. So if no name is present: create one
        if 'individual' in line_dic:
            if 'name' not in line_dic['individual']:
                countIndividual +=1
                individual_name= 'temp_name' +str(countIndividual)
            else:
                individual_name=line_dic['individual']['name']
                #parse spreadsheet individual_name if there is a "."
                if "." in individual_name:
                    individual_name, organism_part_name = extract_individual_name(individual_name, studyDAO)
                    if organism_part_name != "":
                        if 'organism_part' not in line_dic:
                            line_dic['organism_part'] ={}
                #try to parse the 'sex' field
                if 'sex' in line_dic['individual']:
                    if 'male' not in line_dic['individual']['sex'].lower() and line_dic['individual']['sex'].lower() not in ['m', 'f']:
                        if line_dic['individual']['sex'] in ['J']:
                            if not 'developmental_stage' in line_dic:
                                line_dic['developmental_stage'] ={}
                            line_dic['developmental_stage']['name'] = 'juvenile'
                        else:
                            if not 'individual_data' in line_dic:
                                line_dic['individual_data']={}
                            line_dic['individual_data']['value2'] = line_dic['individual']['sex']
                        del line_dic['individual']['sex']
                #populate material table
                if 'material' not in line_dic:
                    line_dic['material'] ={}
                if 'name' not in line_dic['material']:
                    line_dic['material']['name'] = line_dic['individual']['name']
                line_dic['individual']['name'] = individual_name
                #deal with more inconsistency
                if 'alias2' in line_dic['individual']:
                    if 'alias' not in line_dic['individual']:
                        line_dic['individual']['alias'] = line_dic['individual']['alias2']
                    else:
                        if line_dic['material']['name'] == individual_name:
                            line_dic['material']['name'] = line_dic['individual']['alias2']
                        else:
                            line_dic['individual_data']['value5'] = line_dic['individual']['alias2']
                    del line_dic['individual']['alias2']
                if 'project' in line_dic and 'alias2' in line_dic['project']:
                    if 'milan' in line_dic['project']['alias2']:
                        provider_data = studyDAO.getTableData("provider", "provider_id", "provider_name = 'Milan Malinsky'")
                        if len(provider_data) > 0:
                            line_dic['individual']['provider_id'] = provider_data[0]['provider_id']
                    del line_dic['project']['alias2']
        print(individual_name)
        if individual_name in spread_dic:
            spread_dic[individual_name].append(line_dic)
        else:
            spread_dic[individual_name] =[line_dic]
    print(spread_dic)
    ready_spread_dic = format_date(spread_dic)
    return ready_spread_dic, spreadsheet

def parse_json(json_path, studyDAO):
    '''
    function to open the json and reformat the data
    : input json_path (str) absolute path to the json file
    : return json_dic (dic) nested dictionary  with individual_names as keys and values in the format {'table' :['field' : value from datasheet]}
    '''
    json_dic = {}
    #define the equivalence between the json attributes and the database table fields
    file_eq= {'ebi_run_acc': 'accession', 'is_paired_read': 'type', 'total_reads': 'nber_reads' , 'md5' : 'md5' }
    project_eq = {'study': 'name', 'study_accession_number': 'accession', 'study_id' : 'ssid', 'study_title' : 'alias'}
    sample_eq = {'sample' : 'name', 'sample_accession_number' : 'accession', 'sample_id' : 'ssid', 'sample_donor_id' : 'name'}
    library_eq = {'library_id' : 'ssid'}
    library_type_eq = {'library_type' : 'name'}
    lane_eq = {'id_run' : 'accession', 'lane' : 'name'}
    species_eq = {'sample_common_name' : 'name'}
    individual_eq = {'sample_supplier_name' : 'name' , 'sample_public_name': 'alias'}
    dic_eq ={'file':file_eq, 'project':project_eq, 'sample': sample_eq, 'lane' : lane_eq, 'library' : library_eq, 'library_type' : library_type_eq, 'species' : species_eq, 'individual' : individual_eq}
    #get and parse the json file name
    json_name = json_path
    if "/" in json_path:
        json_name=json_path.split("/")[-1]
    try:
        #open the json file
        json_data = json.load(open(json_path, 'r'))
    except:
        logging.info("there is no json file at "+json_path+". Exiting now")
        sys.exit()
    for entry in json_data:
        data_dic = {}
        organism_part_name =""
        file_name = entry['data_object'].split(".")
        for dic in entry['avus']:
            #extract individual_name
            if dic['attribute'] =="sample_supplier_name":
                material_name = dic['value'].strip()
                if "." in dic['value'] or 'gDNA' in dic['value']:
                    individual_name, organism_part_name = extract_individual_name(dic['value'], studyDAO)
                else:
                    individual_name = dic['value'].strip()
                print (individual_name)
                if individual_name not in json_dic:
                    json_dic[individual_name] = []
            else:
                if dic['attribute'] == 'is_paired_read':
                    data_dic[dic['attribute']] = 'PE'
                else:
                    #ensure that there is no apostrophe in the values
                    data_dic[dic['attribute']] = dic['value'].strip().replace("'","\"")
        new_dic ={}
        #translate the json headers in database headers
        for table in dic_eq:
            new_dic[table] = {dic_eq[table][k]:int(v) for k,v in data_dic.items() if k in dic_eq[table] and dic_eq[table][k] in ['ssid','nber_reads']}
            new_dic[table].update({dic_eq[table][k]:v for k,v in data_dic.items() if k in dic_eq[table] and dic_eq[table][k] not in ['ssid','nber_reads']})
        #add additional_data, not stored in 'avus'
        new_dic['file']['name'] = ".".join(file_name[:-1])
        #file_format: bam or cram
        new_dic['file']['format'] = file_name[-1]
        #lane_accession: <run_id>/<lane>
        #the ''-(len(file_name)-1' is to ensure the lane accession is properly extracted (for pacbio json file)
        new_dic['lane']['accession'] = ".".join(file_name[:-(len(file_name)-1)])
        new_dic['individual']['name'] = individual_name
        #deal with inconsistency in some fields
        new_dic['material'] = {}
        new_dic['material']['name'] = material_name
        #ensure organism_part_name has an ontology_id
        if organism_part_name !="":
            new_dic['organism_part']['name'] = organism_part_name
            #get the ontology_id if it exists
            ontology_list = studyDAO.getTableData('ontology', 'ontology_id', "name like '"+organism_part_name+"%'")
            if len(ontology_list) > 0:
                new_dic['organism_part']['ontology_id'] = ontology_list[0]['ontology_id']
        new_dic['species'] = updateSpecies_table(new_dic['species'])
        json_dic[individual_name].append(new_dic)
    return json_dic, json_name

def populate_database(raw_results, entry_name, studyDAO, verbose, mydbconn):
    '''
    generic function to call for update/population of the database with data from json or spreadsheet
    : input raw_results (dic)
    : input entry_name (str) name of the json file or the Google spreadsheet tab
    : input studyDAO (connection object) object to connect to the database
    : input verbose (boolean) when set to True, display messages to user about run progress
    : input mydbconn (database connection_socket) connection to the Cichlid database
    : return none
    '''
    all_flag = []
    idflag =""
    if 'json' in entry_name:
        identifier_dic = {'individual' : 'name', 'species' : 'name', 'material' : 'name', 'organism_part' : 'name', 'seq_tech' : 'name',
         'library' : 'ssid' , 'library_type' : 'name', 'lane' : 'accession', 'sample': 'ssid', 'project' : 'ssid', 'file' : 'name', 'ontology' : 'name'}
        independent_table =  ['species', 'project', 'library_type', 'seq_tech']
        dependent_table =   ['individual', 'organism_part', 'material' , 'sample', 'library', 'lane', 'file']
        dependent_dic= {'organism_part': ['ontology'],  'individual' : ['species'], 'material' : ['individual', 'organism_part'],
        'sample' : ['material'], 'library' :['library_type'], 'lane' : ['sample', 'library', 'seq_tech'], 'file': ['lane']}
        raw_results_type = 'json'
    else: #if 'tissues' in entry_name or 'specimens' in entry_name:
        identifier_dic = {'individual' : 'name', 'species' : 'name', 'material' : 'name', 'location' : 'source_location', 'ontology' : 'name', 'developmental_stage' : 'name', 'project': 'name', 'sample': 'name', 'image' : 'filename'}
        independent_table =  ['species', 'location', 'project']
        dependent_table =  ['developmental_stage', 'individual', 'material', 'sample', 'image']
        dependent_dic={'developmental_stage':['ontology'], 'individual' : ['species', 'developmental_stage', 'location'], 'material' : ['individual'], 'sample':['material'], 'library' : ['library_type'],
        'lane' : ['seq_tech', 'sample', 'library'], 'file':['lane'], 'image':['individual']}
        raw_results_type = 'spreadsheet'
    try:
        for individual_name in raw_results:
            print (individual_name)
            tbflag = ""
            idflag =""
            for raw_entry in raw_results[individual_name]:
                id_dic={}
                #perform insertion if required
                if verbose:
                    logging.info(" working on individual "+individual_name+":")
                    #deal first with table with no dependent tables
                for table in independent_table:
                    if table in raw_entry and len(raw_entry[table])> 0:
                        update_flag, table_id = check_for_update(raw_entry[table], table, studyDAO, verbose)
                        if update_flag == "I":
                            table_id, tbflag = populate_table(raw_entry[table], today, studyDAO, flag, verbose, table, identifier_dic[table])
                        elif update_flag == "U":
                            tbflag = "U"
                        all_flag.append(tbflag)
                        if table_id > 0:
                            id_dic[table] = table_id
                #deal with table with dependent tables
                for table in dependent_table:
                    asso_dic = {}
                    if table in raw_entry and len(raw_entry[table]) > 0:
                            for sub_table in dependent_dic[table]:
                                #populate the dependent table table_id
                                if sub_table in id_dic:
                                    asso_dic[sub_table+"_id"] = id_dic[sub_table]
                                else:
                                    if identifier_dic[sub_table] in raw_entry[table]:
                                        #get the value from db
                                        dependent_data = studyDAO.getTableData(sub_table, sub_table+"_id", identifier_dic[sub_table] + " = '" +raw_entry[table][identifier_dic[sub_table]]+"'")
                                        if len(dependent_data) > 0:
                                            asso_dic[sub_table+"_id"] = dependent_data[0][sub_table+"_id"]
                    #now that the dependent table table_id are populated, use them to populate the table
                    if len(asso_dic) > 0:
                        raw_entry[table].update(asso_dic)
                    if table in raw_entry:
                        #ensure that a new record is created
                        if 'row_id' in raw_entry[table]:
                            del raw_entry[table]['row_id']
                        update_flag, table_id = check_for_update(raw_entry[table], table, studyDAO, verbose)
                        if update_flag == "I":
                            table_id, tbflag = populate_table(raw_entry[table], today, studyDAO, flag, verbose, table, identifier_dic[table])
                        elif update_flag == "U":
                            tbflag = "U"
                        all_flag.append(tbflag)
                        if table_id > 0:
                            #update the dependent table table_id dictionary for the next table
                           id_dic[table] = table_id
                #populate allocation table if project and individual tables have id
                if 'individual' in id_dic and 'project' in id_dic:
                    allocation_data = studyDAO.getTableData('allocation', 'project_id', 'project_id = '+str(id_dic['project'])+" and individual_id = " +str(id_dic['individual']))
                    if len(allocation_data) == 0:
                        studyDAO.populate_table('allocation', '(project_id, individual_id)', '('+ str(id_dic['project']) +','+ str(id_dic['individual']) +')')
                        all_flag.append("I")
                #populate cv and individual_data table (particular cases so consider separate ?)
                if 'individual_data' in raw_entry and len(raw_entry['individual_data']) > 0:
                    if 'value4' in raw_entry['individual_data']:
                        cv_data = studyDAO.getTableData("cv", "cv_id", "attribute = 'Weight obtained from "+entry_name+" Cichlid spreadsheet'")
                        if len(cv_data) == 0:
                            cv_id, idflag = populate_table({'attribute' : 'Weight obtained from '+entry_name+' Cichlid spreadsheet', 'comment' : 'extracted the '+str(today)}, today, studyDAO, flag, verbose, 'cv', 'attribute')
                        else:
                            cv_id = cv_data[0]['cv_id']
                        individual_data_id, id4flag = populate_table({'value' : raw_entry['individual_data']['value4'], 'unit':'g'}, today, studyDAO, flag, verbose, 'individual_data', 'value', 'cv_id', cv_id, 'individual_id', id_dic['individual'])
                        all_flag.append(id4flag)
                    if 'value2' in raw_entry['individual_data']:
                        cv_data = studyDAO.getTableData("cv", "cv_id", "attribute = 'Wrongly set as \"sex\" from \""+entry_name+"\" Cichlid spreadsheet'")
                        if len(cv_data) == 0:
                            cv_id, id2flag = populate_table({'attribute' : 'Wrongly set as \"sex\" from "'+entry_name+'" VGP spreadsheet', 'comment' : 'extracted the '+str(today)}, today, studyDAO, flag, verbose, 'cv', 'attribute')
                        else:
                            cv_id = cv_data[0]['cv_id']
                        individual_data_id, id2flag = populate_table({'value' : raw_entry['individual_data']['value2']}, today, studyDAO, flag, verbose, 'individual_data', 'value', 'cv_id', cv_id, 'individual_id', id_dic['individual'])
                        all_flag.append(id2flag)
                    if 'value' in raw_entry['individual_data']:
                        cv_data = studyDAO.getTableData("cv", "cv_id", "attribute = 'morphology'")
                        if len(cv_data) == 0:
                            cv_id, id2flag = populate_table({'attribute' : 'morphology'}, today, studyDAO, flag, verbose, 'cv', 'attribute')
                        else:
                            cv_id = cv_data[0]['cv_id']
                        individual_data_id, idflag = populate_table({'value' : raw_entry['individual_data']['value']}, today, studyDAO, flag, verbose, 'individual_data', 'value', 'cv_id', cv_id, 'individual_id', id_dic['individual'])
                        all_flag.append(idflag)
                if 'assembly' in raw_entry and len(raw_entry['assembly']) > 0:
                    cv_data = studyDAO.getTableData("cv", "cv_id", "attribute = 'estimated_genome_size'")
                    if len(cv_data) == 0:
                        cv_id, idflag = populate_table({'attribute' : 'estimated_genome_size', 'comment' : 'extracted the '+str(today) +' from "'+entry_name+'" VGP spreadsheet'}, today, studyDAO, flag, verbose, 'cv', 'attribute')
                    else:
                        cv_id = cv_data[0]['cv_id']
                    individual_data_id, idflag = populate_table({'value' : raw_entry['assembly']['length'], 'unit' : 'Gb'}, today, studyDAO, flag, verbose, 'individual_data', 'value', 'cv_id', cv_id, 'individual_id', id_dic['individual'])
                all_flag.append(idflag)
    except:
        logging.error("Rolling back database changes...")
        mydbconn.rollback()
        logging.error("The program failed to import data from the "+raw_results_type+" "+entry_name+" with the below exception:")
        raise
    else:
        if all_flag.count("I") > 0 or all_flag.count("U") > 0:
            logging.info("Committing data from the "+raw_results_type+" "+entry_name+" to the database")
            mydbconn.commit()

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
    #call for json file parsing and inserting/updating
    jresults = {}
    raw_sp_results ={}
    insert_flag =[]
    if jpath:
        #open and parse json file
        if verbose:
            logging.info("Opening the json file")
        raw_j_results, json_name = parse_json(jpath, studyDAO)
        populate_database(raw_j_results, json_name, studyDAO, verbose, mydbconn)
    #call for spreadsheet parsing and inserting/updating
    if len(spath) >0:
        #open spreadsheet url
        if verbose:
            logging.info("Opening the spreadsheet url")
        raw_sp_results, spreadsheet_name = parse_spreadsheet(spath, studyDAO)
        populate_database(raw_sp_results, spreadsheet_name, studyDAO, verbose, mydbconn)
    if verbose:
        logging.info("End of run")

if __name__ == '__main__':
    # Parse script parameters and run program setup
    parser = argparse.ArgumentParser(
        description="Population_db import spreadsheet(s) or json onto the Cichlid_TRACKING database")
    parser.add_argument("-o", "--overwrite", action ='store_true',
                        help="replace previous entries instead of updating the existent records")
    parser.add_argument("-j", "--jpath", help="path to json")
    parser.add_argument("-sp", "--spreadsheet", help="choice of 'samples', 'sequenced', 'mlw'")
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
    #depending of user input, define path to spreadsheets or to files
    if args['spreadsheet'] == 'sequenced':
        end_url = 'pub?gid=911220955'
        spath = url+end_url
    elif args['spreadsheet'] == 'samples':
        end_url = 'pub?gid=386735776'
        spath = url+end_url
    elif args['spreadsheet'] == 'mlw':
        spath = '../Cichlid_database/data/mlw_db_data.txt'
    elif args['spreadsheet'] == 'images':
        spath = '../Cichlid_database/data/2016_Sanger_Malawi_cichlid_sampling_trip.tsv'
    jpath = args['jpath']
    verbose=False
    if args['verbose']: verbose= True
    flag=False
    if args['overwrite']: flag = True
    main(programSetup)
