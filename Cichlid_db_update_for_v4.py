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

__author__ = 'Hubert Denise, Aug. 2019'

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

def prepare_update(update_dic):
    '''
    Function to reformat a dictionary into update statement
    : input update_dic (dic) dictionary of fields to update
    : return final_statement (str) <field> = <value>, <field> = <value> where <table>_id = <identifier>. No return if there is no field to update
    '''
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

def overwrite_field(table, dic, different_items, verbose, studyDAO):
    '''
    Function to perform the update into the database with the data from spreadsheet if overwrite flag has been set
    : input table (str) name of the table where data will be updated
    : input dic (dic) database data for an individual
    : input different_items (dic) dictionary <table>:value for table which spreadsheet data differ from the database
    : input verbose (boolean) when set to True, display messages to user about run progres
    : input studyDAO (connection object) object to connect to the database
    : return table_identifier (int) value of table identifier after update took place or '0' if no update took place
    : return 'U' (str) flag to indicate that update took place
    '''
    table_identifier = 0
    update_dic = dict(different_items)
    #for individual data, use both identifier: individual_id and cv_id
    if table == 'individual_data':
        update_dic['changed'] = today
        field_statement = prepare_update(update_dic)
        try:
            studyDAO.update(table, field_statement, 'identifier_id = ' +str(dic['individual_id']) + ' and cv_id', dic['cv_id'])
        except:
            logging.info("Could not overwrite the data already present in the database. Existing now")
            raise
    #other tables have only one identifier
    else:
        table_identifier = dic[table+"_id"]
        if table not in ['project', 'developmental_stage', 'organism_part', 'location', 'cv', 'seq_centre', 'library_type', 'seq_tech']:
            update_dic['changed'] = today
        field_statement = prepare_update(update_dic)
        try:
            studyDAO.update(table, field_statement, table+"_id", table_identifier)
        except:
            logging.info("Could not overwrite the data already present in the database. Existing now")
            raise
    return table_identifier, 'U'

def update_field(table, dic, different_items, verbose, studyDAO):
    '''
    Function to perform the update into the database with the data from spreadsheet
    : input table (str) name of the table where data will be updated
    : input dic (dic) database data for an individual
    : input different_items (dic) dictionary <table>:value for table which spreadsheet data differ from the database
    : input verbose (boolean) when set to True, display messages to user about run progres
    : input studyDAO (connection object) object to connect to the database
    : return table_identifier (int) value of table identifier after update took place or '0' if no update took place
    : return 'U' (str) flag to indicate that update took place
    '''
    update_dic = {}
    insert_dic ={}
    table_identifier = 0
    #for individual data, use both identifier: individual_id and cv_id
    if table == 'individual_data':
        update_dic['changed'] = today
        update_dic['latest'] = 0
        field_statement = prepare_update(update_dic)
        try:
            studyDAO.update(table, field_statement, 'identifier_id = ' +str(dic['individual_id']) + ' and cv_id', dic['cv_id'])
        except:
            logging.info("Could not update the data already present in the database. Existing now")
            raise
        insert_dic = {k:v for k,v in dic.items() if k not in ['changed']}
        insert_dic.update(different_items)
        field_str, value_str = dic_to_str(insert_dic,", changed, ",", '"+today+"', ")
    #other tables have only one identifier
    else :
        table_identifier = dic[table+"_id"]
        if table not in ['project', 'developmental_stage', 'organism_part', 'location', 'cv', 'seq_centre', 'library_type', 'seq_tech']:
            update_dic['changed'] = today
            update_dic['latest'] = 0
            field_statement = prepare_update(update_dic)
            try:
                studyDAO.update(table, field_statement, table+"_id", table_identifier)
            except:
                logging.info("Could not update the data already present in the database. Existing now")
                raise
        insert_dic = {k:v for k,v in dic.items() if k not in ['row_id', table+'_id', 'changed']}
        insert_dic.update(different_items)
        #get higest index
        max_ID = studyDAO.getmaxIndex(table)
        table_identifier = max_ID[0]['max('+table+'_id)']+1
        #for table with latest and changed field
        if table not in ['project', 'developmental_stage', 'organism_part', 'location', 'cv', 'seq_centre', 'library_type', 'seq_tech']:
            field_str, value_str = dic_to_str(insert_dic,", changed, "+table+"_id",", '"+today+"', " + str(table_identifier))
        else:
            field_str, value_str = dic_to_str(insert_dic,", "+table+"_id",", " + str(table_identifier))
    try:
        studyDAO.populate_table(table, "("+field_str+")", "("+value_str+")")
    except:
        logging.info("Could not insert the update data in the database. Existing now")
        raise
    return table_identifier, 'U'

def insert_field(table, table_dic , table_identifier_dic, dependent_list, verbose, studyDAO):
    '''
    Function to perform the insertion into the database of the data from spreadsheet
    : input table (str) name of the table where data will be inserted
    : input table_dic (dic) parsed data for an individual from the spreadsheet
    : input table_identifier_dic (dic) dictionary <table>: (<identifier field> that is unique for this table)
    : input dependent_list (list) list of dependent table for the table, if existing
    : input verbose (boolean) when set to True, display messages to user about run progres
    : input studyDAO (connection object) object to connect to the database
    : return table_identifier (int) value of table identifier after insertion took place or '0' if no insertion took place
    : return 'I' (str) flag to indicate that insertion took place
    '''
    table_identifier = 0
    #get the dependent table if exists
    if len(dependent_list) > 0:
        for dep_table in dependent_list:
            if dep_table == 'ontology':
                dep_table_index = studyDAO.getIndex(dep_table, table_identifier_dic[dep_table], table_dic[table][table_identifier_dic[dep_table]]+"s")
            else:
                if dep_table not in ['project', 'developmental_stage', 'organism_part', 'location', 'seq_centre', 'library_type', 'cv','seq_tech']:
                    if dep_table in table_dic:
                        dep_table_index = studyDAO.getIndex(dep_table, 'latest = 1  and '+table_identifier_dic[dep_table], table_dic[dep_table][table_identifier_dic[dep_table]])
                else:
                    if dep_table in table_dic:
                        dep_table_index = studyDAO.getIndex(dep_table, table_identifier_dic[dep_table], table_dic[dep_table][table_identifier_dic[dep_table]])
            if dep_table in table_dic:
                if len(dep_table_index) > 0 :
                    table_dic[table].update(dep_table_index[0])
                else:
                    #get higest index
                    max_ID = studyDAO.getmaxIndex(dep_table)
                    if max_ID[0]['max('+dep_table+'_id)'] is not None:
                        table_identifier = max_ID[0]['max('+dep_table+'_id)']+1
                    else:
                        table_identifier=1
                    if dep_table not in ['project', 'developmental_stage', 'organism_part', 'location', 'seq_centre', 'library_type', 'cv', 'seq_tech']:
                        field_str, value_str = dic_to_str(table_dic[dep_table], ", changed, latest, "+dep_table+"_id", ", '"+today+"', 1, "+str(table_identifier))
                    else:
                        field_str, value_str = dic_to_str(table_dic[dep_table], ", "+dep_table+"_id", ", "+str(table_identifier))
                    try:
                        studyDAO.populate_table(dep_table, "("+field_str+")", "("+value_str+")")
                    except:
                        logging.info("Could not insert the new dependent data in the database. Existing now")
                    table_dic[table].update({dep_table+"_id": table_identifier})

    #add to data for table with single identifier
    if table != 'individual_data':
        #get higest index
        max_ID = studyDAO.getmaxIndex(table)
        if max_ID[0]['max('+table+'_id)'] is not None:
            table_identifier = max_ID[0]['max('+table+'_id)']+1
        else:
            table_identifier=1
        #for table with latest and changed field
        if table not in ['project', 'developmental_stage', 'organism_part', 'location', 'seq_centre', 'library_type', 'cv', 'seq_tech']:
            field_str, value_str = dic_to_str(table_dic[table], ", changed, latest, "+table+"_id", ", '"+today+"', 1, "+str(table_identifier))
        else:
            field_str, value_str = dic_to_str(table_dic[table], ", "+table+"_id", ", "+str(table_identifier))
    #for individual data, use both identifier: individual_id and cv_id
    else:
        field_str, value_str = dic_to_str(table_dic[table], ", changed, latest" , ", '"+today+"', 1")
    try:
        studyDAO.populate_table(table, "("+field_str+")", "("+value_str+")")
    except:
        logging.info("Could not insert the new data in the database. Existing now")
        raise
    return table_identifier, 'I'

def format_date(entry_date):
    '''
    function to re-format the date fields from the data fromm the spreadsheet in mysql compatible format (YYYY-MM-DD)
    : input entry_date (str) date as provided in the spreadsheet
    : return return_date (date) date formatted as YYYY-MM-DD (only numeric and separated by '-')
    '''
    pattern_date=re.compile(r'^[0-3][0-9]/[0-1][1-9]/2[0-1][0-3][0-9]')
    return_date = entry_date
    if pattern_date.match(entry_date):
        date_field_part = entry_date.split("/")
        date_part = date_field_part[2]+"-"+date_field_part[1]+"-"+date_field_part[0]
        return_date=datetime.datetime.strptime(date_part, "%Y-%m-%d").date()
    return return_date

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

def format_to_compare(dic):
    '''
    Function to reformat the latitude and longitude entries to compare them (ensure it is a string and that it does not end by '0')
    : input dic (dic) dictionary with location_field:value where location_fields are the fields from the location entry from the spreadsheet
    : return dic (dic) updated location directory
    '''
    for field in ['latitude', 'longitude']:
        dic[field] = str(dic[field])
        #make sure that last digit is not considered if equal to 0
        if dic[field][-1] =='0':
            dic[field] = dic[field][:-1]
    return dic

def add_annotations(table_eq_Annotations, studyDAO, category_Annotations, asso_dic, raw_data, cv_id):
    '''
    Function to add the annotations in the 'individual_data' or 'annotations' tables of the database.
    : input table_eq_Annotations (dic) to translate header in input datasheet into database table in order to insert the data
    : input studyDAO (connection object) object to connect to the database
    : input category_Annotations (str) category of annotations as provided in the spreadsheet
    : input asso_dic (dic) dictionary (<table>_id:identifier value) for the different tables
    : input raw_data (dic) dictionary (table:attribute) parsed from spreadsheet
    : input cv_id (int) identifier for CV entry
    : return add_flag (str) 'U' or 'I' depending if data are updated or inserted
    '''
    add_flag = ""
    table_Annotations = table_eq_Annotations[category_Annotations]
    Annot_table = table_Annotations
    #for insertion into 'annotations' table  which table_id and cv_id are identifiers
    if table_Annotations != 'individual_data':
        Annot_table ='annotations'
        table_id = table_Annotations+"_id"
        update_term = ""
        pop_term = "(table_name, table_id, cv_id, value)"
        pop_value = "('" + table_Annotations +"',"+ str(asso_dic[table_Annotations+"_id"]) +", "+ str(cv_id) +", '"+raw_data['Annotations']['value']+"')"
        if table_Annotations+'_id' not in asso_dic:
            table_Annotations == 'individual_data'
    #for insertion into 'individual_data' table  which individual_id and cv_id are identifiers
    else:
        table_id = 'individual_id'
        update_term = ", changed = "+today+", "
        pop_term = "(individual_id, cv_id, value, changed)"
        pop_value = "(" + str(asso_dic[table_id]) +", "+ str(cv_id) +", '"+raw_data['Annotations']['value']+"', '"+ today +"')"
    update_field =  raw_data['Annotations']['value']+"'" + update_term
    #check if corresponding entry in annotations table
    Annotations_data = studyDAO.getTableData(Annot_table, "*", "cv_id =  "+str(cv_id) +" and "+table_id+"  = " + str(asso_dic[table_id]))
    if len(Annotations_data) > 0:
        if Annotations_data[0]['value'] == "":
            studyDAO.update(Annot_table, "value = " +"'"+update_field,  "cv_id =  "+str(cv_id) +" and table_id  = ", asso_dic[table_id])
        elif raw_data['Annotations']['value'] not in Annotations_data[0]['value'].split('; '):
            studyDAO.update(Annot_table, "value = '"+Annotations_data[0]['value'] +"; "+ update_field,  "cv_id =  "+str(cv_id) +" and table_id", asso_dic[table_id])
        add_flag= 'U'
    else:
        studyDAO.populate_table(Annot_table, pop_term, pop_value)
        add_flag = 'I'
    return add_flag

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
    start_read = 2
    #define header for each table according to spreadsheet url (create new one if different spreadsheet provided)
    if '1978536442' in spread_path:
        eq_list=['individual-name','record-option','individual-alias', 'species-name','species-taxon_id','species-common_name','species-taxon_position','individual-sex',
        'developmental_stage-name','organism_part-name','individual-date_collected','image-filename','image-path','image-comment','project-name','project-alias',
        'project-ssid','project-accession', 'location-country', 'location-location','location-sub_location','location-latitude','location-longitude',
        'material-name','material-accession','material-type', 'material-date_received','material-storage_condition','material-volume','provider-provider_name',
        'cv-attribute','individual_data-value','individual_data-unit','individual_data-comment','sample-name', 'sample-accession','sample-ssid','lane-name','lane-accession',
        'library_type-name', 'library-ssid','file-name','file-accession','file-format', 'file-type','file-md5','file-nber_reads','seq_centre-name','seq_tech-name','Annotations-value', 'Annotations-category']
        spreadsheet = 'input_template'
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
        dataline=line.split("\t")
        #parse the spreadsheet into table and field (after removing leading and trailing space(s))
        for index in range(0,len(dataline)):
            table = eq_list[index].split("-")[0]
            field = eq_list[index].split("-")[1]
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
            if 'name' not in line_dic['material']:
                line_dic['material']['name'] = line_dic['individual']['name']
            if 'date_received' in line_dic['material'] and len(str(line_dic['material']['date_received'])) >0:
                line_dic['material']['date_received'] = format_date(str(line_dic['material']['date_received']))
            line_dic['individual']['name'] = individual_name
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
                    line_dic['species'] = updateSpecies_table(line_dic['species'])
            if 'file' in line_dic:
                if line_dic['file']['type']:
                    line_dic['file']['type']='PE'
            if 'cv' in line_dic and len(line_dic['cv']['attribute']) > 0:
                line_dic['cv']['comment'] = 'entry extracted from input spreadsheet'
            if 'location' in line_dic and (len(line_dic['location']['latitude']) > 0 and len(line_dic['location']['longitude']) >0):
                line_dic['location'] = format_to_compare(line_dic['location'])
            Line_dic=copy.deepcopy(line_dic)
            for table in line_dic:
                for field in line_dic[table]:
                    if not isinstance(line_dic[table][field], datetime.date):
                        if len(line_dic[table][field]) == 0:
                            del Line_dic[table][field]
                if len(Line_dic[table]) ==0:
                    del Line_dic[table]
            if individual_name in spread_dic:
                spread_dic[individual_name].append(Line_dic)
            else:
                spread_dic[individual_name] =[Line_dic]
    print(spread_dic)
    return spread_dic, spreadsheet

def parse_json(json_path, studyDAO):
    '''
    function to open the json and reformat the data
    : input json_path (str) absolute path to the json file
    : input studyDAO (connection object) object to connect to the database
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
                material_name = dic['value']
                if "." in dic['value'] or 'gDNA' in dic['value']:
                    individual_name, organism_part_name = extract_individual_name(dic['value'], studyDAO)
                else:
                    individual_name = dic['value']
                if individual_name not in json_dic:
                    json_dic[individual_name] = []
            else:
                if dic['attribute'] == 'is_paired_read':
                    data_dic[dic['attribute']] = 'PE'
                else:
                    #ensure that there is no apostrophe in the values
                    data_dic[dic['attribute']] = dic['value'].replace("'","\"")
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
    generic function to call for update/population of the database with data from spreadsheet
    : input raw_results (dic) parsed data from the spreadsheet
    : input entry_name (str) name of the Google spreadsheet tab
    : input studyDAO (connection object) object to connect to the database
    : input verbose (boolean) when set to True, display messages to user about run progres
    : input mydbconn (database connection_socket) connection to the Cichlid database
    : return none
    '''
    identifier_dic = {'individual' : 'name', 'species' : 'name', 'material' : 'name', 'location' : 'location', 'ontology' : 'name', 'lane': 'accession',
     'file' : 'name', 'developmental_stage' : 'name', 'project': 'name', 'sample': 'name', 'image' : 'filename', 'organism_part': 'name', 'provider' : 'provider_name',
     'cv' : 'attribute', 'seq_centre': 'name', 'seq_tech' : 'name', 'library_type' : 'name', 'library' : 'ssid'}
    dependent_table =  ['developmental_stage', 'organism_part', 'individual', 'image', 'material', 'sample', 'lane', 'library', 'individual_data', 'file']
    dependent_dic={'developmental_stage':['ontology'], 'organism_part':['ontology'], 'individual' : ['species', 'location', 'provider'],
    'material' : ['individual', 'provider', 'developmental_stage', ], 'sample':['material'], 'library' : ['library_type'], 'lane' : ['seq_tech', 'sample','library', 'seq_centre'],
    'file':['lane'], 'image':['individual'], 'individual_data': ['individual', 'cv']}
    table_eq_Annotations ={'individual' : 'individual_data','material': 'material','morphology': 'individual_data','extraction': 'material',
    'sequencing':'sample','image': 'image', 'files' : 'file', 'lab_note' : 'material','general' : 'individual_data'}
    raw_results_type = 'spreadsheet'
    all_flag =[]
    try:
        for individual_name in raw_results:
            if verbose:
                logging.info(" checking individual name: "+individual_name+":")
            #more than one entry per individual could be encountered
            for index in range(0,len(raw_results[individual_name])):
                update_flag = ""
                insert_flag = ""
                Id_overwrite_flag = ""
                Id_update_flag = ""
                ann_flag = ""
                new_rec=0
                if 'record' in raw_results[individual_name][index]:
                    if raw_results[individual_name][index]['record']['option']=='update':
                        flag = False
                    else:
                        flag = True
                    if raw_results[individual_name][index]['record']['option']=='new_record':
                        new_rec=1
                    del raw_results[individual_name][index]['record']
                table_list= (list(raw_results[individual_name][index].keys()))
                #ensure that individual table is treated at the end as it has most dependencies
                table_list.remove('individual')
                table_list.append('individual')
                #remove Annotations and individual_data table to treat them separately
                if 'Annotations' in table_list: table_list.remove('Annotations')
                if 'individual_data' in table_list: table_list.remove('individual_data')
                asso_dic={}
                insert_dic={}
                #go through the tables refered on the spreadsheet
                for table in table_list:
                    if table not in ['project', 'developmental_stage', 'organism_part', 'location', 'seq_centre', 'library_type', 'cv', 'seq_tech']:
                        db_table_data = studyDAO.getStudyData(table, "latest = 1 and "+identifier_dic[table], raw_results[individual_name][index][table][identifier_dic[table]])
                    else:
                        db_table_data = studyDAO.getStudyData(table, identifier_dic[table], raw_results[individual_name][index][table][identifier_dic[table]])
                    #case where data are already present in the database
                    if len(db_table_data) > 0:
                        #if table has table dependencies, update the raw data with the corresponding ids
                        if table in dependent_table:
                            for parent_table in dependent_dic[table]:
                                if parent_table+"_id" in asso_dic:
                                    raw_results[individual_name][index][table].update({parent_table+"_id":asso_dic[parent_table+"_id"]})
                        #only use field with data in the comparison
                        raw_dic = {k:v for k, v in raw_results[individual_name][index][table].items() if v != 'NULL'}
                        #ensure that latitude and longitude are correctly compared
                        if table == 'location':
                            db_table_data[-1] = format_to_compare(db_table_data[0])
                        #extract fields that are different
                        different_items = {k:v for k,v in raw_dic.items() if k in db_table_data[-1] and str(db_table_data[-1][k]) != str(raw_dic[k])}
                        #no different items: capture table_id in association dic (asso_dic)
                        if len(different_items) == 0:
                            asso_dic[table+"_id"] = db_table_data[0][table+"_id"]
                        #if there is difference: update, overwrite or insert
                        else:
                            #if flag: then overwrite (update) the record and keep the sample_id
                            if flag:
                                #update results in db
                                table_id, update_flag = overwrite_field(table, db_table_data[-1], different_items,verbose, studyDAO)
                            else:
                                #update previous record and create an updated one
                                table_id, update_flag = update_field(table, db_table_data[-1], different_items,verbose, studyDAO)
                            if table_id != 0:
                                asso_dic[table+"_id"] = table_id
                    #There were no data for this table into the db: insert but may need to fetch id from other tables
                    else:
                        #table have dependence so get the id from these tables
                        if table in dependent_table:
                            table_id, insert_flag = insert_field(table, raw_results[individual_name][index], identifier_dic, dependent_dic[table], verbose, studyDAO)
                        #table have no dependence so just insert
                        else:
                            table_id, insert_flag = insert_field(table, raw_results[individual_name][index], identifier_dic, [], verbose, studyDAO)
                        if table_id != 0:
                            asso_dic[table+"_id"] = table_id
                #deal with allocation: check if individual_id and project_id in allocation.. if not present, insert into allocation
                if 'individual_id' in asso_dic and 'project_id' in asso_dic:
                    alloc_data = studyDAO.getTableData('allocation', '*', 'individual_id = '+str(asso_dic['individual_id']) +' and project_id = ' + str(asso_dic['project_id']))
                    if len(alloc_data) == 0:
                        studyDAO.populate_table("allocation", "(individual_id, project_id)",  "("+str(asso_dic['individual_id'])+","+ str(asso_dic['project_id'])+")")
                #deal with individual_data
                #get data for cv and individual identifiers
                if new_rec == 0:
                    cv_data= studyDAO.getTableData("individual_data", "cv_id", "value = '" + raw_results[individual_name][index]['Annotations']['value'] +"';")
                    asso_dic['cv_id'] = cv_data[0]['cv_id']
                    Ind_data_data = studyDAO.getTableData('individual_data', '*', 'individual_id = '+str(asso_dic['individual_id']) +' and cv_id = ' + str(asso_dic['cv_id']))
                    if len(Ind_data_data) > 0:
                        if 'Annotations' in raw_results[individual_name][index] and raw_results[individual_name][index]['Annotations']['category']=='individual':
                            raw_Id_dic = {k:v for k, v in raw_results[individual_name][index]['Annotations'].items() if v != 'NULL'}
                            different_items = {k:v for k,v in raw_Id_dic.items() if k in Ind_data_data[0] and str(Ind_data_data[0][k]) != str(raw_Id_dic[k])}
                            #no different items: capture table_id in association dic (asso_dic)
                            if len(different_items) !=0:
                                if flag:
                                    #update results in db - overwrite_field(table, dic, different_items, verbose, studyDAO):
                                    Id_id, Id_overwrite_flag = overwrite_field('individual_data', Ind_data_data[0], different_items,verbose, studyDAO)
                                else:
                                    Id_id, Id_update_flag = update_field('individual_data', Ind_data_data[0], different_items,verbose, studyDAO)
                                if Id_id != 0:
                                    asso_dic['individual_data_id'] = Id_id
                    else:
                        table_id, Id_insert_flag = insert_field('individual_data', raw_results[individual_name][index], identifier_dic, ['individual', 'cv'], verbose, studyDAO)
                #deal with data in the Annotations fields
                if 'Annotations' in raw_results[individual_name][index]:
                    category_Annotations = raw_results[individual_name][index]['Annotations']['category']
                    value_Annotations = raw_results[individual_name][index]['Annotations']['value']
                    #associate annotations to the corresponding table
                    table_Annotations = table_eq_Annotations[category_Annotations]
                    #get the cv_id
                    cv_data = studyDAO.getTableData("cv", "cv_id", "attribute = 'notes' and comment = 'entry for table " + table_Annotations+"'")
                    if len(cv_data) ==  0:
                        studyDAO.populate_table("cv", "(attribute, comment)",  "('notes', 'entry for table " + table_Annotations+"')")
                        cv_data = studyDAO.getTableData("cv", "cv_id", "attribute = 'notes' and comment = 'entry for table " + table_Annotations+"'")
                        cv_id = cv_data[0]['cv_id']
                        ann_flag = add_annotations(table_eq_Annotations, studyDAO, category_Annotations, asso_dic, raw_results[individual_name][index], cv_id)
                all_flag+=[update_flag,insert_flag,Id_overwrite_flag,Id_update_flag,ann_flag]
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
        description="Cichlid_db_update import input spreadsheet onto the Cichlid_TRACKING database")
    parser.add_argument("-o", "--overwrite", action ='store_true',
                        help="replace previous entries instead of updating the existent records")
    parser.add_argument("-sp", "--spreadsheet", help="spreadsheet path or input")
    parser.add_argument("-j", "--jpath", help="path to json")
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
    jpath = args['jpath']
    verbose=False
    if args['verbose']: verbose= True
    flag=False
    if args['overwrite']: flag = True
    main(programSetup)
