#!/Library/Frameworks/Python.framework/Versions/3.7/bin/python3


from urllib.request import urlopen
import sys
from xml.dom import minidom
import xml.etree.ElementTree as ET

def getTaxid(user_input):
	#created by H.DENISE 15-11-18#
	#replace space with "+" in scientifc name
	query=user_input.replace(" ","+")
	#check if a list has been provided and reformat it appropriately
	Query = query
	#Declaration
	tag = 0
	lineage_tables = {}
	lineage_tables["query"]=[]
	taxo_rank = ["superkingdom","phylum","class","order","family","genus","species"]
	#send back request to ENA and get xml(s) in return
	#url = "http://www.ebi.ac.uk/ena/data/view/Taxon:"+Query+"&display=xml"
	if "+" in Query:
		url = "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?name="+Query+"&lvl=0"
	else:
		url = "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id="+Query+"&lvl=0"
	try:
		connection_socket = urlopen(url)
		html_doc = connection_socket.read()
		res= (html_doc.decode("utf8"))
		connection_socket.close()
	except:
		return list()
	if Query != "":
		title =""
		common_name =""
		rank =""
		taxid = 0
		data = res.split("\n")
		for line in data:
			if '<title>Taxonomy browser' in line :
				end = line.index('</title>')
				title= line[27:end-1]
			if line.startswith('Genbank common name'):
				common_name= line[29:line.index('</str')]
				end= line[line.index('>Rank:'):].index('</str')
				rank = line[line.index('>Rank:')+15:end + line.index('>Rank:')]
			if line.startswith('Taxonomy ID'):
				taxid = line[13: line.index('<small>')]
		return [title, common_name, rank, taxid]
returndic ={}
'''
out=open("update","w")
for entry in open("taxhd","r"):
	entry=entry.strip()
	ENA_taxo = getTaxid(entry)
	if entry != '':
		returndic[entry] = ENA_taxo
		out.write("update species set common_name = '"+returndic[entry][1]+"', taxon_position = '"+returndic[entry][2]+"' where taxon_id  ="+entry+";")
'''
