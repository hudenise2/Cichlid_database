# -*- coding: utf-8 -*-


from urllib.request import urlopen
import sys
from xml.dom import minidom
import xml.etree.ElementTree as ET

def getTaxid(user_input):
	#created by H.DENISE 15-11-18#
	#replace space with "+" in scientifc name
	query=user_input.replace(" ","+")
	#check if a list has been provided and reformat it appropriately
	if len(query.split(",")) > 1:
		for i in range(0, len(query.split(","))):
			if i==0:Query=query.split(",")[0]
			elif len(query.split(",")[i]) > 0 :
				Query=Query+",Taxon:"+query.split(",")[i]
	else : Query = query
	#Declaration
	CleanSet=[]
	tag = 0
	lineage_tables = {}
	lineage_tables["query"]=[]
	taxo_rank = ["superkingdom","phylum","class","order","family","genus","species"]
	#send back request to ENA and get xml(s) in return
	url = "http://www.ebi.ac.uk/ena/data/view/Taxon:"+Query+"&display=xml"
	try:
		connection_socket = urlopen(url)
		xml_doc = minidom.parse(connection_socket)
		connection_socket.close()
		res= (xml_doc.getElementsByTagName("taxon"))
	except:
		return list()
	if len(res) > 0:
		return ([res[0].getAttribute("rank"), res[0].getAttribute("taxId"), res[0].getAttribute("scientificName"), res[0].getAttribute("commonName")])
