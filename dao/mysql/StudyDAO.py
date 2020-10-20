__author__ = 'hudenise'



class StudyDAO:
	"""
	Data access object for for the tables in VGP_TRACKING
	"""

	def __init__(self, dataAccessObject):
		"""
		Constructor
		"""
		self.dataAccessObject = dataAccessObject

	def delete_data(self, table, identifier, identifiant):
		query= "DELETE from "+table+" where "+identifier+" = "+identifiant
		return self.dataAccessObject._runQuery(query)

	def getLinkData(self, table1, field1, table2, field2, returnfield, identifier, crit_table):
		query = "SELECT "+returnfield+" from " + table1 +" t1 join " +table2+" t2 on t1."+field1 +" = t2."+field2 +" where "+ crit_table + " = \'{0}\'""".format(identifier)
		return self.dataAccessObject._runQuery(query)

	def getStudyData(self, table, field, identifier):
		query = "SELECT * from " + table+ " where " + field + " = \'{0}\'".format(identifier)
		print(query)
		return self.dataAccessObject._runQuery(query)

	def getTableData(self, table, return_field, identifier):
		query = "SELECT " + return_field +" from " + table+ " where " +identifier
		print(query)
		return self.dataAccessObject._runQuery(query)

	def getmaxIndex(self, table):
		query = """SELECT max(""" + table+"""_id) from """ + table
		return self.dataAccessObject._runQuery(query)

	def getIndex(self, table, field, criteria):
		query = "SELECT " + table +"_id from " + table +" where " +field + " = {0}".format(criteria)
		return self.dataAccessObject._runQuery(query)

	def populate_table(self, table, field_str, value_str):
		query = "insert INTO "+table +" "+field_str+" values "+value_str
		print(query)
		return self.dataAccessObject._runQuery(query)

	def update(self, table, field_statement, identifier, identifiant):
		query = "update "+table +" set "+field_statement +" where " + identifier +" = '" + str(identifiant) +"'"
		print(query)
		return self.dataAccessObject._runQuery(query)

	def createViews(self, view_name, table_name):
		query = "create or replace view `"+view_name + "` as select * from "+table_name +" where latest = true;"
		return self.dataAccessObject._runQuery(query)

class VGDBError(Exception):
	"""VG Data access exception"""

	def __init__(self, msg):
		self.msg = msg
