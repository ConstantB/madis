import setpath
import vtbase
import functions
import gc
from datetime import datetime
import foursquare as f
import apsw
import os
import sys

### Classic stream iterator
registered=True


       
class foursqr(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):
		
        largs, dictargs = self.full_parse(parsedArgs)

        if 'key' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No Search key argument ")
        else:  
			key= dictargs['key']
			
        if 'near' not in dictargs:
			raise functions.OperatorError(__name__.rsplit('.')[-1],"No Search location specified ")
        else:
			near= dictargs['near']
			
        if 'rate' in dictargs:  #update frequency rate
			rate = dictargs['rate']
        else:
			rate = 0
        
        attrs = ('id','name','hereNow_count','category','contact','location','stats_checkinsCount','stats_tipCount','stats_usersCount') #removed tips for encoding issues when storing to table
        schema = []

        for a in attrs:
			schema = schema + [(a,'text')]
        yield schema
        
        tname = (key + " " + near).replace(" ", "_") 
        db = 'db4.db'
        rows = checkTableMetadata(key,rate,tname,db)     
        if rows is not None:
			#print "GETTING TABLE FROM CACHE"
			for r in rows:
				yield r
			return; 
			
        client = f.Foursquare(client_id='GQMVTAZZPTYOEV5IM4VVTFK5QKSIPWU4WPSREPJKTRK4TLSX', client_secret='HUBLQY2MDPNGG1OK2S02M3YBX0FPB42J0PEEGBQNWC21ZYPZ')
        venues = client.venues.search(params={'query': 'coffee','near':'Chicago, IL','limit':'10'})
        path= os.path.abspath("dbname")

        
        tuples = []
        for v in venues['venues']:
			t = []
			for a in attrs:
				if a == 'category':
					value = getCategory(v)
				elif a == 'contact':
					value = getContact(v);
				elif a == 'location':
					value = getLocation(v);
				elif a == 'tips':
					value = str(getTips(v['id'],client).encode('utf-8'))
				elif a.find('_') > 0 :
					value = getValue(v,a)
				else:
					value = v[a]
				t = t  + [value]
		#		print envar['db']
			yield t
			tuples.append(t)
			
        createTable(db,tname,schema,tuples) #materialize the table
        #getMaterializedContent(db,tname)
        
        gc.enable()
        

def createTable(db,tname, schema, rows, page_size=16384):
	c=apsw.Connection(db)
	cursor=c.cursor()
	list(cursor.execute('pragma page_size='+str(page_size)+';pragma cache_size=-1000;pragma legacy_file_format=false;pragma synchronous=0;pragma journal_mode=OFF;PRAGMA locking_mode = EXCLUSIVE'))
	create_schema='create table if not exists '+tname+' ('
	create_schema+='`'+unicode(schema[0][0])+'`'+ (' '+unicode(schema[0][1]) if schema[0][1]!=None else '')
	for colname, coltype in schema[1:]:
		create_schema+=',`'+unicode(colname)+'`'+ (' '+unicode(coltype) if coltype!=None else '')
	create_schema+='); begin exclusive;'
	list(cursor.execute(create_schema))
	insertquery="insert into "+tname+' values('+','.join(['?']*len(schema))+')'
	gc.disable()
	cursor.executemany(insertquery, rows)
	gc.enable()
	list(cursor.execute('commit'))
	c.close()
	

def getMaterializedContent(db,tname):
	c = apsw.Connection(db)
	cursor = c.cursor()
	
	for i in cursor.execute("select * from " + tname):
		print i
	c.close()
		
def checkTableMetadata(query,f, tname,db):
	#os.remove(db)
	c=apsw.Connection(db)
	cursor = c.cursor()
	
	rows = None
	cnt = 0
	try:
		#print "retrieving data from table"
		for row in cursor.execute("select (strftime('%s','now') - strftime('%s', ttime))/60 as time from foursquare_metadata where tname ='" + tname + "'"):
			cnt = cnt + 1
		if cnt < 1 :  #no such entry - insert entry
			#print "Registering metadata"
			statement = "insert into  foursquare_metadata (query,tname,CURRENT_TIMESTAMP) values (?,?,CURRENT_TIMESTAMP)"
			cursor.execute(statement,(query,tname))
		elif int(row[0]) > int(f): #expired entry - update 
			#print "Expired entry: Updating table"
			statement = "update  foursquare_metadata set ttime = CURRENT_TIMESTAMP where tname = '" + tname + "'" #set time = now()?
			statement = statement + " ;  drop table " + tname #delete table
			cursor.execute(statement)
		else:  #getTableContents
			#print "I CAN USE THE CACHE"
			statement = "select * from " + tname
			rows = cursor.execute(statement).fetchall()
			#print rows
	except apsw.SQLError as e:
		#print e
		try:  #table does not exist
			cursor.execute("create table if not exists foursquare_metadata(query text, tname text, ttime)")
			#print "created metadata  table. ERROR: " + str(e)
		except apsw.SQLError as err:
			print "table exists. Error: " + str(err)
			c.close()
			return rows
		try:
			statement = "insert into foursquare_metadata(query, tname, ttime) values (?,?,CURRENT_TIMESTAMP) "
			cursor.execute(statement,(query,tname))
		#	for i in cursor.execute("select * from foursquare_metadata"):
		#		print i
		except apsw.SQLError as err:
			print err
			c.close()
			
	c.close()		
	return rows		

def getCategory(cats):
	for c in cats['categories']:
		if c['primary'] is True:
			return c['shortName'];

def getValue(vi,text):
	vals = text.split('_')
	one = vals[0]
	two = vals[1]	
	return vi[one][two]	

def getTips(i,c): #maybe fetch also the likes foreach tip and rank by popularity?
	tips = c.venues.tips(i)
	text = ""
	for tip in tips['tips']['items']:
		text = text + " | " + tip['text']
	return text
	
def getLocation(v):
	return v['location']['address']	
			
def getContact(v):
	if 'twitter' in v['contact']:
		return v['contact']['twitter']
	elif 'facebook' in v['contact']:
		return v['contact']['facebook']
	elif 'phone' in v['contact']:
		return v['contact']['phone']
	else:
		return 'N/A'
		
def Source():
    return vtbase.VTGenerator(foursqr)

if not ('.' in __name__):
    """
    This is needed to be able to test the function, put it at the end of every
    new function you create
    """
    import sys
    import setpath
    from functions import *
    testfunction()
    if __name__ == "__main__":
        reload(sys)
        sys.setdefaultencoding('utf-8')
        import doctest
        doctest.testmod()
