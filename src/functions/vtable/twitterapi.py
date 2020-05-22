import setpath
import vtbase
import functions
import gc
from datetime import datetime
import twitter
import apsw
import socket

### Classic stream iterator
registered=True
       
class twitterapi(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):
        largs, dictargs = self.full_parse(parsedArgs)

        if 'key' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No URL argument ")
        else:
			key= dictargs['key']
			
        if 'rate' in dictargs:  
			rate = dictargs['rate']
        else:
			rate = 0
        
        
        schema =  [('id','text'), ('tweet','text'), ('name','text'), ('location','text'), ('favourites','text'), ('screen_name','text'), ('friends','text'), ('followers','text'),  ('sentiment','text')]
        yield schema
        
        db = 'db5.db'
        tname = key
        rows = checkTableMetadata(key,rate,tname,db)     
        if rows is not None:
			#print "GETTING TABLE FROM CACHE"
			for r in rows:
				yield r
			return; 
								
        api = twitter.Api(consumer_key='5vQVQ4B8bUcNGG3WOKr80gPdQ', 
        consumer_secret='jkQw1PPQrKcKddBjg6AqYNH3n7cAogXhNTwf4m13urR37zKUdG', 
        access_token_key='747542150561341440-RyK8r6AA0iCr3w5cbuNKmcxCDRfdJ42',
         access_token_secret='v5PfDnaLCIRu8KyLmfzXDOrykUtK96mmIwkTQNoUHG7mW');
        results = api.GetSearch(raw_query="l=&q="+key+"%20-filter%3Aretweets&count=100")
        
        tuples = []
        sentiment=''
        host = socket.gethostname()
        port = 12345 
        for r in results:
			tweet = unicode(r.text) #s.connect((host, port))
			#s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			#s.connect((host, port))
			#s.sendall(r.text.encode('UTF-8'))
			#sentiment = s.recv(1024)
			t = (r.id, tweet , r.user.name, r.user.location, r.user.favourites_count, r.user.screen_name, r.user.friends_count, r.user.followers_count, sentiment)
			yield t
			tuples.append(t)
        s.close()
						
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
		
		for row in cursor.execute("select (strftime('%s','now') - strftime('%s', ttime))/60 as time from twitter_metadata where tname ='" + tname + "'"):
			cnt = cnt + 1
		if cnt < 1 :  #no such entry - insert entry
			#print "Registering metadata"
			statement = "insert into  twitter_metadata (query,tname,ttime) values (?,?,CURRENT_TIMESTAMP)"
			cursor.execute(statement,(query,tname))
		elif int(row[0]) > int(f): #expired entry - update 
			#print "Expired entry: Updating table"
	
			statement = "update  twitter_metadata set ttime = CURRENT_TIMESTAMP where tname = '" + tname + "'" #set time = now()?
			statement = statement + " ;  drop table if exists " + tname #delete table
			cursor.execute(statement)
		else:  #getTableContents
			#print "I CAN USE THE CACHE"
			statement = "select * from " + tname
			rows = cursor.execute(statement).fetchall()
	except apsw.SQLError as e:
		print e
		try:  #table does not exist
			cursor.execute("create table if not exists twitter_metadata(query text, tname text, ttime)")
			#print "created metadata  table. ERROR: " + str(e)
		except apsw.SQLError as err:
			#print "table exists. Error: " + str(err)
			c.close()
			return rows
		try:
			statement = "insert into twitter_metadata(query, tname, ttime) values (?,?,CURRENT_TIMESTAMP) "
			cursor.execute(statement,(query,tname))
		except apsw.SQLError as err:
			print err
			c.close()
			
	c.close()		
	return rows		

def Source():
    return vtbase.VTGenerator(twitterapi)

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
