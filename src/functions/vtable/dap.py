"""

.. function:: slidingwindow(window) -> query results

select * from (opendap url:http://test.opendap.org/dap/data/nc/coads_climatology.nc dim:AIRT slice:(0,),(10,14),(10,14));

Returns the query input results annotated with the window id as an extra column.
The window parameter defines the size of the window.

:Returned table schema:
    Same as input query schema.

Examples::

    >>> table1('''
    ... James   10
    ... Mark    7
    ... Lila    74
    ... Jane    44
    ... ''')
    >>> sql("slidingwindow window:2 select * from table1")
    wid | a     | b
    ----------------
    0   | James | 10
    1   | James | 10
    1   | Mark  | 7
    2   | Mark  | 7
    2   | Lila  | 74
    3   | Lila  | 74
    3   | Jane  | 44
    >>> sql("slidingwindow window:3 select * from table1")
    wid | a     | b
    ----------------
    0   | James | 10
    1   | James | 10
    1   | Mark  | 7
    2   | James | 10
    2   | Mark  | 7
    2   | Lila  | 74
    3   | Mark  | 7
    3   | Lila  | 74
    3   | Jane  | 44


"""

import setpath
import vtbase
import functions
import gc
from datetime import datetime
from dateutil.relativedelta import *
from pydap.client import open_url
from pydap.client import open_file
from netCDF4 import Dataset
from netCDF4 import num2date
from osgeo import ogr
import ast 
from shapely.geometry import Point
from shapely.wkt import loads


### Classic stream iterator
registered=True
       
class dap(vtbase.VT):
    def VTiter(self, *parsedArgs,**envars):
        largs, dictargs = self.full_parse(parsedArgs)

        if 'url' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No URL argument ")
        url=dictargs['url']
        
        if 'var' not in dictargs:
            var = 'lai'
        else:
			var=dictargs['var']
        
				
        if 'query' not in dictargs:
			sl = None;
			#print dictargs
        else:
			sl=dictargs['query']
			window=loads(sl) #loading a wkt to refine the geometries
			
        #cursor().execute("PRAGMA temp_store_directory='.';PRAGMA page_size=16384;PRAGMA default_cache_size=3000;")

        #print sl

        #cur=envars['db'].cursor()
        #c=cur.execute(query, parse = False)

       # try:
       #     yield [('wid','integer')] + list(cur.getdescriptionsafe())
       ## except StopIteration:
         #   try:
         #       raise
          #  finally:
          ##      try:
           #         c.close()
            #    except:
             #       pass

        gc.disable()
        nc_fid = Dataset(url, "r", format="NETCDF4")

        lats = nc_fid.variables['y'][:]
        lons = nc_fid.variables['x'][:]
        times = nc_fid.variables['T'][:]
        lai = nc_fid.variables[var][:]
        i = 1
        j = 1
        h = 1
        
        yield [('id', 'integer')] + [(var, 'float')] + [('time', 'float')]   + [('wkt', 'text')]
        
        for _ in times:
			for lat in lats:
				for lon in lons:
					obsstep =  str(datetime(1970,1,1) + relativedelta(days=int(times[i - 1]))).replace(' ','T')
					laival =  lai[i - 1, j - 1, h - 1]
					if laival <= 0:
						continue;
					if lon < 180.0:
							geomcoords = "POINT(" + repr(lon) + " "+repr(lat) + ")"
					else:
							geomcoords = "POINT(" + repr(lon - 360.0) + " " + repr(lat) + ")"
							
					yield [str(i)+str(j)+str(h)] + [float(laival)] + [obsstep] + [geomcoords]
					                
					h += 1
					if h == lons.__len__():
						h = 1

				j += 1
				if j == lats.__len__():
					j = 1

			i += 1

        nc_fid.close()
        gc.enable()


def Source():
    return vtbase.VTGenerator(dap)

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
