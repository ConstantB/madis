
"""
.. function:: output formatting_options 'filename' query

Writes in *filename* the output of *query* formatted according to *formatting* options.

:Returned table schema:
    - *return_value* int
        Boolean value 1 indicating success. On failure an exception is thrown.

Formatting options:

.. toadd html        In html mode table is formatted as an html table TODO ????

:mode:
    - plain     *Default*. The columns are concatened and written together.
    - tsv       Writes data in a tab separated format. *TSV* mode is autoselected when the filename ends in ".tsv".
    - csv       Writes data in a comma separated format. *CSV* mode is autoselected when the filename ends in ".csv".
    - db        Writes data in a SQLite DB. *DB* mode is autoselected when the filename ends in ".db".

                - If *split* option is also provided, the output is multiplexed into multiple databases according to first input table column.

                - If pagesize:xxxxx option is given, set new DBs page size to parameter given, if not inherit page size from parent DB.

    - gtable    In gtable mode table is formatted as a google Data Table for visualisation.
    - gjson     In gjson mode table is formatted in a json format accepted by google visualisation widgets.

    If *mode* is not *csv* any given csv formatting options are ignored

:append:
    t/f If true the output is append in the file, ignored in compression mode

:split:
    (number) It splits the input into many db files. Splitting only works when writting to a *db*. Splitting is done by using the first column of
    the input and it outputs all columns except the first one, in the db files. If the *split* argument is greater than *1* then the output will
    always be splitted to the defined number of db files. If the split argument is 1 or lower, then the output will only contain the parts of which
    a key were found on the first column of the input data.

Detailed description of additional output formating options can be found in :func:`~functions.vtable.file.file` function description.

Examples:

    >>> table1('''
    ... James   10	2
    ... Mark    7	3
    ... Lila    74	1
    ... ''')
    >>> sql("select * from table1")
    a     | b  | c
    --------------
    James | 10 | 2
    Mark  | 7  | 3
    Lila  | 74 | 1
    >>> sql("output file:../../tests/table1.csv delimiter:# header:t select a as name , b as age, c as rank from table1")
    return_value
    ------------
    1
    >>> sql("file file:../../tests/table1.csv delimiter:# header:t")
    name  | age | rank
    ------------------
    James | 10  | 2
    Mark  | 7   | 3
    Lila  | 74  | 1
"""
import os.path

import setpath
from vtout import SourceNtoOne
from lib.dsv import writer
import gzip
from lib.ziputils import ZipIter
import functions
from lib.vtoutgtable import vtoutpugtformat
import lib.inoutparsing
import os
import apsw
from collections import defaultdict

registered=True

def fileit(p,append=False):
    if append:
        return open(p,"a")
    return open(p,"w")

def getoutput(p,append,compress,comptype):
    source=p
    it=None

    if compress and ( comptype=='zip'):
        it=ZipIter(source,"w")
    elif compress and ( comptype=='gzip' or comptype=='gz'):
            itt=fileit(source+'.gz')
            it=gzip.GzipFile(mode="w",fileobj=itt)
    else:
        it=fileit(source,append)
    return it

def autoext(f, ftype, typelist):
    fname, ext=os.path.splitext(f)
    if ext=='' and ftype in typelist:
        ext=typelist[ftype]
        return fname+'.'+ext
    return f

def autotype(f, extlist):
    fname, ext=os.path.splitext(f)
    if ext!='':
        ext=ext[1:]
        if ext in extlist:
            return extlist[ext]
    return 'plain'

def outputData(diter, connection, *args, **formatArgs):
    ### Parameter handling ###
    where=None
    if len(args)>0:
        where=args[0]
    elif 'file' in formatArgs:
        where=formatArgs['file']
    else:
        raise functions.OperatorError(__name__.rsplit('.')[-1],"No destination provided")

    if 'file' in formatArgs:
        del formatArgs['file']

    if 'mode' not in formatArgs:
        formatArgs['mode']=autotype(where, {'csv':'csv', 'tsv':'tsv', 'xls':'tsv', 'db':'db'})

    if 'header' not in formatArgs:
        header=False
    else:
        header=formatArgs['header']
        del formatArgs['header']

    if 'compression' not in formatArgs:
       formatArgs['compression']=False
    if 'compressiontype' not in formatArgs:
        formatArgs['compressiontype']='zip'

    append=False
    if 'append' in formatArgs:
        append=formatArgs['append']
        del formatArgs['append']

    type2ext={'csv':'csv', 'tsv':'xls', 'plain':'txt', 'db':'db'}

    where=autoext(where, formatArgs['mode'], type2ext)
    filename, ext=os.path.splitext(os.path.basename(where))

    if formatArgs['mode']!='db':
        fileIter=getoutput(where,append,formatArgs['compression'],formatArgs['compressiontype'])

    del formatArgs['compressiontype']
    del formatArgs['compression']
    try:
        if formatArgs['mode']=='csv':
            del formatArgs['mode']
            csvprinter=writer(fileIter,'excel',**formatArgs)
            for row,headers in diter:
                if header:
                    csvprinter.writerow([h[0] for h in headers])
                    header=False
                csvprinter.writerow(row)
        elif formatArgs['mode']=='tsv':
            del formatArgs['mode']
            csvprinter=writer(fileIter,'excel-tab',**formatArgs)
            for row,headers in diter:
                if header:
                    csvprinter.writerow([h[0] for h in headers])
                    header=False
                csvprinter.writerow([x.replace('\t','    ') if type(x)==str or type(x)==unicode else x for x in row])
        elif formatArgs['mode']=='gtable':
            vtoutpugtformat(fileIter,diter,simplejson=False)
        elif formatArgs['mode']=='gjson':
            vtoutpugtformat(fileIter,diter,simplejson=True)
        elif formatArgs['mode']=='html':
            raise functions.OperatorError(__name__.rsplit('.')[-1],"HTML format not available yet")
        elif formatArgs['mode']=='plain':
            for row,headers in diter:
                fileIter.write(((''.join([unicode(x) for x in row]))+'\n').encode('utf-8'))
        elif formatArgs['mode']=='db':

            def createdb(where, tname, schema, page_size=16384):
                c=apsw.Connection(where)
                cursor=c.cursor()
                list(cursor.execute('pragma page_size='+str(page_size)+';pragma cache_size=-1000;pragma legacy_file_format=false;pragma synchronous=0;pragma journal_mode=OFF;'))
                create_schema='create table '+tname+' ('
                create_schema+='`'+unicode(schema[0][0])+'`'+ (' '+unicode(schema[0][1]) if schema[0][1]!=None else '')
                for colname, coltype in schema[1:]:
                    create_schema+=',`'+unicode(colname)+'`'+ (' '+unicode(coltype) if coltype!=None else '')
                create_schema+='); begin;'
                list(cursor.execute(create_schema))
                insertquery="insert into "+tname+' values('+','.join(['?']*len(schema))+')'
                return c, cursor, insertquery

            if 'pagesize' in formatArgs:
                page_size=int(formatArgs['pagesize'])
            else:
                page_size=list(connection.cursor().execute('pragma page_size'))[0][0]
                
            tablename=filename
            if 'tablename' in formatArgs:
                tablename=formatArgs['tablename']

            if 'split' in formatArgs:
                fullpath=os.path.split(where)[0]
                def cdb():
                    global insertqueryw
                    unikey = unicode(key)
                    t=createdb(os.path.join(fullpath, filename+'.'+unikey+ext), tablename, headers[1:], page_size)
                    splitkeys[unikey]=t[1].execute
                    insertqueryw = t[2]
                    dbcon[key]=t[0], t[1]
                    # Case for number as key
                    if unikey != key:
                        splitkeys[key] = splitkeys[unikey]
                    return splitkeys[key]
                
                dbcon = {}
                splitkeys=defaultdict(cdb)

                row, headers = diter.next()
                key=row[0]
                cexec = splitkeys[key]
                cexec(insertqueryw, row[1:])
                # Copy to local var
                insquery = insertqueryw
                for row, headers in diter:
                    key=row[0]
                    splitkeys[key](insquery, row[1:])

                # Create other parts
                maxparts = 1
                try:
                    maxparts = int(formatArgs['split'])
                except ValueError:
                    maxparts = 1

                if maxparts > 1:
                    for i in xrange(0, maxparts):
                        if i not in splitkeys:
                            key = i
                            tmp = splitkeys[key]

                for c, cursor in dbcon.values():
                    if c != None:
                        cursor.execute('commit')
                        c.close()
            else:
                row, headers=diter.next()
                c, cursor, insertquery=createdb(where, tablename, headers, page_size)
                cursor.execute(insertquery, row)
                cursor.executemany(insertquery, (x[0] for x in diter))
                list(cursor.execute('commit'))
                c.close()
        else:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"Unknown mode value")

    except StopIteration,e:
        pass

    if 'mode' not in formatArgs or ('mode' in formatArgs and formatArgs['mode']!='db'):
        fileIter.close()


boolargs=lib.inoutparsing.boolargs+['append','header','compression']


def Source():
    global boolargs, nonstringargs
    return SourceNtoOne(outputData,boolargs, lib.inoutparsing.nonstringargs,lib.inoutparsing.needsescape, connectionhandler=True)


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
