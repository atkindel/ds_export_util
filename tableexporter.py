from pymysql_utils1 import MySQLDB
import os
import sys
import logging
from datetime import datetime as dt
import csv

class TableExporter(MySQLDB):
    '''
    Interface to MySQL databases. Extends database interface with file
    handling and streamlined user input to facilitate rapid turnaround
    of data requests.

    Expects two files in .ssh, one with MySQL db credentials and one
    with a list of database-table pairs (e.g. RadioHut.Sales).
    '''

    def __init__(self, outdir=""):
        '''
        Initializes table export tool with db credentials from .ssh
        directory. Will write outfiles to current working directory
        unless otherwise specified.
        '''

        # Helper function for making required directories
        def ensureExists(dpath):
            if not os.path.exists(dpath):
                os.mkdir(dpath)

        # Configure export logging
        n = dt.now()
        logDir = os.getcwd()+'/logs/'
        ensureExists(logDir)
        logging.basicConfig(filename="logs/TableExport_%d-%d-%d_%s.log"
                                % (n.year, n.month, n.day, n.strftime('%I:%M%p')),
                            level=logging.INFO)

        # Set write directory for outfiles.
        self.writeDir = os.getcwd()+'/'+outdir
        ensureExists(self.writeDir)

        # Get MySQL database user credentials from .ssh directory
        home = os.path.expanduser("~")
        dbFile = home + "/.ssh/mysql_user"
        if not os.path.isfile(dbFile):
            sys.exit("MySQL user credentials not found @ %s." % dbFile)
        dbuser, dbpass = None, None
        with open(dbFile, 'r') as f:
            dbuser, dbpass = f.readline().rstrip(), f.readline().rstrip()

        # Read in table lookup from .ssh directory.
        tblFile = home + "/.ssh/ds_table_lookup.cfg"
        if not os.path.isfile(tblFile):
            sys.exit("Table lookup file not found @ %s." % tblFile)
        self.tableLookup = dict()
        with open(tblFile, 'r') as f:
            for line in f:
                db, tbl = line.split('.')
                self.tableLookup[tbl] = line

        # Initialize MySQL database connection
        MySQLDB.__init__(self, user=dbuser, passwd=dbpass)
        logging.info("Connected to database.")


    def __getColumnNames(self, tableName):
        '''
        Given a table name, returns a list containing the column names
        of the corresponding database table.
        '''
        q = "DESC %s" % tableName
        colgen = self.query(q.encode('UTF-8', 'ignore'))
        columns = []
        for row in colgen:
            columns += row[0]
        return columns


    def __getTable(self, course, table):
        '''
        Emits data from named table as a list of lists. Accepts array of
        column names matched to array of constraints, constructed as WHERE
        qualifiers in the SQL query.
        '''
        # Get table name
        tblName = self.tableLookup[table] if table in self.tableLookup else 'NULL'
        if tblName == "NULL":
            print "Requested table not in database: %s" % tblName
            return None

        # Put together WHERE clause as needed
        cid = 'course_id' if table in ['FinalGrade', 'UserGrade'] else 'course_display_name'
        constraint = "WHERE `%s`='%s'" % (cid, course)

        # Assemble query and send to database
        q = "SELECT * FROM %s %s;" % (tblName, constraint)
        rowgen = self.query(q.encode('UTF-8', 'ignore'))

        # Get column names
        cNames = self.__getColumnNames(tblName)

        # Append each row to list for output and return
        tableOutput = [cNames]
        for row in rowgen:
            strrow = map(str(), row)
            print row
            pass #TODO: add row as list  to tableOutput
        return tableOutput


    def __writeTable(self, data, filename):
        '''
        Writes data to specified CSV outfile. Expects table data to be
        formatted as a list of lists. Assumes that specified outfile
        does not already exist and must be created from scratch.
        '''
        open(filename, 'w')
        out = csv.writer(filename)
        for row in data:
            out.writerow(row)

    def exportTables(self, filename):
        '''
        Client interface to TableExporter module. Accepts a file that
        specifies table(s) to be exported as a course name separated
        from a table name by a colon. For example, the line:
         KindelU/Python101/Summer2015:EventXtract
        will emit the corresponding table from the database.
        '''
        with open(filename, 'r') as f:
            for line in f:
                course, table = line.split(':')
                data = self.__getTable(course, table)
                # filename = "%s_%s.csv" % course, table
                # self.__writeTable(data, filename)


if __name__ == '__main__':

    outdir = raw_input("Write directory for table exports: ")
    te = TableExporter(outdir)
    indir = raw_input("Name of file with export requests: ")
    te.exportTables(indir)
