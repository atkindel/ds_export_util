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
        with open(dbFile, 'r') as f:
            for line in f:
                db, tbl = line.split('.')
                self.tableLookup[tbl] = line

        # Initialize MySQL database connection
        MySQLDB.__init__(self, user=dbuser, passwd=dbpass)
        logging.info("Connected to database.")


    def __assembleWhere(self, columns, values):
        '''
        Given an array of column names and an array of values, builds a
        compound WHERE clause. Assumes that columns should equal the value
        provided. Matches arrays by index. Truncates longer of two arrays
        if lengths are mismatched.
        '''
        for col, val in columns, values:
            pass #TODO: serialize as needed


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


    def __getTable(self, table, columns=None, values=None):
        '''
        Emits data from named table as a list of lists. Accepts array of
        column names matched to array of constraints, constructed as WHERE
        qualifiers in the SQL query.
        '''
        # Get table name
        tblName = self.tableLookup.pop(table, "NULL")
        if tblName == "NULL":
            raise "Requested table not in database: %s" % tableName

        # Put together WHERE clause as needed
        if (columns != None && values != None):
            constraints = self.__assembleWhere(columns, values)

        # Assemble query and send to database
        q = "SELECT * FROM %s %s;" % (tblName, constraints)
        rowgen = self.query(q.encode('UTF-8', 'ignore'))

        # Get column names
        cNames = self.__getColumnNames(tblName)

        # Append each row to list for output and return
        tableOutput = [cNames]
        for row in rowgen:
            pass #TODO: add row as tuple to tableOutput
        return tableOutput


    def __writeTable(self, data, filename):
        '''
        Writes data to specified CSV outfile. Expects table data to be
        formatted as a list of lists. Assumes that specified outfile
        does not already exist and must be created from scratch.
        '''
        open(filename+".csv", 'w')
        out = csv.writer(filename)
        for row in data:
            out.writerow(row)


if __name__ == '__main__':

    te = TableExporter("tables")
