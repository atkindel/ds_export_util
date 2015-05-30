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

    def writeTableToFile(self, data, filename):
        '''
        Writes data to specified CSV outfile. Expects table data to be
        formatted as a list of tuples. Assumes that specified outfile
        does not already exist and must be created from scratch.
        '''
        open(filename+".csv", 'w')
        out = csv.writer(filename)
        for row in data:
            out.writerow(row)

    def getTable(self, tableName, columns=None, constraints=None):
        '''
        Emits data from named table as a list of tuples. Accepts array of
        column names matched to array of constraints, constructed as WHERE
        qualifiers in the SQL query.
        '''
        pass


if __name__ == '__main__':

    te = TableExporter("tables")
