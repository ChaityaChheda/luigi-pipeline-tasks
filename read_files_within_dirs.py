import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs as hdfs
import luigi.contrib.mysqldb as mysql
import os
import subprocess
from datetime import datetime


class TestTask(luigi.Task):

    rundate = luigi.DateParameter(default=datetime.now().date())
    table = "test"
    host = "localhost:3306"
    db = "testdb"
    user = "read_only"
    pw = "password"

    def input(self):

	    """
	    Provides the input directories. the path of directories are of the form /in/'directory'/'filename'.
	    It opens all the directories within \in and provides all the files in those directories as input.	
	    """
        dir_in = '/in'
        args = "hdfs dfs -ls "+dir_in+" | awk '{print $8}'"
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        s_output, s_err = proc.communicate()
        result = s_output.split()
        
        listf =[]
        for dir in result:
            args = "hdfs dfs -ls "+dir+" | awk '{print $8}'"
            proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

            s_output, s_err = proc.communicate()
            listf.extend(s_output.split())

        return [hdfs.HdfsTarget(str) for str in listf ]
            

    def output(self):

	    """
	    Provides the MySQL target as the outuput target.
	    """
        return mysql.MySqlTarget(
            host=self.host,
            database=self.db,
            user=self.user,
            password=self.pw,
            table=self.table,
            update_id=str(self.rundate)
        )

    @property
    def columns(self):
        """
        Provides definition of columns.
        If only writing to existing tables, then columns() need only provide a list of names.
        If also needing to create the table, then columns() should define a list of
        (name, definition) tuples. For example, ('first_name', 'VARCHAR(255)').
		In this example we create columns with column names word word1 word2 with data type as VARCHAR. 
        """
        cols = [('word','VARCHAR(255)'),('word1','VARCHAR(255)'),('word2','VARCHAR(255)')]
        return cols
	#raise NotImplementedError


    def create_table(self, connection):
        """
        Creates the target table, if it does not exist.
        Use the provided connection object for setting
        up the table in order to create the table and insert data
        """ 
        if len(self.columns[0]) != 2:
        # only names of columns specified, no types
        	raise NotImplementedError(
           		"create_table() not implemented for %r and columns types not specified"
                % self.table
            )

        # Assumes that columns are specified as (name, definition) tuples
        columns = []

        columns.extend(self.columns)


        coldefs = ','.join('{name} {definition}'.format(name=name, definition=definition) for name, definition in columns)
        query = "CREATE TABLE IF NOT EXISTS {table} ({coldefs})".format(
            table=self.table, coldefs=coldefs
        )
        
        connection.cursor().execute(query)


    def run(self):
        connection = self.output().connect()
        cursor = connection.cursor()
        self.create_table(connection)        

        #Read every file from input()
        for filename in self.input():
            with filename.open('r') as file:
                #reading every line from the file with column values separated by ','.
                #You ca change it to tab("	") or space(" ") delimated lines as you wish.
                for line in file:
                    data = line.strip().split(",")
                    cursor.execute("""INSERT INTO test VALUES (%s,%s, %s)""", (data[0], data[1], data[2]))

        cursor.close()
        connection.commit()
        connection.close()
