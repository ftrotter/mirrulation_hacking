# A script that will decompose a graph into its parts using the igraph library

import igraph
from trottertools.WriteOnceDict import WriteOnceDict
from trottertools.myDBTable import myDBTable
from trottertools.mySQLh import mySQLh
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
from datetime import datetime
import json
import pureyaml
import pymysql.cursors 
import sys
import resource
import os.path


def memory_usage_resource():
    rusage_denom = 1024
    if sys.platform == 'darwin':
        # ... it seems that in OSX the output is different units ...
        rusage_denom = rusage_denom * rusage_denom
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / rusage_denom
    return str(mem)

mysql_config_file = open(os.path.dirname(os.path.abspath(__file__)) + "/util/db.yaml");
mysql_config = pureyaml.load(mysql_config_file.read())

db = pymysql.connect(	host=mysql_config['mysql_host'],
			user=mysql_config['mysql_user'],
			password=mysql_config['mysql_password'],
			db=mysql_config['mysql_database'],
			charset='utf8mb4',
			cursorclass=pymysql.cursors.DictCursor)

try:
	sql = []
	with db.cursor() as cursor:
		#the reason I put an 'n' in front of the node names is that this prevents
		# igraph from using the npi numbers as node identifiers below...
		#there is obviously a better way to do this.	

		if(len(sys.argv) != 4):
			print('I need three arguments. the name of the .sql file that loads the graph, the database and table name to put the output into');
			sys.exit()


		#these are the default source and destination table...
		dest_db = 'munge_esse'
		dest_table = 'SG'
		#accept a source and destination table from the command line... if it is given...
		if len(sys.argv) > 1:
			sql_file_name = sys.argv[1]
			if len(sys.argv) > 2:
				dest_db = sys.argv[2]
				if len(sys.argv) > 3:
					dest_table = sys.argv[3]

		print('sql_file_name:' + str(sql_file_name) + ' dest_db:' + str(dest_db) + ' dest_table:' + dest_table);

		this_sql = open(sql_file_name, 'r').read()

		#drop the output table
		drop_sql = "DROP TABLE IF EXISTS %(DB)s.%(DEST_TABLE)s" % {"DEST_TABLE": dest_table, "DB": dest_db}
		cursor.execute(drop_sql);
		#create a new output table
		create_sql = "CREATE TABLE %(DB)s.%(DEST_TABLE)s (`node` varchar(255) DEFAULT NULL,`node_name` varchar(255), `node_type` varchar(255), `esse_id` int(11) NOT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1;" % {"DEST_TABLE": dest_table, 'DB': dest_db}
		cursor.execute(create_sql);
		#index the output table
		index_sql = "ALTER TABLE %(DB)s.%(DEST_TABLE)s ADD INDEX (`node`,`esse_id`);" % {"DEST_TABLE": dest_table, 'DB': dest_db}
		cursor.execute(index_sql);

		print('Destination table blanked, created and indexed. Running query..');

		all_nodes = {}
		all_edges = []


		cursor.execute(this_sql)
		results = cursor.fetchall()
		
		g = igraph.Graph()

		i = 0;
		print('processing data...')
		print('Memory usage: ' + memory_usage_resource())
		for this_row in results:
			i += 1
			all_nodes[this_row['node_a']] = {'node_name': this_row['node_a_name'], 'node_type': this_row['node_a_type'], 'node_id': this_row['node_a']}
			all_nodes[this_row['node_b']] = {'node_name': this_row['node_b_name'], 'node_type': this_row['node_b_type'], 'node_id': this_row['node_b']}
			all_edges.append((this_row['node_a_name'], this_row['node_b_name']))
			if((i % 10000) == 0):
				print('.: '+str(i))

		print('Memory usage: ' + memory_usage_resource())
		i = 0;
		for this_node_name in all_nodes:
			i += 1
			this_node = all_nodes[this_node_name]
			my_node_type = this_node['node_type']
			my_node_name = this_node['node_name']
			g.add_vertex(my_node_name, node_type = my_node_type)
			if((i % 10000) == 0):
				print('n: '+str(i))
		
		print('Memory usage: ' + memory_usage_resource())
		print('Starting bulk edge import')
		i = 0;
		print(g);
		# for whatever reason, this function does not seem capable of 
		# getting integers as output variables... it trys to use them 	
		# as the internal numbers that idetify nodes... and crashes
		# this is the reason for putting the 'n' in front of the node names 
		g.add_edges(all_edges)

		#Graph is fully built in memory at this point...

		print('')
		print('Memory usage: ' + memory_usage_resource())
		print('Starting decomposition')

		sub_graph_list = g.decompose()

		print('')
		print('Memory usage: ' + memory_usage_resource())
		print('Total subgraphs:')
		print(len(sub_graph_list))

		for esse_id, this_sub_graph in enumerate(sub_graph_list):
			#What I really wish I could do is have a different data field instead of the name field
			#That way when I wrote the data out I would not have an 'n' in from of anything
			#or I could just switch the edge tracking logic above to use the internal graph indexing
			#would be trivial to do in php...
			#this section really should write out the esse id, nodename AND nodetype
			#but this command (which I cannot remember writing) would have to be replaced with something
			#that could get more data out, and then the loop would need to be more complex
			#simple enough to do if I were more fluent in basic python syntax
			#print(index)
			insert_sql = ''
			comma = ''
			insert_sql_start = "INSERT IGNORE INTO %(DB)s.%(DEST_TABLE)s VALUES " % {'DB': dest_db, 'DEST_TABLE': dest_table}
			insert_sql = insert_sql_start
			i = 0
			for this_node in this_sub_graph.vs:
				i = i + 1
				#insert_sql += comma + "( '"+ str(this_node['node_id']) + "', '" + str(this_node['name']) + "','" + str(this_node['node_type']) + "', '" + str(esse_id) + "')"
				# This does not work, need another way to access the node_id...
				insert_sql += comma + "( NULL, '" + str(this_node['name']) + "','" + str(this_node['node_type']) + "', '" + str(esse_id) + "')"
				comma = ','
				if(i > 1000):
					#lets do a write to the db.
					cursor.execute(insert_sql)
					#now lets start the whole sql insert from scratch for the next 1000
					insert_sql = insert_sql_start
					comma = ''
					i = 0
					sys.stdout.write('w')

			#all done with the loop, lets run the last insert_sql
			cursor.execute(insert_sql)
			sys.stdout.write('wl')

		#now lets get rid of those n in the node names
		clean_sql = "UPDATE %(DB)s.%(DEST_TABLE)s SET node = SUBSTR(node_name,3), node_name = SUBSTR(node_name,3)" % {'DB': dest_db, 'DEST_TABLE': dest_table}
		cursor.execute(clean_sql)
		
finally:

	print('')
	db.close()
	print('all done. finally over')




