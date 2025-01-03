"""
clustering_comments_graphs_split.py

Purpose:
    This script processes a graph of related comments by decomposing it into distinct subgraphs (clusters).
    It reads edge and node data from a MySQL database, constructs an igraph Graph object, decomposes it 
    into connected components, and writes the results back to a specified database table.

Requirements:
    - Python libraries: igraph, pymysql, dotenv, sqlalchemy, pureyaml
    - MySQL database connection
    - Configuration file: util/db.yaml containing MySQL credentials
    - Input SQL file that defines the graph structure

Usage:
    python clustering_comments_graphs_split.py <sql_file> <dest_db> <dest_table>

Arguments:
    sql_file    : Path to SQL file containing the query that loads the graph data
    dest_db     : Destination database name (default: 'munge_esse')
    dest_table  : Destination table name (default: 'SG')

Input SQL Requirements:
    The SQL query should return rows with the following columns:
    - node_a: First node identifier
    - node_b: Second node identifier
    - node_a_name: Name of first node
    - node_b_name: Name of second node
    - node_a_type: Type of first node
    - node_b_type: Type of second node

Output Table Structure:
    The script creates a table with the following columns:
    - node: VARCHAR(255) - Node identifier
    - node_name: VARCHAR(255) - Node name
    - node_type: VARCHAR(255) - Node type
    - esse_id: INT(11) - Subgraph identifier

Memory Management:
    The script includes memory usage tracking using the resource module.
    Progress indicators are printed every 10,000 records processed.

Process Flow:
    1. Validates command line arguments
    2. Sets up destination database table
    3. Reads graph data from source query
    4. Constructs igraph Graph object
    5. Decomposes graph into connected components
    6. Writes subgraph data to destination table
    7. Cleans up node names (removes 'n' prefix)
"""

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
    """
    Get current memory usage in a platform-independent way.
    
    Returns:
        str: Memory usage in appropriate units (KB on Linux, MB on macOS)
    """
    rusage_denom = 1024
    if sys.platform == 'darwin':
        # On macOS, ru_maxrss is in bytes, need to convert to MB
        rusage_denom = rusage_denom * rusage_denom
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / rusage_denom
    return str(mem)

# Load MySQL configuration from YAML file
mysql_config_file = open(os.path.dirname(os.path.abspath(__file__)) + "/util/db.yaml");
mysql_config = pureyaml.load(mysql_config_file.read())

# Initialize database connection
db = pymysql.connect(	host=mysql_config['mysql_host'],
			user=mysql_config['mysql_user'],
			password=mysql_config['mysql_password'],
			db=mysql_config['mysql_database'],
			charset='utf8mb4',
			cursorclass=pymysql.cursors.DictCursor)

try:
	sql = []
	with db.cursor() as cursor:
		# Validate command line arguments
		if(len(sys.argv) != 4):
			print('I need three arguments. the name of the .sql file that loads the graph, the database and table name to put the output into');
			sys.exit()

		# Set default and override destination settings
		dest_db = 'munge_esse'
		dest_table = 'SG'
		if len(sys.argv) > 1:
			sql_file_name = sys.argv[1]
			if len(sys.argv) > 2:
				dest_db = sys.argv[2]
				if len(sys.argv) > 3:
					dest_table = sys.argv[3]

		print('sql_file_name:' + str(sql_file_name) + ' dest_db:' + str(dest_db) + ' dest_table:' + dest_table);

		# Read SQL query from file
		this_sql = open(sql_file_name, 'r').read()

		# Initialize destination table
		drop_sql = "DROP TABLE IF EXISTS %(DB)s.%(DEST_TABLE)s" % {"DEST_TABLE": dest_table, "DB": dest_db}
		cursor.execute(drop_sql);
		create_sql = "CREATE TABLE %(DB)s.%(DEST_TABLE)s (`node` varchar(255) DEFAULT NULL,`node_name` varchar(255), `node_type` varchar(255), `esse_id` int(11) NOT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1;" % {"DEST_TABLE": dest_table, 'DB': dest_db}
		cursor.execute(create_sql);
		index_sql = "ALTER TABLE %(DB)s.%(DEST_TABLE)s ADD INDEX (`node`,`esse_id`);" % {"DEST_TABLE": dest_table, 'DB': dest_db}
		cursor.execute(index_sql);

		print('Destination table blanked, created and indexed. Running query..');

		# Data structures for graph construction
		all_nodes = {}
		all_edges = []

		# Execute source query and fetch results
		cursor.execute(this_sql)
		results = cursor.fetchall()
		
		# Initialize igraph Graph object
		g = igraph.Graph()

		# Process query results into graph structure
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

		# Add vertices to graph
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
		# Add edges to graph
		# Note: node names need 'n' prefix to prevent igraph from using numeric IDs as internal identifiers
		g.add_edges(all_edges)

		print('')
		print('Memory usage: ' + memory_usage_resource())
		print('Starting decomposition')

		# Decompose graph into connected components
		sub_graph_list = g.decompose()

		print('')
		print('Memory usage: ' + memory_usage_resource())
		print('Total subgraphs:')
		print(len(sub_graph_list))

		# Process and store subgraphs
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
			# Process nodes in current subgraph
			for this_node in this_sub_graph.vs:
				i = i + 1
				insert_sql += comma + "( NULL, '" + str(this_node['name']) + "','" + str(this_node['node_type']) + "', '" + str(esse_id) + "')"
				comma = ','
				# Batch inserts in groups of 1000
				if(i > 1000):
					cursor.execute(insert_sql)
					insert_sql = insert_sql_start
					comma = ''
					i = 0
					sys.stdout.write('w')

			# Execute final batch for current subgraph
			cursor.execute(insert_sql)
			sys.stdout.write('wl')

		# Clean up node names by removing 'n' prefix
		clean_sql = "UPDATE %(DB)s.%(DEST_TABLE)s SET node = SUBSTR(node_name,3), node_name = SUBSTR(node_name,3)" % {'DB': dest_db, 'DEST_TABLE': dest_table}
		cursor.execute(clean_sql)
		
finally:
	print('')
	db.close()
	print('all done. finally over')
