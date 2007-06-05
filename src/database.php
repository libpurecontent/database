<?php

/*
 * Coding copyright Martin Lucas-Smith, University of Cambridge, 2003-6
 * Version 1.3.6
 * Distributed under the terms of the GNU Public Licence - www.gnu.org/copyleft/gpl.html
 * Requires PHP 4.1+ with register_globals set to 'off'
 * Download latest from: http://download.geog.cam.ac.uk/projects/database/
 */


# Class containing basic generalised database manipulation functions for PDO
class database
{
	# Global class variables
	var $connection = NULL;
	
	
	# Function to connect to the database
	function database ($hostname, $username, $password, $database = NULL, $vendor = 'mysql', $logFile = false, $userForLogging = false)
	{
		# Assign the user for logging
		$this->logFile = $logFile;
		$this->userForLogging = $userForLogging;
		
		# Make attributes available for querying by calling applications
		$this->hostname = $hostname;
		$this->vendor = $vendor;
		
		# Connect to the database and return the status
		$dsn = "{$vendor}:host={$hostname}" . ($database ? ";dbname={$database}" : '');
		try {
			$this->connection = new PDO ($dsn, $username, $password);
		} catch (PDOException $e) {
			return false;
		}
	}
	
	
	# Function to disconnect from the database
	function close ()
	{
		# Close the connection
		$this->connection = NULL;
	}
	
	
	# Function to execute a generic SQL query
	function query ($query)
	{
		# Connect to the database and return the status
		try {
			$this->connection->query ($query);
		} catch (PDOException $e) {
			return false;
		}
		
		# Return success
  		return true;
	}
	
	
	# Function to execute a generic SQL query
	function execute ($query, $debug = false)
	{
		# Show the query if debugging
		if ($debug) {
			echo $query . "<br />";
		}
		
		# Connect to the database and return the status
		try {
			$rows = $this->connection->exec ($query);
		} catch (PDOException $e) {
			if ($debug) {echo $e;}
			return false;
		}
		
		# Return the number of affected rows
  		return $rows;
	}
	
	
	# Function to get the data where only one item will be returned; this function has the same signature as getData
	function getOne ($query, $associative = false, $keyed = true)
	{
		# Get the data
		$data = $this->getData ($query, $associative, $keyed);
		
		# Ensure that only one item is returned
		if (count ($data) !== 1) {return false;}
		
		# Return the data
		#!# This could be unset if it's associative
		return $data[0];
	}
	
	
	# Function to get data from an SQL query and return it as an array
	function getData ($query, $associative = false, $keyed = true)
	{
		# Create an empty array to hold the data
		$data = array ();
		
		# Assign the query
		if (!$statement = $this->connection->query ($query)) {
			return $data;
		}
		
		# Set fetch mode
		$mode = ($keyed ? PDO::FETCH_ASSOC : PDO::FETCH_NUM);
		$statement->setFetchMode ($mode);
		
		# Loop through each row and add the data to it
		while ($row = $statement->fetch ()) {
			$data[] = $row;
		}
		#!# Use fetchAll instead with the relevant constant(s) ?
//		$data = $statement->fetchAll ();	// Doesn't seem to work
		
		# Reassign the keys to being the unique field's name, in associative mode
		if ($associative) {
			
			# Split the associative into the database and table name
			if (strpos ($associative, '.') !== false) {
				list ($database, $table) = explode ('.', $associative, 2);
			}
			
			# Get the unique field name, or return as non-keyed data
			if (!$uniqueField = $this->getUniqueField ($database, $table)) {
				return $data;
			}
			
			# Re-key with the field name
			$newData = array ();
			foreach ($data as $key => $attributes) {
				$newData[$attributes[$uniqueField]] = $attributes;
			}
			
			# Entirely replace the dataset; doing on a key-by-key basis doesn't work because the auto-generated keys can clash with real id key names
			$data = $newData;
		}
		
		# Return the array
		return $data;
	}
	
	
	# Function to get fields
	function getFields ($database, $table)
	{
		# Get the data
		$query = "SHOW FULL FIELDS FROM {$database}.{$table};";
		$data = $this->getData ($query);
		
		# Convert the field name to be the key name
		$fields = array ();
		foreach ($data as $key => $attributes) {
			$fields[$attributes['Field']] = $attributes;
		}
		
		# Return the result
		return $fields;
	}
	
	
	# Function to get the unique field name
	function getUniqueField ($database, $table, $fields = false)
	{
		# Get the fields if not already supplied
		if (!$fields) {$fields = $this->getFields ($database, $table);}
		
		# Loop through to find the unique one
		foreach ($fields as $field => $attributes) {
			if ($attributes['Key'] == 'PRI') {
				return $field;
			}
		}
		
		# Otherwise return false, indicating no unique field
		return false;
	}
	
	
	# Function to get field names
	function getFieldNames ($database, $table, $fields = false)
	{
		# Get the fields if not already supplied
		if (!$fields) {$fields = $this->getFields ($database, $table);}
		
		# Get the array keys of the fields
		return array_keys ($fields);
	}
	
	
	# Function to obtain a list of databases on the server
	function getDatabases ($removeReserved = true)
	{
		# Get the data
		$query = "SHOW DATABASES;";
		$data = $this->getData ($query);
		
		# Define reserved databases
		$reserved = array ('information_schema', 'mysql');
		
		# Rearrange
		$databases = array ();
		foreach ($data as $index => $attributes) {
			if ($removeReserved && in_array ($attributes['Database'], $reserved)) {continue;}
			$databases[] = $attributes['Database'];
		}
		
		# Return the data
		return $databases;
	}
	
	
	# Function to obtain a list of tables in a database
	function getTables ($database)
	{
		# Get the data
		$query = "SHOW TABLES FROM {$database};";
		$data = $this->getData ($query);
		
		# Rearrange
		$tables = array ();
		foreach ($data as $index => $attributes) {
			$tables[] = $attributes["Tables_in_{$database}"];
		}
		
		# Return the data
		return $tables;
	}
	
	
	# Function to get the ID generated from the previous insert operation
	function getLatestId ()
	{
		# Return the latest ID
		return $this->connection->lastInsertId ();
	}
	
	
	# Function to clean all data
	function escape ($uncleanData, $cleanKeys = true)
	{
		# If the data is an string, return it directly
		if (is_string ($uncleanData)) {
			return addslashes ($uncleanData);
		}
		
		# Loop through the data
		$data = array ();
		foreach ($uncleanData as $key => $value) {
			if ($cleanKeys) {$key = $this->escape ($key);}
			$data[$key] = $this->escape ($value);
		}
		
		# Return the data
		return $data;
	}
	
	
	# Function to deal with quotation, i.e. escaping AND adding quotation marks around the item
	function quote ($data)
	{
		# Strip slashes if necessary
		if (get_magic_quotes_gpc ()) {
			$data = stripslashes ($data);
		}
		
		# Special case a timestamp indication as unquoted SQL
		if ($data == 'NOW()') {
			return $data;
		}
		
		# Quote string by calling the PDO quoting method
		$data = $this->connection->quote ($data);
		
		# Return the data
		return $data;
	}
	
	
	# Function to construct and execute a SELECT statement
	function select ($database, $table, $conditions = array (), $columns = array (), $associative = true, $orderBy = false/*, $getOne = false*/)
	{
		# Construct the WHERE clause
		$where = '';
		if ($conditions) {
			$where = array ();
			foreach ($conditions as $key => $value) {
				$where[] = $key . "=" . $this->quote ($value);
			}
			$where = ' WHERE ' . implode (' AND ', $where);
		}
		
		# Construct the columns part; if the key is numeric, assume it's not a key=>value pair, but that the value is the fieldname
		$what = '*';
		if ($columns) {
			$what = array ();
			foreach ($columns as $key => $value) {
				if (is_numeric ($key)) {
					$what[] = $value;
				} else {
					$what[] = "{$key} AS {$value}";
				}
			}
			$what = implode (',', $what);
		}
		
		# Construct the ordering
		$orderBy = ($orderBy ? " ORDER BY {$orderBy}" : '');
		
		# Assemble the query
		$query = "SELECT {$what} FROM `{$database}`.`{$table}`{$where}{$orderBy};\n";
		
		# Get the data
		$data = $this->getData ($query, ($associative ? "{$database}.{$table}" : false));
		
		# Return the data
		return $data;
	}
	
	
	# Function to construct and execute an INSERT statement
	function insert ($database, $table, $data, $onDuplicateKeyUpdate = false, $safe = false, $showErrors = false)
	{
		# Ensure the data is an array and that there is data
		if (!is_array ($data) || !$data) {return false;}
		
		# Assemble the field names
		$fields = '`' . implode ('`,`', array_keys ($data)) . '`';
		
		# Assemble the values
		foreach ($data as $key => $value) {
			$values[] = $this->quote ($value);
		}
		$values = implode (',', $values);
		
		# Allow for an optional ON DUPLICATE KEY UPDATE clause - see: http://dev.mysql.com/doc/refman/5.1/en/insert-on-duplicate.html
		#!# Quoting?
		if ($onDuplicateKeyUpdate) {
			if ($onDuplicateKeyUpdate === true) {
				foreach ($data as $key => $value) {
					$clauses[] = "`{$key}`=VALUES(`{$key}`)";
				}
				$onDuplicateKeyUpdate = ' ON DUPLICATE KEY UPDATE ' . implode (',', $clauses);
			} else {
				$onDuplicateKeyUpdate = " ON DUPLICATE KEY UPDATE {$onDuplicateKeyUpdate}";
			}
		} else {
			$onDuplicateKeyUpdate = '';
		}
		
		# Assemble the query
		$query = "INSERT INTO {$database}.{$table} ({$fields}) VALUES ({$values}){$onDuplicateKeyUpdate};\n";
		
		# In safe mode, only show the query
		if ($safe) {
			echo $query . "<br />";
			return true;
		}
		
		# Execute the query
		$rows = $this->execute ($query, $showErrors);
		
		# Determine the result
		$result = ($rows !== false);
		
		# Log the change
		$this->logChange ($query, $result);
		
		# Return the result
		return $result;
	}
	
	
	# Function to construct and execute an UPDATE statement
	function update ($database, $table, $data, $conditions = array (), $safe = false)
	{
		# Ensure the data is an array and that there is data
		if (!is_array ($data) || !$data) {return false;}
		
		# Assemble the pairs
		foreach ($data as $key => $value) {
			$updates[] = "`{$key}`=" . $this->quote ($value);
			
			# Make the condition be that the first item is the key if nothing specified
			if (!$conditions) {
				$conditions[$key] = $value;
			}
		}
		$updates = implode (', ', $updates);
		
		# Construct the WHERE clause
		$where = array ();
		foreach ($conditions as $key => $value) {
			$where[] = '`' . $key . "` = " . $this->quote ($value);
		}
		$where = implode (' AND ', $where);
		
		# Assemble the query
		$query = "UPDATE {$database}.{$table} SET {$updates} WHERE {$where};\n";
		
		# In safe mode, only show the query
		if ($safe) {
			echo $query . "<br />";
			return true;
		}
		
		# Execute the query
		$rows = $this->execute ($query);
		
		# Determine the result
		$result = ($rows !== false);
		
		# Log the change
		$this->logChange ($query, $result);
		
		# Return whether the operation failed or succeeded
		return $result;
	}
	
	
	# Function to log a change
	function logChange ($query, $result)
	{
		# End if logging disabled
		if (!$this->logFile) {return false;}
		
		# End if file is not writable
		if (!is_writable ($this->logFile)) {return false;}
		
		# Create the log entry
		$logEntry = '/* ' . ($result ? 'Success' : 'Failure') . ' ' . date ('Y-m-d H:i:s') . ' by ' . $this->userForLogging . ' */ ' . str_replace ("\r\n", '\\r\\n', $query);
		
		# Log the change
		file_put_contents ($this->logFile, $logEntry, FILE_APPEND);
	}
	
	
	# Function to delete data
	function delete ($database, $table, $conditions)
	{
		# Ensure the data is an array and that there is data
		if (!is_array ($conditions) || !$conditions) {return false;}
		
		# Construct the WHERE clause
		$where = '';
		if ($conditions) {
			$where = array ();
			foreach ($conditions as $key => $value) {
				$where[] = $key . "=" . $this->quote ($value);
			}
			$where = ' WHERE ' . implode (' AND ', $where);
		}
		
		# Assemble the query
		$query = "DELETE FROM {$database}.{$table}{$where};\n";
		
		# Execute the query
		$result = $this->execute ($query);
		
		# Log the change
		$this->logChange ($query, $result);
		
		# Return whether the operation failed or succeeded
		return $result;
	}
	
	
	# Function to get field descriptions as a simple associative array
	function getHeadings ($database, $table)
	{
		# Get the fields
		$fields = $this->getFields ($database, $table);
		
		# Rearrange the data
		$headings = array ();
		foreach ($fields as $field => $attributes) {
			$headings[$field] = $attributes['Comment'];
		}
		
		# Return the headings
		return $headings;
	}
	
	
	# Function to get table metadata
	function getTableStatus ($database, $table, $getOnly = false /*array ('Comment')*/)
	{
		# Define the query
		$query = "SHOW TABLE STATUS FROM `{$database}` LIKE '{$table}';";
		
		# Get the results
		$data = $this->getOne ($query);
		
		# If only needing certain columns, return only those
		if ($getOnly && is_array ($getOnly)) {
			foreach ($getOnly as $field) {
				if (isSet ($data[$field])) {
					$attributes[$field] = $data[$field];
				}
			}
		} else {
			$attributes = $data;
		}
		
		# Return the results
		return $attributes;
	}
}

?>