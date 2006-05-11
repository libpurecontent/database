<?php

/*
 * Coding copyright Martin Lucas-Smith, University of Cambridge, 2003-6
 * Version 1.1.0
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
	function database ($hostname, $username, $password, $database = NULL, $vendor = 'mysql')
	{
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
	function execute ($query)
	{
		# Connect to the database and return the status
		try {
			$rows = $this->connection->exec ($query);
		} catch (PDOException $e) {
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
	function getUniqueField ($database, $table)
	{
		# Get the fields
		$fields = $this->getFields ($database, $table);
		
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
	function getFieldNames ($database, $table)
	{
		# Get the array keys of the fields
		return array_keys ($this->getFields ($database, $table));
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
	
	
	# Function to modify the sort order of the data to take account of opening non-alphanumeric characters
	#!# Really this ought to be done in the SQL, but no way yet found to remove "'", 'a', 'an', or 'the' from the start
	function modifyDataSorting ($data, $columnName = 'title')
	{
		# Define strings to be ignored in the sort-order if they are at the start
		$ignoreStrings = array ("'", 'A ', 'An ', 'The ');
		
		# Loop through the data
		foreach ($data as $key => $value) {
			$sortedData[$key] = $value;
			
			# Determine the corrected item
			$newColumn = $value[$columnName];
			foreach ($ignoreStrings as $ignoreString) {
				if (substr ($newColumn, 0, strlen ($ignoreString)) == $ignoreString) {
					$newColumn = substr ($newColumn, strlen ($ignoreString));
				}
			}
			
			# Add in the new additional title column
			$sortedData[$key]["_{$columnName}"] = $newColumn;
		}
		
		# Resort the data by the specified column name by creating an anonymous function for the purpose
		$sortFunction = create_function ('$a,$b', 'return strcmp ($a["_' . $columnName . '"], $b["_' . $columnName . '"]);');
		usort ($sortedData, $sortFunction);
		
		# Discard the additional column name
		foreach ($sortedData as $key => $value) {
			unset ($sortedData[$key]["_{$columnName}"]);
		}
		
		# Return the data
		return $sortedData;
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
		
		# Quote string by calling the PDO quoting method
		$data = $this->connection->quote ($data);
		
		# Return the data
		return $data;
	}
	
	
	# Function to construct and execute a SELECT statement
	function select ($database, $table, $data = array (), $columns = array (), $associative = true)
	{
		# Construct the WHERE clause
		$where = '';
		if ($data) {
			$where = array ();
			foreach ($data as $key => $value) {
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
		
		# Assemble the query
		$query = "SELECT {$what} FROM {$table}{$where};\n";
		
		# Get the data
		$data = $this->getData ($query, ($associative ? "{$database}.{$table}" : false));
		
		# Return the data
		return $data;
	}
	
	
	# Function to construct and execute an INSERT statement
	function insert ($database, $table, $data)
	{
		# Ensure the data is an array and that there is data
		if (!is_array ($data) || !$data) {return false;}
		
		# Assemble the field names
		$fields = implode (',', array_keys ($data));
		
		# Assemble the values
		foreach ($data as $key => $value) {
			$values[] = $this->quote ($value);
		}
		$values = implode (',', $values);
		
		# Assemble the query
		$query = "INSERT INTO {$database}.{$table} ({$fields}) VALUES ({$values});\n";
		
		# Execute the query
		$rows = $this->execute ($query);
		
		# Determine the result
		$result = ($rows !== false);
		
		# Return the result
		return $result;
	}
	
	
	# Function to construct and execute an UPDATE statement
	function update ($database, $table, $data, $conditions = array ())
	{
		# Ensure the data is an array and that there is data
		if (!is_array ($data) || !$data) {return false;}
		
		# Assemble the pairs
		foreach ($data as $key => $value) {
			$updates[] = "{$key}=" . $this->quote ($value);
			
			# Make the condition be that the first item is the key if nothing specified
			if (!$conditions) {
				$conditions[$key] = $value;
			}
		}
		$updates = implode (', ', $updates);
		
		# Construct the WHERE clause
		$where = array ();
		foreach ($conditions as $key => $value) {
			$where[] = $key . "=" . $this->quote ($value);
		}
		$where = implode (' AND ', $where);
		
		# Assemble the query
		$query = "UPDATE {$database}.{$table} SET {$updates} WHERE {$where};\n";
		
		# Execute the query
		$rows = $this->execute ($query);
		
		# Determine the result
		$result = ($rows !== false);
		
		# Return whether the operation failed or succeeded
		return $result;
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
		$query = "DELETE FROM {$table}{$where};\n";
		
		# Execute the query
		$result = $this->execute ($query);
		
		# Return whether the operation failed or succeeded
		return $result;
	}
}

?>