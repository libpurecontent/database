<?php

/*
 * Coding copyright Martin Lucas-Smith, University of Cambridge, 2003-9
 * Version 1.6.13
 * Distributed under the terms of the GNU Public Licence - www.gnu.org/copyleft/gpl.html
 * Requires PHP 4.1+ with register_globals set to 'off'
 * Download latest from: http://download.geog.cam.ac.uk/projects/database/
 */


# Class containing basic generalised database manipulation functions for PDO
class database
{
	# Global class variables
	var $connection = NULL;
	var $query = NULL;
	
	# Function to connect to the database
	function database ($hostname, $username, $password, $database = NULL, $vendor = 'mysql', $logFile = false, $userForLogging = false, $unicode = true)
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
		
		# Set transfers to UTF-8
		if ($unicode) {
			$this->execute ("SET NAMES 'utf8'");
			// # The following is a more portable version that could be used instead
			//$charset = $this->getOne ("SHOW VARIABLES LIKE 'character_set_database';");
			//$this->execute ("SET NAMES '" . $charset['Value'] . "';");
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
		# Global the query
		$this->query = $query;
		
		# Connect to the database and return the status
		try {
			$this->connection->query ($this->query);
		} catch (PDOException $e) {
			return false;
		}
		
		# Return success
  		return true;
	}
	
	
	# Function to execute a generic SQL query
	function execute ($query, $debug = false)
	{
		# Global the query
		$this->query = $query;
		
		# Show the query if debugging
		if ($debug) {
			echo $this->query . "<br />";
		}
		
		# Connect to the database and return the status
		try {
			$rows = $this->connection->exec ($this->query);
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
		# Global the query
		$this->query = $query;
		
		# Get the data
		$data = $this->getData ($this->query, $associative, $keyed);
		
		# Ensure that only one item is returned
		if (count ($data) > 1) {return NULL;}
		if (count ($data) !== 1) {return false;}
		
		# Return the data
		#!# This could be unset if it's associative
		return $data[0];
	}
	
	
	# Function to get the data where either (i) only one column per item will be returned, resulting in index => value, or (ii) two columns are returned, resulting in col1 => col2
	function getPairs ($query, $trimAndUnique = true)
	{
		# Global the query
		$this->query = $query;
		
		# Get the data
		$data = $this->getData ($this->query, false, $keyed = false);
		
		# Arrange the data into key/value pairs
		$pairs = array ();
		foreach ($data as $key => $item) {
			foreach ($item as $field => $value) {
				if (count ($item) == 1) {
					$pairs[$key] = ($trimAndUnique ? trim ($value) : $value);
				} else {
					$pairs[$item[0]] = ($trimAndUnique ? trim ($item[1]) : $item[1]);
				}
				break;
			}
		}
		
		# Unique the data if necessary
		if ($trimAndUnique) {$pairs = array_unique ($pairs);}
		
		# Return the data
		return $pairs;
	}
	
	
	# Function to get data from an SQL query and return it as an array; $associative should be false or a string "$database.$table" (which reindexes the data to the field containing the unique key)
	function getData ($query, $associative = false, $keyed = true)
	{
		# Global the query
		$this->query = $query;
		
		# Create an empty array to hold the data
		$data = array ();
		
		# Assign the query
		if (!$statement = $this->connection->query ($this->query)) {
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
				#!# This causes offsets if the key is not amongst the fields requested
				$newData[$attributes[$uniqueField]] = $attributes;
			}
			
			# Entirely replace the dataset; doing on a key-by-key basis doesn't work because the auto-generated keys can clash with real id key names
			$data = $newData;
		}
		
		# Return the array
		return $data;
	}
	
	
	# Function to count the number of records
	function getTotal ($database, $table, $restrictionSql = '')
	{
		# Check that the table exists
		$tables = $this->getTables ($database);
		if (!in_array ($table, $tables)) {return false;}
		
		# Get the total
		$this->query = "SELECT COUNT(*) AS total FROM `{$database}`.`{$table}` {$restrictionSql};";
		$data = $this->getOne ($this->query);
		
		# Return the value
		return $data['total'];
	}
	
	
	# Function to get fields
	function getFields ($database, $table)
	{
		# Get the data
		$this->query = "SHOW FULL FIELDS FROM `{$database}`.`{$table}`;";
		$data = $this->getData ($this->query);
		
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
	
	
	# Function to get field descriptions as a simple associative array
	function getHeadings ($database, $table, $fields = false, $useFieldnameIfEmpty = true, $commentsAsHeadings = true)
	{
		# Get the fields if not already supplied
		if (!$fields) {$fields = $this->getFields ($database, $table);}
		
		# Rearrange the data
		$headings = array ();
		foreach ($fields as $field => $attributes) {
			$headings[$field] = ((((empty ($attributes['Comment']) && $useFieldnameIfEmpty)) || !$commentsAsHeadings) ? $field : $attributes['Comment']);
		}
		
		# Return the headings
		return $headings;
	}
	
	
	# Function to obtain a list of databases on the server
	function getDatabases ($removeReserved = array ('cluster', 'information_schema', 'mysql'))
	{
		# Get the data
		$this->query = "SHOW DATABASES;";
		$data = $this->getData ($this->query);
		
		# Sort the list
		if ($data) {sort ($data);}
		
		# Rearrange
		$databases = array ();
		foreach ($data as $index => $attributes) {
			if ($removeReserved && in_array ($attributes['Database'], $removeReserved)) {continue;}
			$databases[] = $attributes['Database'];
		}
		
		# Return the data
		return $databases;
	}
	
	
	# Function to obtain a list of tables in a database
	function getTables ($database)
	{
		# Get the data
		$this->query = "SHOW TABLES FROM `{$database}`;";
		$data = $this->getData ($this->query);
		
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
		# End if no data
		if (empty ($uncleanData)) {return $uncleanData;}
		
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
	/* private */ function quote ($data)
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
	
	
	# Function to select the data where only one item will be returned (as per getOne); this function has the same signature as select, except for the default on associative
	function selectOne ($database, $table, $conditions = array (), $columns = array (), $associative = false, $orderBy = false)
	{
		# Get the data
		$data = $this->select ($database, $table, $conditions, $columns, $associative, $orderBy);
		
		# Ensure that only one item is returned
		if (count ($data) > 1) {return NULL;}
		if (count ($data) !== 1) {return false;}
		
		# Return the data
		#!# This could be unset if it's associative
		#!# http://bugs.mysql.com/36824 could result in a value slipping through that is not strictly matched
		return $data[0];
	}
	
	
	# Function to construct and execute a SELECT statement
	function select ($database, $table, $conditions = array (), $columns = array (), $associative = true, $orderBy = false)
	{
		# Construct the WHERE clause
		$where = '';
		if ($conditions) {
			$where = array ();
			foreach ($conditions as $key => $value) {
				$where[] = $key . '=' . $this->quote ($value);
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
		$this->query = "SELECT {$what} FROM `{$database}`.`{$table}`{$where}{$orderBy};\n";
		
		# Get the data
		$data = $this->getData ($this->query, ($associative ? "{$database}.{$table}" : false));
		
		# Return the data
		return $data;
	}
	
	
	# Function to construct and execute an INSERT statement
	function insert ($database, $table, $data, $onDuplicateKeyUpdate = false, $emptyToNull = true, $safe = false, $showErrors = false)
	{
		# Ensure the data is an array and that there is data
		if (!is_array ($data) || !$data) {return false;}
		
		# Assemble the field names
		$fields = '`' . implode ('`,`', array_keys ($data)) . '`';
		
		# Assemble the values
		foreach ($data as $key => $value) {
			if ($emptyToNull && ($value == '')) {$value = NULL;}	// Convert empty to NULL
			$values[] = ($value === NULL ? 'NULL' : $this->quote ($value));
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
		$this->query = "INSERT INTO `{$database}`.`{$table}` ({$fields}) VALUES ({$values}){$onDuplicateKeyUpdate};\n";
		
		# In safe mode, only show the query
		if ($safe) {
			echo $this->query . "<br />";
			return true;
		}
		
		# Execute the query
		$rows = $this->execute ($this->query, $showErrors);
		
		# Determine the result
		$result = ($rows !== false);
		
		# Log the change
		$this->logChange ($this->query, $result);
		
		# Return the result
		return $result;
	}
	
	
	# Function to construct and execute an INSERT statement containing many items
	function insertMany ($database, $table, $dataSet, $onDuplicateKeyUpdate = false, $emptyToNull = true, $safe = false, $showErrors = false)
	{
		# ON DUPLICATE KEY UPDATE is not available in this interface (though has been left in the argument list for API consistency)
		if ($onDuplicateKeyUpdate) {return false;}
		
		# Ensure the data is an array and that there is data
		if (!is_array ($dataSet) || !$dataSet) {return false;}
		
		# Loop through each set of data
		foreach ($dataSet as $index => $data) {
			
			# Ensure the data is an array and that there is data
			if (!is_array ($data) || !$data) {return false;}
			
			# Get the field names
			$fields = array_keys ($data);
			
			# Cache the previous field names and check for consistency, returning false if a different set of field names is found
			if (isSet ($cachedFieldList)) {
				if ($fields !== $cachedFieldList) {	// Enforce field list (including order) consistency across every record
					return false;
				}
			}
			$cachedFieldList = $fields;
			
			# Assemble the field names
			$fields = '`' . implode ('`,`', $fields) . '`';
			
			# Assemble the values
			$values = array ();
			foreach ($data as $key => $value) {
				if ($emptyToNull && ($value == '')) {$value = NULL;}	// Convert empty to NULL
				$values[] = ($value === NULL ? 'NULL' : $this->quote ($value));
			}
			$valuesSet[$index] = implode (',', $values);
		}
		
		# Assemble the query
		$query = "INSERT INTO `{$database}`.`{$table}` ({$fields}) VALUES (" . implode ('),(', $valuesSet) . ");\n";
		
		# Prevent submission of over-long queries
		$maxLength = $this->getOne ("SHOW VARIABLES LIKE 'max_allowed_packet'");
		if (isSet ($maxLength['Value'])) {
			if (strlen ($query) > (int) $maxLength['Value']) {
				return false;
			}
		}
		
		# Assign the global query value; this is done after the getOne query to avoid reallocation
		$this->query = $query;
		
		# In safe mode, only show the query
		if ($safe) {
			echo $this->query . "<br />";
			return true;
		}
		
		# Execute the query
		$rows = $this->execute ($this->query, $showErrors);
		
		# Determine the result
		$result = ($rows !== false);
		
		# Log the change
		$this->logChange ($this->query, $result);
		
		# Return the result
		return $result;
	}
	
	
	# Function to construct and execute an UPDATE statement
	function update ($database, $table, $data, $conditions = array (), $emptyToNull = true, $safe = false)
	{
		# Ensure the data is an array and that there is data
		if (!is_array ($data) || !$data) {return false;}
		
		# Assemble the pairs
		foreach ($data as $key => $value) {
			if ($emptyToNull && ($value == '')) {$value = NULL;}	// Convert empty to NULL
			$useValue = ($value === NULL ? 'NULL' : $this->quote ($value));
			$updates[] = "`{$key}`=" . $useValue;
			
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
		$this->query = "UPDATE `{$database}`.`{$table}` SET {$updates} WHERE {$where};\n";
		
		# In safe mode, only show the query
		if ($safe) {
			echo $this->query . "<br />";
			return true;
		}
		
		# Execute the query
		$rows = $this->execute ($this->query);
		
		# Determine the result
		$result = ($rows !== false);
		
		# Log the change
		$this->logChange ($this->query, $result);
		
		# Return whether the operation failed or succeeded
		return $result;
	}
	
	
	# Function to log a change
	#!# Ideally have some way to throw an error if the logfile is not writable
	function logChange ($query, $result)
	{
		# End if logging disabled
		if (!$this->logFile) {return false;}
		
		# End if the file is not writable, or the containing directory is not if the file does not exist
		if (file_exists ($this->logFile)) {
			if (!is_writable ($this->logFile)) {return false;}
		} else {
			$directory = dirname ($this->logFile);
			if (!is_writable ($directory)) {return false;}
		}
		
		# Create the log entry
		$logEntry = '/* ' . ($result ? 'Success' : 'Failure') . ' ' . date ('Y-m-d H:i:s') . ' by ' . $this->userForLogging . ' */ ' . str_replace ("\r\n", '\\r\\n', $query);
		
		# Log the change
		file_put_contents ($this->logFile, $logEntry, FILE_APPEND);
	}
	
	
	# Function to delete data
	function delete ($database, $table, $conditions, $limit = false)
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
		
		# Determine any limit
		$limit = ($limit ? " LIMIT {$limit}" : '');
		
		# Assemble the query
		$this->query = "DELETE FROM `{$database}`.`{$table}`{$where}{$limit};\n";
		
		# Execute the query
		$result = $this->execute ($this->query);
		
		# Log the change
		$this->logChange ($this->query, $result);
		
		# Return whether the operation failed or succeeded
		return $result;
	}
	
	
	# Function to delete a set of ids
	function deleteIds ($database, $table, $list, $field = 'id')
	{
		# End if no items
		if (!$list || !is_array ($list)) {return false;}
		
		# Apply escaping; see comment in http://dev.mysql.com/doc/refman/5.0/en/regexp.html stating that preg_quote seems to work
		foreach ($list as $index => $item) {
			$list[$index] = addslashes (preg_quote ($item));
		}
		
		# Assemble the query
		$this->query = "DELETE FROM `{$database}`.`{$table}` WHERE `{$field}` REGEXP '^(" . implode ('|', $list) . ")$';\n";
		
		# Execute the query
		$result = $this->execute ($this->query);
		
		# Log the change
		$this->logChange ($this->query, $result);
		
		# Return whether the operation failed or succeeded
		return $result;
	}
	
	
	# Function to get table metadata
	function getTableStatus ($database, $table, $getOnly = false /*array ('Comment')*/)
	{
		# Define the query
		$this->query = "SHOW TABLE STATUS FROM `{$database}` LIKE '{$table}';";
		
		# Get the results
		$data = $this->getOne ($this->query);
		
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
	
	
	# Function to get error information
	function error ()
	{
		# Get the error details
		if ($this->connection) {
			$error = $this->connection->errorInfo ();
		} else {
			$error = array ('error' => 'No database connection available');
		}
		
		# Add in the SQL statement
		$error['query'] = $this->query;
		
		# Return the details
		return $error;
	}
	
	
	# Define a lookup function used to join fields in the format fieldname__JOIN__targetDatabase__targetTable__reserved
	#!# Caching mechanism needed for repeated fields (and fieldnames as below), one level higher in the calling structure
	function lookup ($databaseConnection, $fieldname, $fieldType, $showKeys = NULL, $orderby = false, $sort = true, $group = false, $firstOnly = false)
	{
		# Determine if it's a special JOIN field
		$values = array ();
		$targetDatabase = NULL;
		$targetTable = NULL;
		if ($matches = database::convertJoin ($fieldname)) {
			
			# Load required libraries
			require_once ('application.php');
			
			# Assign the new fieldname
			$fieldname = $matches['field'];
			$targetDatabase = $matches['database'];
			$targetTable = $matches['table'];
			
			# Get the fields of the target table
			$fields = $databaseConnection->getFieldNames ($targetDatabase, $targetTable);
			
			# Deal with ordering
			$orderbySql = '';
			if ($orderby) {
				
				# Get those fields in the orderby list that exist in the table being linked to
				$orderby = application::ensureArray ($orderby);
				$fieldsPresent = array_intersect ($orderby, $fields);
				
				# Compile the SQL
				$orderbySql = ' ORDER BY ' . implode (',', $fieldsPresent);
			}
			
			# Get the data
			#!# Enable recursive lookups
			$query = "SELECT * FROM {$targetDatabase}.{$targetTable}{$orderbySql};";
			if (!$data = $databaseConnection->getData ($query, "{$targetDatabase}.{$targetTable}")) {
				return array ($fieldname, array (), $targetDatabase, $targetTable);
			}
			
			# Sort
			if ($sort) {ksort ($data);}
			
			# Determine whether to show keys (defaults to showing keys if the field is numeric)
			$showKey = ($showKeys === NULL ? (!strstr ($fieldType, 'int(')) : $showKeys);
			
			# Deal with grouping if required
			$grouped = false;
			if ($group) {
				
				# Determine the field to attempt to use, either a supplied fieldname or the second (first non-key) field. If the group 'name' supplied is a number, treat as an index (e.g. second key name)
				$groupField = (($group === true || is_numeric ($group)) ? application::arrayKeyName ($data, (is_numeric ($group) ? $group : 2), true) : $group);
				
				# Confirm existence of that field
				if ($groupField && in_array ($groupField, $fields)) {
					
					# Find if any group field values are unique; if so, regroup the whole dataset; if not, don't regroup
					$groupValues = array ();
					foreach ($data as $key => $rowData) {
						$groupFieldValue = $rowData[$groupField];
						if (!in_array ($groupFieldValue, $groupValues)) {
							$groupValues[$key] = $groupFieldValue;
						} else {
							
							# Regroup the data and flag this
							$data = application::regroup ($data, $groupField, false);
							$grouped = true;
							break;
						}
					}
				}
			}
			
			# Convert the data into a single key/value pair, removing repetition of the key if required
			if ($grouped) {
				foreach ($data as $groupKey => $groupData) {
					foreach ($groupData as $key => $rowData) {
						#!# This assumes the key is the first ...
						array_shift ($rowData);
						/*
						unset ($rowData[$groupField]);
						if (application::allArrayElementsEmpty ($rowData)) {
							array_unshift ($rowData, "{{$groupKey}}");
						}
						*/
						$values[$groupKey][$key]  = ($showKey ? "{$key}: " : '');
						$set = array_values ($rowData);
						$values[$groupKey][$key] .= ($firstOnly ? $set[0] : implode (' - ', $set));
					}
				}
			} else {
				foreach ($data as $key => $rowData) {
					#!# This assumes the key is the first ...
					array_shift ($rowData);
					$values[$key]  = ($showKey ? "{$key}: " : '');
					$set = array_values ($rowData);
					$values[$key] .= ($firstOnly ? $set[0] : implode (' - ', $set));
				}
			}
		}
		
		# Return the field name and the lookup values
		return array ($fieldname, $values, $targetDatabase, $targetTable);
	}
	
	
	# Function to convert joins
	function convertJoin ($fieldname)
	{
		# Return if matched
		if (ereg ('^([a-zA-Z0-9]+)__JOIN__([a-zA-Z0-9]+)__([-_a-zA-Z0-9]+)__reserved$', $fieldname, $matches)) {
			return array (
				'field' => $matches[1],
				'database' => $matches[2],
				'table' => $matches[3],
			);
		}
		
		# Otherwise return false;
		return false;
	}
}

?>