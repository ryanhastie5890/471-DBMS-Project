/*
 * This is the grammar parser class for parsing the commands
 * 
 * TokenMaker inner class controls the initial token creation  of the command as well as advancing through the tokens in the command
 * 
 * beginParse starts examining the command and calls the relevant functions depending on which command it is
 * 
 * ************Need to implement recursive descent parser to start reading the part of the command that comes after the initial call
 * ************For example i think it is needed for the part after CREATE TABLE in CREATE TABLE users(id int PRIMARY KEY, name varchar(10)) but im not sure yet
 *///Nothing is checking for semicolons yet. need to do that still
//rn just checking for syntax. eventually need it to start actually doing the commands
//tokens[tokenMaker.position] doesnt advance position. tokenMaker.check() does

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;



// if in database1 there is a table table1, then a table table1 in database2 would have the same files and would get mixed up.
//it also overwrited the original file
//to solve this  file names are now as follows
// (tableName)MetaData-(databaseName).txt for meta data
//(tableName)Record-(databaseName).txt for records
//(tableName)Index-(databaseName).txt for indexes
//(databaseName)-DB.txt for database files

public class GrammarParser {

	private TokenMaker tokenMaker;// utilization of inner class
	private String[] tokens;// tokens from inner class

	// database storage
	private ArrayList<DataBase> databases = new ArrayList<DataBase>();
	private DataBase currentDB = null;
	private File currentDBFile = null;

	private ArrayList<ColumnDefinition> pendingCreateColumns = new ArrayList<ColumnDefinition>();
	private ArrayList<String> pendingRenameAttributes = new ArrayList<String>();
	private ArrayList<String> pendingInsertValues = new ArrayList<String>();
	private LinkedHashMap<String, String> pendingUpdateAssignments = new LinkedHashMap<String, String>();
	private ArrayList<String> pendingSelectAttributes = new ArrayList<String>();
	private ArrayList<String> pendingSelectTables = new ArrayList<String>();
	private ArrayList<ConditionTerm> pendingConditions = new ArrayList<ConditionTerm>();
	private ArrayList<String> pendingConditionJoins = new ArrayList<String>();
	private String pendingAggregateFunction = null;
	private String pendingAggregateAttribute = null;

	public GrammarParser() throws IOException {// empty constructor
        init();
	}

	public GrammarParser(String text) throws IOException {// constructor
		tokenMaker = new TokenMaker(text);
		tokens = tokenMaker.tokens;
		init();

	}

	public void setCommand(String command) {
		tokenMaker = new TokenMaker(command);
		tokens = tokenMaker.tokens;
	}
	
	public void init() throws IOException {
		currentDB = null;
		currentDBFile = null;
		databases.clear();
		
		//----read all databases, create database, add files
		readDatabaseFiles();

	}

	public void readDatabaseFiles() throws IOException {//----read all databases, create database, add files
		File root = new File(".");//should point to root directory
		
		File[] files = root.listFiles((directory,name)->//lambda to find files
		   name.endsWith("-DB.txt")
	     );
		
		if(files != null) {
			for(File file : files) {
				//reader
				BufferedReader reader = new BufferedReader(new FileReader(file));
				
				String currentLine;
				//create database
				
				if((currentLine = reader.readLine())!=null) {//make sure file not empty
					//currentLine is now db name
					DataBase db = new DataBase(currentLine.trim());
					this.databases.add(db);
					
					//read table names in the file
					while((currentLine =reader.readLine()) != null) {
						currentLine = currentLine.trim();
						if(!currentLine.isEmpty() && !db.tableNames.contains(currentLine)) {
							db.tableNames.add(currentLine);
						}
					}
					
					//start adding table files for database
					getTableFiles(db);
				}
				reader.close();

			}
		}
	}
	
	public void getTableFiles(DataBase db) throws IOException {
		//database should have all table names, now find all its table's files
		
		
		File root = new File(".");//root
		
		File [] files = root.listFiles();//get all files
		
		for(int i = 0; i < db.tableNames.size(); i++) {//for each table
		
		  String tableName = db.tableNames.get(i);
			
		  if(files != null) {//search
			for(File file : files) {
				if(file.getName().equals(tableName+"MetaData-"+db.name+".txt")) {//meta data file found
						   if(!db.metaDataFiles.contains(file)) {
							   db.metaDataFiles.add(file);	
						   }
				}
				if(file.getName().equals(tableName+"Record-"+db.name+".txt")) {//record file found
				
						   if(!db.recordFiles.contains(file)) {
							   db.recordFiles.add(file);	
						   }
				}
				if(file.getName().equals(tableName+"Index-"+db.name+".txt")) {//index file found
					
						   if(!db.indexFiles.contains(file)) {
							   db.indexFiles.add(file);	
						   }
					
				}
			}
		}
		  if(db.tableTrees.size() <= i) {
			  db.tableTrees.add(new BST());
		  }
		}
	}
	
	public void beginParse() throws IOException, GrammarParser.InvalidCommandException {// parses the initial input
		if(tokenMaker == null || tokens == null || tokens.length == 0 || tokenMaker.peek().equals("EOF")) {
			throw new InvalidCommandException("Empty command detected");
		}
		String current = tokens[tokenMaker.position];

		// add an else if for each command and then call a function that parses the
		// relevant command
		if (current.equalsIgnoreCase("CREATE")) {
			create();
		} else if (current.equalsIgnoreCase("INSERT")) {
			insert();
		} else if (current.equalsIgnoreCase("DESCRIBE")) {
			describe();
		} else if (current.equalsIgnoreCase("RENAME")) {
			rename();
		} else if (current.equalsIgnoreCase("UPDATE")) {
			update();
		} else if (current.equalsIgnoreCase("SELECT")) {
			select();
		} else if (current.equalsIgnoreCase("LET")) {
			let();
		} else if (current.equalsIgnoreCase("DELETE")) {
			delete();
		} else if (current.equalsIgnoreCase("INPUT")) {
			input();
		} else if (current.equalsIgnoreCase("USE")) {
			use();
		} else {
			throw new InvalidCommandException("Invalid command detected");
			
		}
		if(!tokenMaker.atEnd()) {
			throw new InvalidCommandException("Unexpected token after command: " + tokenMaker.peek());
		}

	}

	public void create() throws IOException, GrammarParser.InvalidCommandException {// create command
		if (!tokenMaker.match("CREATE")) {// checks if it is create table
			throw new InvalidCommandException("CREATE missing or incorrect");
		} else {
			if (!(tokenMaker.peek().equalsIgnoreCase("TABLE") || tokenMaker.peek().equalsIgnoreCase("DATABASE"))) {
				throw new InvalidCommandException("TABLE or DATABASE not found or incorrect");
			} else {

				String token = tokenMaker.check();
				if (token.equalsIgnoreCase("DATABASE")) {
					String databaseName = parseIdentifier();
					if(tokenMaker.match(";")) {
						//enacting db command
						if(findDatabase(databaseName) != null) {
							throw new InvalidCommandException("Database already exists");
						}
						DataBase db = new DataBase(databaseName);
						//currentDB = db; need to use USE now
						databases.add(db);
						
						//start creating file for database that is titled dbname-DB.txt and contains the names of tables that are under the db
						//this is for initialization to save dbs and tables between sessions
						
						File databaseFile = new File(databaseName + "-DB.txt");
						//write db name as first line of file
						FileWriter dbWrite = new FileWriter(databaseFile,true);
						PrintWriter printer = new PrintWriter(dbWrite);//using to do names on new lines, may not be necessary
						
						if(databaseFile.length() == 0) {
							printer.println(databaseName);
						}
						printer.close();
						
						System.out.println("Database created");
						
					}
					else {
						throw new InvalidCommandException("Semicolon missing or incorrect");
					}
					
					
					
				} else if (token.equalsIgnoreCase("TABLE")) {
					requireCurrentDB();
					if (!tokenMaker.match("(")) {// check that table name wasnt skipped

						String tableName = parseIdentifier();
						
						//verify table name doesnt exist
						Boolean found = false;
						ArrayList<String> tables = this.currentDB.tableNames;
						for(int i = 0; i < tables.size(); i ++) {
							String checkName = tables.get(i);
							if (checkName.equalsIgnoreCase(tableName)) {
								found = true;
								throw new InvalidCommandException("Table already exists");
							}
						}
						if(found) {
							throw new InvalidCommandException("Table already exists");
						}
						

						if (!tokenMaker.match("(")) {
							throw new InvalidCommandException("( missing or incorrect");
						} else {// begin parsing inside the parentheses
							pendingCreateColumns.clear();
							parseCreateList();

							if (!tokenMaker.match(")")) {
								throw new InvalidCommandException(") missing or incorrect");
							} else {
								if (!tokenMaker.match(";")) {
									throw new InvalidCommandException("Semicolon missing or incorrect");
								} else {
									//System.out.println("Create command parsing done");
									enactCreate(tableName);
								}
							}
						}

					} else {
						throw new InvalidCommandException("Table name not found");
					}
				} else {
					throw new InvalidCommandException("Invalid create command, TABLE or DATABASE not read");
				}
			}
		}
	}

	// methods work in progress, untested so far
	public void parseCreateList() throws GrammarParser.InvalidCommandException {// starts parsing create command after the '('
		parseCreateDefinition();// start parsing first attribute

		while (tokenMaker.match(",")) {// hopefully takes each attribute and begins parsing it
			// tokenMaker.check();//advance past comma

			parseCreateDefinition();// continue parsing attributes
		}
	}

	public void parseCreateDefinition() throws GrammarParser.InvalidCommandException {
		String name = parseIdentifier();// name of attribute

		TypeDefinition type = parseCreateType();// type
		boolean primaryKey = false;
		boolean foreignKey = false;

		if (tokenMaker.peek().equalsIgnoreCase("PRIMARY") || tokenMaker.peek().equalsIgnoreCase("FOREIGN")) {
			String constraintType = parseCreateConstraint();// primary key stuff, add foreign key stuff later
			if(constraintType.equalsIgnoreCase("PRIMARY")) {
				primaryKey = true;
			}
			if(constraintType.equalsIgnoreCase("FOREIGN")) {
				foreignKey = true;
			}
		}
		pendingCreateColumns.add(new ColumnDefinition(name, type.kind, type.size, primaryKey, foreignKey));
	}

	public TypeDefinition parseCreateType() throws GrammarParser.InvalidCommandException {
		if (tokenMaker.match("VARCHAR") || tokenMaker.match("TEXT")) {
			if(tokens[tokenMaker.position - 1].equalsIgnoreCase("TEXT")) {
				return new TypeDefinition("TEXT", 100);
			}
			if (tokenMaker.match("(")) {
				String number = tokenMaker.check();
				if(!isIntegerLiteral(number)) {
					throw new InvalidCommandException("VARCHAR size must be numeric");
				}
				if (tokenMaker.match(")")) {
					return new TypeDefinition("TEXT", Integer.parseInt(number));
				} else {
					throw new InvalidCommandException(") missing or incorrect");
				}
			} else {
				throw new InvalidCommandException("( missing or incorrect");
			}
		} else if (tokenMaker.match("INT") || tokenMaker.match("INTEGER")) {
			return new TypeDefinition("INTEGER", 32);
		} else if(tokenMaker.match("FLOAT")) {
			return new TypeDefinition("FLOAT", 0);
		} else {
			throw new InvalidCommandException("Invalid attribute type detected");
		}
	}

	public String parseCreateConstraint() throws GrammarParser.InvalidCommandException {
		// System.out.println(tokens[tokenMaker.position] +" and
		// "+tokens[tokenMaker.position +1]);
		if (tokenMaker.peek().equalsIgnoreCase("PRIMARY") && tokenMaker.peekNext().equalsIgnoreCase("KEY")) {
			// System.out.println("Primary key test passed");
			tokenMaker.check();
			tokenMaker.check();
			return "PRIMARY";
		} else if (tokenMaker.peek().equalsIgnoreCase("FOREIGN") && tokenMaker.peekNext().equalsIgnoreCase("KEY")) {
			tokenMaker.check();
			tokenMaker.check();
			return "FOREIGN";
		} else {
			throw new InvalidCommandException("Invalid constraint detected");
		}
	}

	public void enactCreate(String tableName) throws IOException, GrammarParser.InvalidCommandException {// actually does actions based on the valid create table command. havent done
								// database yet
		requireCurrentDB();
		if(currentDB.tableNames.contains(tableName)) {
			throw new InvalidCommandException("Table already exists in database");
		}

		// create tables and store in data members

		// meta data
		File metaDataFile = new File(tableName + "MetaData-"+currentDB.name+".txt");
		currentDB.metaDataFiles.add(metaDataFile);

		// record data
		File recordFile = new File(tableName + "Record-"+currentDB.name+".txt");
		currentDB.recordFiles.add(recordFile);

		// index
		File indexFile = new File(tableName + "Index-"+currentDB.name+".txt");
		currentDB.indexFiles.add(indexFile);

		// bst
		BST tableTree = new BST();
		currentDB.tableTrees.add(tableTree);

		// tablename
		currentDB.tableNames.add(tableName);

		// start writing to files. this part will probably need to be tweaked depending
		// on how we actually use the files

		// write attributes to metaData File
		// file should start with table name then contain all attributes and their types
		
		FileWriter writer = new FileWriter(metaDataFile);
		PrintWriter printer = new PrintWriter(writer);//using to do names on new lines, may not be necessary
		printer.println(tableName);

		//loop makes metadata file for command create table tableName (id int, name varchar(10)); look like
		//tablename
		//id int
		//name varchar 10
		for(ColumnDefinition column : pendingCreateColumns) {
			StringBuilder line = new StringBuilder();
			line.append(column.name).append("|").append(column.type).append("|").append(column.size).append("|").append(column.primaryKey ? "1" : "0").append("|").append(column.foreignKey ? "1" : "0");
			printer.println(line.toString());
		}
		printer.close();

		if(!recordFile.exists()) {
			recordFile.createNewFile();
		}
		if(!indexFile.exists()) {
			indexFile.createNewFile();
		}
		
		//need to write to database file which tables belong to each database so when creating table, am writing to currentDb's file
		//the table name. append it

		
		FileWriter dbWrite = new FileWriter((currentDB.name+"-DB.txt"),true);
		printer = new PrintWriter(dbWrite);//using to do names on new lines, may not be necessary
		
		printer.println(tableName);
		printer.close();
		writeIndexFile(tableName, new LinkedHashMap<String, Integer>());
		System.out.println("Table created");
	}

	// USE command

	public void use() throws GrammarParser.InvalidCommandException {
		tokenMaker.expect("USE");//burn USE since already detected
		String dbName = parseIdentifier();
		tokenMaker.expect(";");
		
		//check if dbName is in list of databases
		Boolean found = false;
		DataBase foundDb = null;
		for(int i = 0; i < databases.size(); i ++) {
			DataBase database = databases.get(i);
			if (database.name.equalsIgnoreCase(dbName)) {
				found = true;
				foundDb = database;
			}
		}
		
		if(!found) {
			throw new InvalidCommandException("Database not found");
		}else {
			this.currentDB = foundDb;
			this.currentDBFile = new File(foundDb.name + "-DB.txt");
			System.out.println("Switched db to: "+dbName);
		}
	}
	// inserts
	public void insert() throws IOException, GrammarParser.InvalidCommandException {// prototype insert command read
		requireCurrentDB();
		if (!tokenMaker.check().equalsIgnoreCase("INSERT")) {// checks if it is create table
			throw new InvalidCommandException("Invalid insert command 1");
		} else {
			String name = parseIdentifier();
			ensureTableExists(name);

			if (!tokenMaker.check().equalsIgnoreCase("VALUES")) {// check that table name wasnt skipped

				throw new InvalidCommandException("Invalid insert command 2");

			} else {
				if (!tokenMaker.check().equalsIgnoreCase("(")) {
					throw new InvalidCommandException("Invalid insert command 3");
				} else {
					pendingInsertValues.clear();
					parseInsertList();
					tokenMaker.expect(")");
					tokenMaker.expect(";");
					enactInsert(name);
				}
			}
		}
	}

	public void parseInsertList() throws GrammarParser.InvalidCommandException {// insert version of parse list
		int i = 1;
		parseInsertValues(i);// start parsing first attribute

		while (tokens[tokenMaker.position].equalsIgnoreCase(",")) {// hopefully takes each attribute and begins parsing it
			tokenMaker.check();// advance past comma
			parseInsertValues(++i);// continue parsing attributes
		}
	}

	public void parseInsertValues(int attributeNumber) throws GrammarParser.InvalidCommandException {
		String value = parseConstantToken();
		pendingInsertValues.add(value);
		// need to check domain key and entity integrity constraints
	}

	private void enactInsert(String tableName) throws IOException, InvalidCommandException {
		TableDefinition definition = readTableDefinition(tableName);
		if(pendingInsertValues.size() != definition.columns.size()) {
			throw new InvalidCommandException("Incorrect number of values");
		}

		ArrayList<String> normalizedValues = new ArrayList<String>();
		for(int i = 0; i < definition.columns.size(); i++) {
			ColumnDefinition column = definition.columns.get(i);
			String rawValue = pendingInsertValues.get(i);
			String checkedValue = validateAndNormalizeConstant(rawValue, column);
			if(column.primaryKey && checkedValue.equalsIgnoreCase("null")) {
				throw new InvalidCommandException("Primary key cannot be null");
			}
			normalizedValues.add(checkedValue);
		}

		String primaryKeyValue = getPrimaryKeyValue(definition, normalizedValues);
		if(primaryKeyValue != null) {
			Map<String, Integer> indexMap = readIndexFile(tableName);
			String key = canonicalCompareValue(primaryKeyValue);
			if(indexMap.containsKey(key)) {
				throw new InvalidCommandException("Duplicate primary key");
			}
		}

		File recordFile = getRecordFile(tableName);
		ArrayList<String> currentLines = readAllLines(recordFile);
		int nextRecordNumber = currentLines.size();
		try(PrintWriter printer = new PrintWriter(new FileWriter(recordFile, true))) {
			printer.println(joinRecord(normalizedValues));
		}

		if(primaryKeyValue != null) {
			Map<String, Integer> indexMap = readIndexFile(tableName);
			indexMap.put(canonicalCompareValue(primaryKeyValue), nextRecordNumber);
			writeIndexFile(tableName, indexMap);
		}
		System.out.println("1 tuple inserted");
	}

	// beginning of describe command
	public void describe() throws IOException, GrammarParser.InvalidCommandException {
		tokenMaker.expect("DESCRIBE");
		if (!tokenMaker.check().equalsIgnoreCase("(")) {
			throw new InvalidCommandException("Invalid describe command 1");
		} else {
			String command = tokenMaker.check();
			if (command.equalsIgnoreCase("ALL")) {
				if(tokenMaker.match(")")) {
				 if(tokenMaker.match(";")) {
				    describeAll();
				 }
				 else {
					 throw new InvalidCommandException("Missing or incorrect ;");
				 }
				}
				else {
					throw new InvalidCommandException("Missing or incorrect )");
				}
			} else {
				if(tokenMaker.match(")")) {
					 if(tokenMaker.match(";")) {
						 describeTable(command);
					 }
					 else {
						 throw new InvalidCommandException("Missing or incorrect ;");
					 }
					}
					else {
						throw new InvalidCommandException("Missing or incorrect )");
					}
				
			}
		}
	}

	public void describeAll() throws IOException, GrammarParser.InvalidCommandException {
		//System.out.println("describe all test passed");
		requireCurrentDB();
        for(int i = 0; i < currentDB.metaDataFiles.size(); i++) {
        	File file = currentDB.metaDataFiles.get(i);
        	TableDefinition definition = readTableDefinition(file);
        	printTableDefinition(definition);
        	System.out.println();
        }
	}

	public void describeTable(String tableName) throws IOException, GrammarParser.InvalidCommandException {
		//System.out.println("describe table test passed table: " + tableName);
		requireCurrentDB();
		ensureTableExists(tableName);
	    File file = new File(tableName+"MetaData-"+currentDB.name+".txt");
	    TableDefinition definition = readTableDefinition(file);
	    printTableDefinition(definition);
	}

	// beginning of rename command
	public void rename() throws GrammarParser.InvalidCommandException, IOException {
		requireCurrentDB();
		tokenMaker.expect("RENAME");// burn a token
		String tableName = parseIdentifier();
		
		if(!containsIgnoreCase(currentDB.tableNames, tableName)) {
			throw new InvalidCommandException("Could not find table");
		}

		if (!tokenMaker.check().equals("(")) {
			throw new InvalidCommandException("Missing or incorrect (");
		} else {
			pendingRenameAttributes.clear();
			renameList(tableName);
		}
	}

	public void renameList(String tableName) throws GrammarParser.InvalidCommandException, IOException {
		int i = 1;
		parseRenameAttributes(i);// start parsing first attribute

		while (tokens[tokenMaker.position].equals(",")) {// hopefully takes each attribute and begins parsing it
			tokenMaker.check();// advance past comma
			parseRenameAttributes(++i);// continue parsing attributes
		}
		
		//check for ) and ;
		if(!tokenMaker.match(")")) {
			throw new InvalidCommandException("Missing or incorrect )");
		}
		if(!tokenMaker.match(";")) {
			throw new InvalidCommandException("Missing or incorrect ;");
		}
		
		//make sure number of attributes is correct
		int numAttributes = -1;//-1 so table name wont count
		
		//find meta data file 
		File file = new File(tableName+"MetaData-"+currentDB.name+".txt");
		
		if(file.exists()) {
			//count attributes
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String currentLine;
			while((currentLine=reader.readLine())!=null) {
				numAttributes++;
			}
			reader.close();
		}else {
			throw new InvalidCommandException("Could not find table");
		}
		
		if(numAttributes!=i) {
			throw new InvalidCommandException("Incorrect number of attributes");
		}
		
		//command should be fine now, carry out command
		enactRename(file);
		
	}
	
	//carries out the rename command
	public void enactRename(File file) throws IOException, GrammarParser.InvalidCommandException {
	    tokenMaker.position=0;//reset command
	    //burn uneeded part of command
	    tokenMaker.check();
	    tokenMaker.check();
	    tokenMaker.check();
	    
	    //need to go through file starting on line two and change the first word as that is the name
	    BufferedReader reader = new BufferedReader(new FileReader(file));
	    
	    ArrayList<String> lines = new ArrayList<String>();//store read lines
	    
	    String currentLine;
	    int lineNumber = 1;
	    int renameIndex = 0;
	    
	    while((currentLine = reader.readLine())!=null) {
	    	
	    	if(lineNumber >= 2) {//not table line so split
	    		ColumnDefinition column = parseColumnLine(currentLine);
	    		column.name = pendingRenameAttributes.get(renameIndex);
	    		currentLine = serializeColumn(column);
	    		renameIndex++;
	    	}
	    	lines.add(currentLine);
	    	lineNumber++;//next line
	    }
	    
	    reader.close();
	    
	    //write lines back
	    BufferedWriter writer = new BufferedWriter(new FileWriter(file));
	    
	    for(String line : lines) {
	    	writer.write(line);
	    	writer.newLine();
	    }
	    
	    writer.close();
	    System.out.println("Attributes renamed");
	}

	public void parseRenameAttributes(int attributeNumber) throws GrammarParser.InvalidCommandException {
		String attr = parseIdentifier();
		pendingRenameAttributes.add(attr);
		// test statement
		//System.out.println("Attribute " + attributeNumber + ": " + tokenMaker.check());
	}

	// begin update methods
	public void update() throws IOException, GrammarParser.InvalidCommandException {
		requireCurrentDB();
		tokenMaker.expect("UPDATE");// burn UPDATE
		String tableName = parseIdentifier();
		ensureTableExists(tableName);

		if (!tokenMaker.check().equalsIgnoreCase("SET")) {
			throw new InvalidCommandException("Invalid update command 1");
		} else {
			pendingUpdateAssignments.clear();
			parseUpdateList();

			// start parsing where statement
			pendingConditions.clear();
			pendingConditionJoins.clear();
			if(tokenMaker.peek().equalsIgnoreCase("WHERE")) {
				tokenMaker.check();
				parseCondition();
			}
			tokenMaker.expect(";");
			enactUpdate(tableName);
		}
	}

	public void parseUpdateList() throws GrammarParser.InvalidCommandException {
		int i = 1;
		parseUpdateValues(i);// start parsing first attribute

		while (tokenMaker.position < tokens.length && tokens[tokenMaker.position].equals(",")) {// hopefully takes each
														// attribute and begins
														// parsing it
			tokenMaker.check();// advance past comma
			parseUpdateValues(++i);// continue parsing attributes
		}
	}

	public void parseUpdateValues(int attributeNumber) throws GrammarParser.InvalidCommandException {
		String name = parseIdentifier();

		if (!tokenMaker.check().equals("=")) {
			throw new InvalidCommandException("Invalid update command 2");
		}

		String value = parseConstantToken();
		pendingUpdateAssignments.put(name, value);

		System.out.println("Received constant " + attributeNumber + ": " + name + " " + value);
	}

	private void enactUpdate(String tableName) throws IOException, InvalidCommandException {
		TableDefinition definition = readTableDefinition(tableName);
		ArrayList<Map<String, String>> rows = readAllRows(definition);
		ArrayList<String> rawLines = readAllLines(getRecordFile(tableName));
		if(rows.size() != rawLines.size()) {
			throw new InvalidCommandException("Record file mismatch");
		}

		int updatedCount = 0;
		for(int i = 0; i < rows.size(); i++) {
			Map<String, String> row = rows.get(i);
			if(pendingConditions.isEmpty() || evaluateCondition(row, pendingConditions, pendingConditionJoins)) {
				for(Map.Entry<String, String> entry : pendingUpdateAssignments.entrySet()) {
					ColumnDefinition column = definition.findColumn(entry.getKey());
					if(column == null) {
						throw new InvalidCommandException("Unknown attribute in update: " + entry.getKey());
					}
					String normalized = validateAndNormalizeConstant(entry.getValue(), column);
					if(column.primaryKey && normalized.equalsIgnoreCase("null")) {
						throw new InvalidCommandException("Primary key cannot be null");
					}
					row.put(column.name, normalized);
				}
				updatedCount++;
			}
		}

		ensureUniquePrimaryKeys(definition, rows);
		writeAllRows(definition, rows);
		System.out.println(updatedCount + " tuple(s) updated");
	}

	// select
	public void select() throws IOException, GrammarParser.InvalidCommandException {
		requireCurrentDB();
		pendingSelectAttributes.clear();
		pendingSelectTables.clear();
		pendingConditions.clear();
		pendingConditionJoins.clear();
		pendingAggregateFunction = null;
		pendingAggregateAttribute = null;

		if (!tokenMaker.check().equalsIgnoreCase("SELECT")) {
			throw new InvalidCommandException("Invalid select command 1");
		} else {
			selectNameList();

			if (!tokenMaker.check().equalsIgnoreCase("FROM")) {
				throw new InvalidCommandException("Invalid select command 2");
			} else {
				selectTablesList();

				// start where portion
				if(tokenMaker.peek().equalsIgnoreCase("WHERE")) {
					tokenMaker.check();
					parseCondition();
				}
				tokenMaker.expect(";");
				enactSelect();
			}
		}
	}

	public void selectNameList() throws GrammarParser.InvalidCommandException {
		if(tokenMaker.peek().equalsIgnoreCase("COUNT") || tokenMaker.peek().equalsIgnoreCase("MIN") || tokenMaker.peek().equalsIgnoreCase("MAX") || tokenMaker.peek().equalsIgnoreCase("AVERAGE")) {
			parseAggregateSelection();
			return;
		}
		int i = 1;
		parseAttributeNames(i);// start parsing first attribute

		while (tokenMaker.position < tokens.length && tokens[tokenMaker.position].equals(",")) {// hopefully takes each
														// attribute and begins
														// parsing it
			tokenMaker.check();// advance past comma
			parseAttributeNames(++i);// continue parsing attributes
		}
	}

	public void parseAttributeNames(int nameNumber) throws GrammarParser.InvalidCommandException {
		String attr = parseIdentifierOrStar();
		pendingSelectAttributes.add(attr);
		System.out.println("Received Attribute " + nameNumber + ": " + attr);
	}

	public void selectTablesList() throws GrammarParser.InvalidCommandException {
		int i = 1;
		parseTableNames(i);// start parsing first attribute

		while (tokenMaker.position < tokens.length && tokens[tokenMaker.position].equals(",")) {// hopefully takes each
														// attribute and begins
														// parsing it
			tokenMaker.check();// advance past comma
			parseTableNames(++i);// continue parsing attributes
		}
	}

	public void parseTableNames(int tableNumber) throws GrammarParser.InvalidCommandException {
		String tableName = parseIdentifier();
		pendingSelectTables.add(tableName);
		System.out.println("Received table " + tableNumber + ": " + tableName);
	}

	private void enactSelect() throws IOException, InvalidCommandException {
		if(pendingSelectTables.isEmpty()) {
			throw new InvalidCommandException("No table selected");
		}
		if(pendingSelectTables.size() > 1) {
			throw new InvalidCommandException("Multiple tables in SELECT are not supported in this file yet");
		}

		String tableName = pendingSelectTables.get(0);
		ensureTableExists(tableName);
		TableDefinition definition = readTableDefinition(tableName);
		ArrayList<Map<String, String>> rows = readAllRows(definition);
		ArrayList<Map<String, String>> matched = new ArrayList<Map<String, String>>();
		for(Map<String, String> row : rows) {
			if(pendingConditions.isEmpty() || evaluateCondition(row, pendingConditions, pendingConditionJoins)) {
				matched.add(row);
			}
		}

		if(pendingAggregateFunction != null) {
			printAggregateResult(definition, matched);
			return;
		}

		ArrayList<String> projection = new ArrayList<String>();
		if(pendingSelectAttributes.size() == 1 && pendingSelectAttributes.get(0).equals("*")) {
			for(ColumnDefinition column : definition.columns) {
				projection.add(column.name);
			}
		} else {
			for(String attr : pendingSelectAttributes) {
				ColumnDefinition column = definition.findColumn(attr);
				if(column == null) {
					throw new InvalidCommandException("Unknown attribute in SELECT: " + attr);
				}
				projection.add(column.name);
			}
		}

		if(matched.isEmpty()) {
			System.out.println("Nothing found");
			return;
		}

		System.out.println(String.join(" | ", projection));
		for(int i = 0; i < matched.size(); i++) {
			Map<String, String> row = matched.get(i);
			ArrayList<String> out = new ArrayList<String>();
			for(String attr : projection) {
				out.add(displayValue(row.get(attr)));
			}
			System.out.println((i + 1) + ". " + String.join(" | ", out));
		}
	}

	private void parseAggregateSelection() throws InvalidCommandException {
		String function = tokenMaker.check().toUpperCase(Locale.ROOT);
		pendingAggregateFunction = function;
		tokenMaker.expect("(");
		if(function.equals("COUNT") && tokenMaker.peek().equals("*")) {
			tokenMaker.check();
			pendingAggregateAttribute = "*";
		} else {
			pendingAggregateAttribute = parseIdentifier();
		}
		tokenMaker.expect(")");
	}

	private void printAggregateResult(TableDefinition definition, ArrayList<Map<String, String>> matched) throws InvalidCommandException {
		if(pendingAggregateFunction.equals("COUNT")) {
			System.out.println("COUNT(*) = " + matched.size());
			return;
		}

		ColumnDefinition column = definition.findColumn(pendingAggregateAttribute);
		if(column == null) {
			throw new InvalidCommandException("Unknown aggregate attribute");
		}
		if(!column.type.equalsIgnoreCase("INTEGER") && !column.type.equalsIgnoreCase("FLOAT")) {
			throw new InvalidCommandException("Aggregate requires numeric attribute");
		}
		if(matched.isEmpty()) {
			System.out.println("Nothing found");
			return;
		}

		double min = Double.POSITIVE_INFINITY;
		double max = Double.NEGATIVE_INFINITY;
		double sum = 0.0;
		for(Map<String, String> row : matched) {
			double value = Double.parseDouble(row.get(column.name));
			if(value < min) {
				min = value;
			}
			if(value > max) {
				max = value;
			}
			sum += value;
		}
		if(pendingAggregateFunction.equals("MIN")) {
			System.out.println("MIN(" + column.name + ") = " + trimDouble(min));
		} else if(pendingAggregateFunction.equals("MAX")) {
			System.out.println("MAX(" + column.name + ") = " + trimDouble(max));
		} else if(pendingAggregateFunction.equals("AVERAGE")) {
			System.out.println("AVERAGE(" + column.name + ") = " + trimDouble(sum / matched.size()));
		} else {
			throw new InvalidCommandException("Unsupported aggregate");
		}
	}

	// commands below this line are not as developed as the commands above yet
	// let
	public void let() throws IOException, GrammarParser.InvalidCommandException {
		requireCurrentDB();
		if (!tokenMaker.check().equalsIgnoreCase("LET")) {
			throw new InvalidCommandException("Invalid let command 1");
		} else {
			String tableName = parseIdentifier();

			if (!tokenMaker.check().equalsIgnoreCase("KEY")) {
				throw new InvalidCommandException("Invalid let command 2");
			} else {
				String attributeName = parseIdentifier();

				System.out.println("let test passed");
				// select part
				if(!tokenMaker.peek().equalsIgnoreCase("SELECT")) {
					throw new InvalidCommandException("LET must be followed by SELECT");
				}
				// reuse existing select parse and write result to a new table
				pendingSelectAttributes.clear();
				pendingSelectTables.clear();
				pendingConditions.clear();
				pendingConditionJoins.clear();
				pendingAggregateFunction = null;
				pendingAggregateAttribute = null;
				select();
				if(pendingSelectTables.size() != 1) {
					throw new InvalidCommandException("LET currently supports one source table");
				}
				String sourceTable = pendingSelectTables.get(0);
				TableDefinition sourceDefinition = readTableDefinition(sourceTable);
				ArrayList<String> projection = new ArrayList<String>();
				if(pendingSelectAttributes.size() == 1 && pendingSelectAttributes.get(0).equals("*")) {
					for(ColumnDefinition column : sourceDefinition.columns) {
						projection.add(column.name);
					}
				} else {
					projection.addAll(pendingSelectAttributes);
				}
				if(!containsIgnoreCase(projection, attributeName)) {
					throw new InvalidCommandException("Key attribute must be one of selected attributes");
				}
				createDerivedTableFromSelect(tableName, attributeName, sourceDefinition, projection);
			}
		}
	}

	private void createDerivedTableFromSelect(String newTableName, String keyName, TableDefinition sourceDefinition, ArrayList<String> projection) throws IOException, InvalidCommandException {
		if(containsIgnoreCase(currentDB.tableNames, newTableName)) {
			throw new InvalidCommandException("Table already exists");
		}
		ArrayList<Map<String, String>> rows = readAllRows(sourceDefinition);
		ArrayList<Map<String, String>> filtered = new ArrayList<Map<String, String>>();
		for(Map<String, String> row : rows) {
			if(pendingConditions.isEmpty() || evaluateCondition(row, pendingConditions, pendingConditionJoins)) {
				filtered.add(row);
			}
		}

		pendingCreateColumns.clear();
		for(String attr : projection) {
			ColumnDefinition source = sourceDefinition.findColumn(attr);
			if(source == null) {
				throw new InvalidCommandException("Unknown selected attribute in LET");
			}
			pendingCreateColumns.add(new ColumnDefinition(source.name, source.type, source.size, source.name.equalsIgnoreCase(keyName), false));
		}
		enactCreate(newTableName);

		for(Map<String, String> row : filtered) {
			pendingInsertValues.clear();
			for(String attr : projection) {
				pendingInsertValues.add(formatValueForWriteBack(row.get(attr), sourceDefinition.findColumn(attr)));
			}
			enactInsert(newTableName);
		}
	}

	// delete
	public void delete() throws IOException, GrammarParser.InvalidCommandException {
		requireCurrentDB();
		tokenMaker.expect("DELETE");// dont need the first if in some of the other commands since it checks in the
							// other method

		String tableName = parseIdentifier();
		ensureTableExists(tableName);

		System.out.println("delete test passed");
		// where part
		pendingConditions.clear();
		pendingConditionJoins.clear();
		if(tokenMaker.peek().equalsIgnoreCase("WHERE")) {
			tokenMaker.check();
			parseCondition();
		}
		tokenMaker.expect(";");
		enactDelete(tableName);
	}

	private void enactDelete(String tableName) throws IOException, InvalidCommandException {
		TableDefinition definition = readTableDefinition(tableName);
		if(pendingConditions.isEmpty()) {
			deleteEntireTable(tableName);
			System.out.println("Table deleted");
			return;
		}
		ArrayList<Map<String, String>> rows = readAllRows(definition);
		ArrayList<Map<String, String>> kept = new ArrayList<Map<String, String>>();
		int deleted = 0;
		for(Map<String, String> row : rows) {
			if(evaluateCondition(row, pendingConditions, pendingConditionJoins)) {
				deleted++;
			} else {
				kept.add(row);
			}
		}
		writeAllRows(definition, kept);
		System.out.println(deleted + " tuple(s) deleted");
	}

	private void deleteEntireTable(String tableName) throws IOException {
		File metaFile = getMetaFile(tableName);
		File recordFile = getRecordFile(tableName);
		File indexFile = getIndexFile(tableName);
		if(metaFile.exists()) {
			metaFile.delete();
		}
		if(recordFile.exists()) {
			recordFile.delete();
		}
		if(indexFile.exists()) {
			indexFile.delete();
		}
		removeCaseInsensitive(currentDB.tableNames, tableName);
		removeMatchingFile(currentDB.metaDataFiles, tableName + "MetaData-" + currentDB.name + ".txt");
		removeMatchingFile(currentDB.recordFiles, tableName + "Record-" + currentDB.name + ".txt");
		removeMatchingFile(currentDB.indexFiles, tableName + "Index-" + currentDB.name + ".txt");
		rewriteDatabaseFile();
	}

	// input
	public void input() throws IOException, GrammarParser.InvalidCommandException {
		tokenMaker.expect("INPUT");
		String fileName1 = parseIdentifierLikeFileName();
		String outputFile = null;
		if(tokenMaker.peek().equalsIgnoreCase("OUTPUT")) {
			tokenMaker.check();
			outputFile = parseIdentifierLikeFileName();
		}
		tokenMaker.expect(";");

		System.out.println("input test passed");
		executeInputFile(fileName1, outputFile);
	}

	private void executeInputFile(String inputFileName, String outputFileName) throws IOException, InvalidCommandException {
		File inputFile = new File(inputFileName);
		if(!inputFile.exists()) {
			throw new InvalidCommandException("Input file not found");
		}
		StringBuilder builder = new StringBuilder();
		try(BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
			String line;
			while((line = reader.readLine()) != null) {
				builder.append(line).append(' ');
			}
		}
		String[] statements = builder.toString().split(";");
		PrintWriter outputWriter = null;
		if(outputFileName != null) {
			outputWriter = new PrintWriter(new FileWriter(outputFileName));
		}
		for(String statement : statements) {
			String trimmed = statement.trim();
			if(trimmed.isEmpty()) {
				continue;
			}
			String fullStatement = trimmed + ";";
			if(outputWriter != null) {
				outputWriter.println("> " + fullStatement);
			}
			GrammarParser nested = this;
			nested.setCommand(fullStatement);
			nested.beginParse();
		}
		if(outputWriter != null) {
			outputWriter.close();
		}
	}

	// exit taken care of in DBMS.java
	
	// -------This is where new functions for the command should be added

	private void parseCondition() throws InvalidCommandException {
		pendingConditions.add(parseConditionTerm());
		while(tokenMaker.peek().equalsIgnoreCase("AND") || tokenMaker.peek().equalsIgnoreCase("OR")) {
			pendingConditionJoins.add(tokenMaker.check().toUpperCase(Locale.ROOT));
			pendingConditions.add(parseConditionTerm());
		}
	}

	private ConditionTerm parseConditionTerm() throws InvalidCommandException {
		String leftAttribute = parseIdentifier();
		String relOp = parseRelOp();
		String rightValue = parseConstantOrIdentifier();
		boolean rightIsAttribute = isUnquotedIdentifierToken(rightValue) && !isNumericLiteral(rightValue);
		return new ConditionTerm(leftAttribute, relOp, rightValue, rightIsAttribute);
	}

	private String parseRelOp() throws InvalidCommandException {
		String token = tokenMaker.check();
		if(token.equals("<") || token.equals(">") || token.equals("<=") || token.equals(">=") || token.equals("=") || token.equals("!=")) {
			return token;
		}
		throw new InvalidCommandException("Invalid relational operator");
	}

	private boolean evaluateCondition(Map<String, String> row, ArrayList<ConditionTerm> conditions, ArrayList<String> joins) throws InvalidCommandException {
		if(conditions.isEmpty()) {
			return true;
		}
		boolean result = evaluateSingleCondition(row, conditions.get(0));
		for(int i = 1; i < conditions.size(); i++) {
			String join = joins.get(i - 1);
			boolean next = evaluateSingleCondition(row, conditions.get(i));
			if(join.equals("AND")) {
				result = result && next;
			} else {
				result = result || next;
			}
		}
		return result;
	}

	private boolean evaluateSingleCondition(Map<String, String> row, ConditionTerm term) throws InvalidCommandException {
		String leftValue = findRowValue(row, term.leftAttribute);
		if(leftValue == null) {
			throw new InvalidCommandException("Unknown attribute in WHERE: " + term.leftAttribute);
		}
		String rightValue;
		if(term.rightIsAttribute) {
			rightValue = findRowValue(row, term.rightValue);
			if(rightValue == null) {
				throw new InvalidCommandException("Unknown attribute in WHERE: " + term.rightValue);
			}
		} else {
			rightValue = normalizeConstantForComparison(term.rightValue);
		}
		return compareValues(leftValue, rightValue, term.relOp);
	}

	private String findRowValue(Map<String, String> row, String attributeName) {
		for(Map.Entry<String, String> entry : row.entrySet()) {
			if(entry.getKey().equalsIgnoreCase(attributeName)) {
				return entry.getValue();
			}
		}
		return null;
	}

	private boolean compareValues(String leftValue, String rightValue, String relOp) {
		if(isNumericLiteral(leftValue) && isNumericLiteral(rightValue)) {
			double left = Double.parseDouble(leftValue);
			double right = Double.parseDouble(rightValue);
			if(relOp.equals("<")) return left < right;
			if(relOp.equals(">")) return left > right;
			if(relOp.equals("<=")) return left <= right;
			if(relOp.equals(">=")) return left >= right;
			if(relOp.equals("=")) return Double.compare(left, right) == 0;
			if(relOp.equals("!=")) return Double.compare(left, right) != 0;
		}
		String left = canonicalCompareValue(leftValue);
		String right = canonicalCompareValue(rightValue);
		int cmp = left.compareTo(right);
		if(relOp.equals("<")) return cmp < 0;
		if(relOp.equals(">")) return cmp > 0;
		if(relOp.equals("<=")) return cmp <= 0;
		if(relOp.equals(">=")) return cmp >= 0;
		if(relOp.equals("=")) return cmp == 0;
		if(relOp.equals("!=")) return cmp != 0;
		return false;
	}

	private void printTableDefinition(TableDefinition definition) {
		System.out.println(definition.tableName.toUpperCase(Locale.ROOT));
		for(ColumnDefinition column : definition.columns) {
			StringBuilder line = new StringBuilder();
			line.append(column.name.toUpperCase(Locale.ROOT)).append(": ");
			if(column.type.equalsIgnoreCase("TEXT")) {
				line.append("Text");
			} else if(column.type.equalsIgnoreCase("INTEGER")) {
				line.append("Integer");
			} else if(column.type.equalsIgnoreCase("FLOAT")) {
				line.append("Float");
			} else {
				line.append(column.type);
			}
			if(column.primaryKey) {
				line.append(" PRIMARY KEY");
			}
			System.out.println(line.toString());
		}
	}

	private TableDefinition readTableDefinition(String tableName) throws IOException, InvalidCommandException {
		return readTableDefinition(getMetaFile(tableName));
	}

	private TableDefinition readTableDefinition(File metaFile) throws IOException, InvalidCommandException {
		if(!metaFile.exists()) {
			throw new InvalidCommandException("Metadata file not found");
		}
		BufferedReader reader = new BufferedReader(new FileReader(metaFile));
		String tableName = reader.readLine();
		if(tableName == null) {
			reader.close();
			throw new InvalidCommandException("Metadata file empty");
		}
		TableDefinition definition = new TableDefinition(tableName.trim());
		String line;
		while((line = reader.readLine()) != null) {
			line = line.trim();
			if(!line.isEmpty()) {
				definition.columns.add(parseColumnLine(line));
			}
		}
		reader.close();
		return definition;
	}

	private ColumnDefinition parseColumnLine(String line) throws InvalidCommandException {
		String[] pieces = line.split("\\|");
		if(pieces.length >= 5) {
			return new ColumnDefinition(pieces[0], pieces[1], Integer.parseInt(pieces[2]), "1".equals(pieces[3]), "1".equals(pieces[4]));
		}
		String[] words = line.split("\\s+");
		if(words.length < 2) {
			throw new InvalidCommandException("Invalid metadata line");
		}
		String name = words[0];
		String type = words[1].toUpperCase(Locale.ROOT);
		int size = type.equals("TEXT") ? 100 : (type.equals("INTEGER") ? 32 : 0);
		boolean primary = false;
		boolean foreign = false;
		if(words.length >= 4 && words[2].equalsIgnoreCase("PRIMARY") && words[3].equalsIgnoreCase("KEY")) {
			primary = true;
		}
		if(words.length >= 4 && words[2].equalsIgnoreCase("FOREIGN") && words[3].equalsIgnoreCase("KEY")) {
			foreign = true;
		}
		if(type.equals("VARCHAR") && words.length >= 3 && isIntegerLiteral(words[2])) {
			type = "TEXT";
			size = Integer.parseInt(words[2]);
		}
		return new ColumnDefinition(name, type, size, primary, foreign);
	}

	private String serializeColumn(ColumnDefinition column) {
		return column.name + "|" + column.type + "|" + column.size + "|" + (column.primaryKey ? "1" : "0") + "|" + (column.foreignKey ? "1" : "0");
	}

	private ArrayList<Map<String, String>> readAllRows(TableDefinition definition) throws IOException {
		ArrayList<Map<String, String>> rows = new ArrayList<Map<String, String>>();
		File recordFile = getRecordFile(definition.tableName);
		if(!recordFile.exists()) {
			return rows;
		}
		BufferedReader reader = new BufferedReader(new FileReader(recordFile));
		String line;
		while((line = reader.readLine()) != null) {
			if(line.trim().isEmpty()) {
				continue;
			}
			ArrayList<String> values = splitRecord(line);
			LinkedHashMap<String, String> row = new LinkedHashMap<String, String>();
			for(int i = 0; i < definition.columns.size(); i++) {
				String value = i < values.size() ? values.get(i) : "";
				row.put(definition.columns.get(i).name, value);
			}
			rows.add(row);
		}
		reader.close();
		return rows;
	}

	private void writeAllRows(TableDefinition definition, ArrayList<Map<String, String>> rows) throws IOException {
		File recordFile = getRecordFile(definition.tableName);
		PrintWriter printer = new PrintWriter(new FileWriter(recordFile));
		for(Map<String, String> row : rows) {
			ArrayList<String> ordered = new ArrayList<String>();
			for(ColumnDefinition column : definition.columns) {
				ordered.add(row.get(column.name));
			}
			printer.println(joinRecord(ordered));
		}
		printer.close();
		rebuildIndex(definition, rows);
	}

	private void rebuildIndex(TableDefinition definition, ArrayList<Map<String, String>> rows) throws IOException {
		ColumnDefinition primaryKey = definition.getPrimaryKeyColumn();
		LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>();
		if(primaryKey != null) {
			ArrayList<Map.Entry<String, Integer>> orderedEntries = new ArrayList<Map.Entry<String, Integer>>();
			for(int i = 0; i < rows.size(); i++) {
				String value = rows.get(i).get(primaryKey.name);
				orderedEntries.add(new java.util.AbstractMap.SimpleEntry<String, Integer>(canonicalCompareValue(value), i));
			}
			Collections.sort(orderedEntries, new Comparator<Map.Entry<String, Integer>>() {
				@Override
				public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b) {
					return a.getKey().compareTo(b.getKey());
				}
			});
			for(Map.Entry<String, Integer> entry : orderedEntries) {
				map.put(entry.getKey(), entry.getValue());
			}
		}
		writeIndexFile(definition.tableName, map);
	}

	private void ensureUniquePrimaryKeys(TableDefinition definition, ArrayList<Map<String, String>> rows) throws InvalidCommandException {
		ColumnDefinition primaryKey = definition.getPrimaryKeyColumn();
		if(primaryKey == null) {
			return;
		}
		LinkedHashSet<String> seen = new LinkedHashSet<String>();
		for(Map<String, String> row : rows) {
			String value = row.get(primaryKey.name);
			if(value == null || value.equalsIgnoreCase("null")) {
				throw new InvalidCommandException("Primary key cannot be null");
			}
			String key = canonicalCompareValue(value);
			if(seen.contains(key)) {
				throw new InvalidCommandException("Duplicate primary key");
			}
			seen.add(key);
		}
	}

	private File getMetaFile(String tableName) {
		return new File(resolveTableName(tableName) + "MetaData-" + currentDB.name + ".txt");
	}

	private File getRecordFile(String tableName) {
		return new File(resolveTableName(tableName) + "Record-" + currentDB.name + ".txt");
	}

	private File getIndexFile(String tableName) {
		return new File(resolveTableName(tableName) + "Index-" + currentDB.name + ".txt");
	}

	private int getAttributeCount(String tableName) throws IOException, InvalidCommandException {
		return readTableDefinition(tableName).columns.size();
	}

	private String getPrimaryKeyValue(TableDefinition definition, List<String> values) {
		for(int i = 0; i < definition.columns.size(); i++) {
			if(definition.columns.get(i).primaryKey) {
				return values.get(i);
			}
		}
		return null;
	}

	private String validateAndNormalizeConstant(String token, ColumnDefinition column) throws InvalidCommandException {
		if(column.type.equalsIgnoreCase("INTEGER")) {
			if(!isIntegerLiteral(token)) {
				throw new InvalidCommandException("Expected integer for " + column.name);
			}
			return String.valueOf(Integer.parseInt(token));
		}
		if(column.type.equalsIgnoreCase("FLOAT")) {
			if(!isNumericLiteral(token)) {
				throw new InvalidCommandException("Expected float for " + column.name);
			}
			return trimDouble(Double.parseDouble(token));
		}
		String unquoted = unquote(token);
		if(unquoted.length() > column.size) {
			throw new InvalidCommandException("Text too long for " + column.name);
		}
		return unquoted;
	}

	private String normalizeConstantForComparison(String token) {
		if(isQuotedString(token)) {
			return unquote(token);
		}
		if(isNumericLiteral(token)) {
			return trimDouble(Double.parseDouble(token));
		}
		return token;
	}

	private ArrayList<String> readAllLines(File file) throws IOException {
		ArrayList<String> lines = new ArrayList<String>();
		if(!file.exists()) {
			return lines;
		}
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line;
		while((line = reader.readLine()) != null) {
			lines.add(line);
		}
		reader.close();
		return lines;
	}

	private Map<String, Integer> readIndexFile(String tableName) throws IOException {
		LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>();
		File indexFile = getIndexFile(tableName);
		if(!indexFile.exists()) {
			return map;
		}
		BufferedReader reader = new BufferedReader(new FileReader(indexFile));
		String line;
		while((line = reader.readLine()) != null) {
			String[] parts = line.split("\\|", 2);
			if(parts.length == 2 && isIntegerLiteral(parts[1])) {
				map.put(parts[0], Integer.parseInt(parts[1]));
			}
		}
		reader.close();
		return map;
	}

	private void writeIndexFile(String tableName, Map<String, Integer> indexMap) throws IOException {
		File indexFile = getIndexFile(tableName);
		PrintWriter printer = new PrintWriter(new FileWriter(indexFile));
		ArrayList<String> keys = new ArrayList<String>(indexMap.keySet());
		Collections.sort(keys);
		for(String key : keys) {
			printer.println(key + "|" + indexMap.get(key));
		}
		printer.close();
	}

	private void rewriteDatabaseFile() throws IOException {
		if(currentDB == null) {
			return;
		}
		PrintWriter printer = new PrintWriter(new FileWriter(currentDB.name + "-DB.txt"));
		printer.println(currentDB.name);
		for(String tableName : currentDB.tableNames) {
			printer.println(tableName);
		}
		printer.close();
	}

	private void ensureTableExists(String tableName) throws InvalidCommandException {
		if(!containsIgnoreCase(currentDB.tableNames, tableName)) {
			throw new InvalidCommandException("Table not found");
		}
	}

	private void requireCurrentDB() throws InvalidCommandException {
		if(currentDB == null) {
			throw new InvalidCommandException("No database selected. Use USE Dbname;");
		}
	}

	private DataBase findDatabase(String databaseName) {
		for(DataBase db : databases) {
			if(db.name.equalsIgnoreCase(databaseName)) {
				return db;
			}
		}
		return null;
	}

	private String resolveTableName(String tableName) {
		for(String name : currentDB.tableNames) {
			if(name.equalsIgnoreCase(tableName)) {
				return name;
			}
		}
		return tableName;
	}

	private String parseIdentifier() throws InvalidCommandException {
		String token = tokenMaker.check();
		if(!isIdentifierToken(token)) {
			throw new InvalidCommandException("Invalid identifier: " + token);
		}
		return token;
	}

	private String parseIdentifierOrStar() throws InvalidCommandException {
		String token = tokenMaker.check();
		if(token.equals("*")) {
			return token;
		}
		if(!isIdentifierToken(token)) {
			throw new InvalidCommandException("Invalid attribute: " + token);
		}
		return token;
	}

	private String parseIdentifierLikeFileName() throws InvalidCommandException {
		String token = tokenMaker.check();
		if(token.equals("EOF") || token.equals(";")) {
			throw new InvalidCommandException("Invalid file name");
		}
		return token;
	}

	private String parseConstantToken() throws InvalidCommandException {
		String token = tokenMaker.check();
		if(token.equals("EOF") || token.equals(",") || token.equals(")") || token.equals(";")) {
			throw new InvalidCommandException("Constant expected");
		}
		return token;
	}

	private String parseConstantOrIdentifier() throws InvalidCommandException {
		String token = tokenMaker.check();
		if(token.equals("EOF") || token.equals(";")) {
			throw new InvalidCommandException("Constant or attribute expected");
		}
		return token;
	}

	private boolean isIdentifierToken(String token) {
		return token.matches("[A-Za-z][A-Za-z0-9_]{0,19}");
	}

	private boolean isUnquotedIdentifierToken(String token) {
		return isIdentifierToken(token);
	}

	private boolean isIntegerLiteral(String token) {
		return token.matches("-?\\d+");
	}

	private boolean isNumericLiteral(String token) {
		return token.matches("-?\\d+(\\.\\d+)?");
	}

	private boolean isQuotedString(String token) {
		return token.length() >= 2 && ((token.startsWith("\"") && token.endsWith("\"")) || (token.startsWith("'") && token.endsWith("'")));
	}

	private String unquote(String token) {
		if(isQuotedString(token)) {
			return token.substring(1, token.length() - 1);
		}
		return token;
	}

	private String canonicalCompareValue(String value) {
		if(value == null) {
			return "";
		}
		if(isNumericLiteral(value)) {
			return trimDouble(Double.parseDouble(value));
		}
		return value.toLowerCase(Locale.ROOT);
	}

	private String trimDouble(double value) {
		if(value == (long) value) {
			return String.valueOf((long) value);
		}
		String text = String.valueOf(value);
		while(text.contains(".") && (text.endsWith("0") || text.endsWith("."))) {
			text = text.substring(0, text.length() - 1);
		}
		return text;
	}

	private String displayValue(String value) {
		return value == null ? "" : value;
	}

	private String formatValueForWriteBack(String storedValue, ColumnDefinition column) {
		if(column.type.equalsIgnoreCase("TEXT")) {
			return "\"" + storedValue + "\"";
		}
		return storedValue;
	}

	private boolean containsIgnoreCase(List<String> items, String target) {
		for(String item : items) {
			if(item.equalsIgnoreCase(target)) {
				return true;
			}
		}
		return false;
	}

	private void removeCaseInsensitive(List<String> items, String target) {
		for(int i = 0; i < items.size(); i++) {
			if(items.get(i).equalsIgnoreCase(target)) {
				items.remove(i);
				return;
			}
		}
	}

	private void removeMatchingFile(List<File> files, String fileName) {
		for(int i = 0; i < files.size(); i++) {
			if(files.get(i).getName().equals(fileName)) {
				files.remove(i);
				return;
			}
		}
	}

	private String joinRecord(List<String> values) {
		ArrayList<String> escaped = new ArrayList<String>();
		for(String value : values) {
			escaped.add(escapeRecordValue(value));
		}
		return String.join("|", escaped);
	}

	private ArrayList<String> splitRecord(String line) {
		ArrayList<String> values = new ArrayList<String>();
		StringBuilder current = new StringBuilder();
		boolean escaping = false;
		for(int i = 0; i < line.length(); i++) {
			char c = line.charAt(i);
			if(escaping) {
				current.append(c);
				escaping = false;
			} else if(c == '\\') {
				escaping = true;
			} else if(c == '|') {
				values.add(current.toString());
				current.setLength(0);
			} else {
				current.append(c);
			}
		}
		values.add(current.toString());
		return values;
	}

	private String escapeRecordValue(String value) {
		return value.replace("\\", "\\\\").replace("|", "\\|");
	}

	// Inner class that splits the original command, stores it, and increments
	// through it.
	public class TokenMaker {
		private String[] tokens;// stores all tokens
		private int position;// keeps track of where in array it currently is

		public TokenMaker(String text) {// constructor
			tokens = tokenize(text);
			position = 0;
		}

		// used for reading a token
		public String check() {// gives current token, increments position
			if(position >= tokens.length) {
				return "EOF";
			}
			position += 1;
			return tokens[position - 1];
		}

		// turn raw input into token
		private String[] tokenize(String input) {
			ArrayList<String> out = new ArrayList<String>();
			StringBuilder current = new StringBuilder();
			boolean inQuotes = false;
			char quoteChar = '\0';
			for(int i = 0; i < input.length(); i++) {
				char c = input.charAt(i);
				if(inQuotes) {
					current.append(c);
					if(c == quoteChar) {
						out.add(current.toString());
						current.setLength(0);
						inQuotes = false;
					}
					continue;
				}
				if(c == '\"' || c == '\'') {
					if(current.length() > 0) {
						out.add(current.toString());
						current.setLength(0);
					}
					inQuotes = true;
					quoteChar = c;
					current.append(c);
					continue;
				}
				if(Character.isWhitespace(c)) {
					if(current.length() > 0) {
						out.add(current.toString());
						current.setLength(0);
					}
					continue;
				}
				if(c == '(' || c == ')' || c == ',' || c == ';' || c == '=' || c == '*') {
					if(current.length() > 0) {
						out.add(current.toString());
						current.setLength(0);
					}
					out.add(String.valueOf(c));
					continue;
				}
				if(c == '!' || c == '<' || c == '>') {
					if(current.length() > 0) {
						out.add(current.toString());
						current.setLength(0);
					}
					if(i + 1 < input.length() && input.charAt(i + 1) == '=') {
						out.add("" + c + '=');
						i++;
					} else {
						out.add(String.valueOf(c));
					}
					continue;
				}
				current.append(c);
			}
			if(current.length() > 0) {
				out.add(current.toString());
			}
			return out.toArray(new String[0]);
		}
		// looks at current token

		private String peek() {
			if (position >= tokens.length)
				return "EOF";
			return tokens[position];
		}

		private String peekNext() {
			if (position + 1 >= tokens.length)
				return "EOF";
			return tokens[position + 1];
		}

		// returns current token and moves to next
		private String advance() {
			if (position >= tokens.length)
				return "EOF";
			return tokens[position++];
		}

		// checks if current token equals what is expected
		private boolean match(String expected) {
			if (peek().equalsIgnoreCase(expected)) {
				advance();
				return true;
			}
			return false;
		}

		private void expect(String expected) throws InvalidCommandException {
			if(!match(expected)) {
				throw new InvalidCommandException(expected + " missing or incorrect");
			}
		}

		private boolean atEnd() {
			return position >= tokens.length || peek().equals("EOF");
		}
	}

	// inner database class
	public class DataBase {
		String name;
		// data members to store tables
		private ArrayList<File> metaDataFiles = new ArrayList<File>();
		private ArrayList<File> recordFiles = new ArrayList<File>();
		private ArrayList<File> indexFiles = new ArrayList<File>();
		private ArrayList<BST> tableTrees = new ArrayList<BST>();
		private ArrayList<String> tableNames = new ArrayList<String>();

		public DataBase(String name) {
			this.name = name;
		}
	}
	
	private static class ColumnDefinition {
		String name;
		String type;
		int size;
		boolean primaryKey;
		boolean foreignKey;

		ColumnDefinition(String name, String type, int size, boolean primaryKey, boolean foreignKey) {
			this.name = name;
			this.type = type;
			this.size = size;
			this.primaryKey = primaryKey;
			this.foreignKey = foreignKey;
		}
	}

	private static class TypeDefinition {
		String kind;
		int size;

		TypeDefinition(String kind, int size) {
			this.kind = kind;
			this.size = size;
		}
	}

	private static class TableDefinition {
		String tableName;
		ArrayList<ColumnDefinition> columns = new ArrayList<ColumnDefinition>();

		TableDefinition(String tableName) {
			this.tableName = tableName;
		}

		ColumnDefinition findColumn(String name) {
			for(ColumnDefinition column : columns) {
				if(column.name.equalsIgnoreCase(name)) {
					return column;
				}
			}
			return null;
		}

		ColumnDefinition getPrimaryKeyColumn() {
			for(ColumnDefinition column : columns) {
				if(column.primaryKey) {
					return column;
				}
			}
			return null;
		}
	}

	private static class ConditionTerm {
		String leftAttribute;
		String relOp;
		String rightValue;
		boolean rightIsAttribute;

		ConditionTerm(String leftAttribute, String relOp, String rightValue, boolean rightIsAttribute) {
			this.leftAttribute = leftAttribute;
			this.relOp = relOp;
			this.rightValue = rightValue;
			this.rightIsAttribute = rightIsAttribute;
		}
	}
	
	//inner exception class for throwing my errors
	public class InvalidCommandException extends Exception {
	    public InvalidCommandException(String message) {
	        super(message);
	    }
	}

}