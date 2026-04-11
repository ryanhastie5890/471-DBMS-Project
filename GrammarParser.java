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
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;



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
					DataBase db = new DataBase(currentLine);
					this.databases.add(db);
					
					//read table names in the file
					while((currentLine =reader.readLine()) != null) {
						db.tableNames.add(currentLine);
					}
					
					//start adding table files for database
					getTableFiles(db);
				}

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
						   db.metaDataFiles.add(file);	
				}
				if(file.getName().equals(tableName+"Records-"+db.name+".txt")) {//record file found
				
						   db.recordFiles.add(file);	
				}
				if(file.getName().equals(tableName+"Index-"+db.name+".txt")) {//index file found
					
						   db.indexFiles.add(file);	
					
				}
			}
		}
		}
	}
	
	public void beginParse() throws IOException, GrammarParser.InvalidCommandException {// parses the initial input
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
					String databaseName = tokenMaker.check();
					if(tokenMaker.match(";")) {
						//enacting db command
						DataBase db = new DataBase(databaseName);
						//currentDB = db; need to use USE now
						databases.add(db);
						
						//start creating file for database that is titled dbname-DB.txt and contains the names of tables that are under the db
						//this is for initialization to save dbs and tables between sessions
						
						File databaseFile = new File(databaseName + "-DB.txt");
						//write db name as first line of file
						FileWriter dbWrite = new FileWriter(databaseFile,true);
						PrintWriter printer = new PrintWriter(dbWrite);//using to do names on new lines, may not be necessary
						
						printer.println(databaseName);
						printer.close();
						
						System.out.println("Database created");
						
					}
					else {
						throw new InvalidCommandException("Semicolon missing or incorrect");
					}
					
					
					
				} else if (token.equalsIgnoreCase("TABLE")) {
					if (!tokenMaker.match("(")) {// check that table name wasnt skipped

						String tableName = tokenMaker.check();
						
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
						

						if (!tokenMaker.match("(")) {
							throw new InvalidCommandException("( missing or incorrect");
						} else {// begin parsing inside the parentheses
							parseCreateList();

							if (!tokenMaker.match(")")) {
								throw new InvalidCommandException(") missing or incorrect");
							} else {
								if (!tokenMaker.match(";")) {
									throw new InvalidCommandException("Semicolon missing or incorrect");
								} else {
									//System.out.println("Create command parsing done");
									enactCreate();
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
		String name = tokenMaker.check();// name of attribute

		parseCreateType();// type

		if (tokenMaker.peek().equalsIgnoreCase("PRIMARY") || tokenMaker.peek().equalsIgnoreCase("FOREIGN")) {
			parseCreateConstraint();// primary key stuff, add foreign key stuff later
		}
	}

	public void parseCreateType() throws GrammarParser.InvalidCommandException {
		if (tokenMaker.match("VARCHAR")) {

			if (tokenMaker.match("(")) {
				String number = tokenMaker.check();
				if (tokenMaker.match(")")) {
					return;
				} else {
					throw new InvalidCommandException(") missing or incorrect");
				}
			} else {
				throw new InvalidCommandException("( missing or incorrect");
			}
		} else if (tokenMaker.match("INT")) {

		} else {
			throw new InvalidCommandException("Invalid attribute type detected");
		}
	}

	public void parseCreateConstraint() throws GrammarParser.InvalidCommandException {
		// System.out.println(tokens[tokenMaker.position] +" and
		// "+tokens[tokenMaker.position +1]);
		if (tokenMaker.peek().equalsIgnoreCase("PRIMARY") && tokenMaker.peekNext().equalsIgnoreCase("KEY")) {
			// System.out.println("Primary key test passed");
			tokenMaker.check();
			tokenMaker.check();
		} else if (tokenMaker.peek().equalsIgnoreCase("FOREIGN") && tokenMaker.peekNext().equalsIgnoreCase("KEY")) {
			tokenMaker.check();
			tokenMaker.check();
		} else {
			throw new InvalidCommandException("Invalid constraint detected");
		}
	}

	public void enactCreate() throws IOException {// actually does actions based on the valid create table command. havent done
								// database yet
		tokenMaker.position = 2;// start at tablename

		String tableName = tokenMaker.check();
		tokenMaker.check();// burn (

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
			while (!tokenMaker.peek().equals(";")) {
				while(!tokenMaker.match(",") && !tokenMaker.peek().equals(";")) {
				  if (!(tokenMaker.match("(") || tokenMaker.match(")"))) {
					  printer.print(tokenMaker.check() + " ");
				  } else {
                   
				  }
				}
				
				printer.println();
			}
		printer.close();
		
		//need to write to database file which tables belong to each database so when creating table, am writing to currentDb's file
		//the table name. append it

		
		FileWriter dbWrite = new FileWriter((currentDB.name+"-DB.txt"),true);
		printer = new PrintWriter(dbWrite);//using to do names on new lines, may not be necessary
		
		printer.println(tableName);
		printer.close();
		
		System.out.println("Table created");
	}

	// USE command

	public void use() throws GrammarParser.InvalidCommandException {
		tokenMaker.check();//burn USE since already detected
		String dbName = tokenMaker.advance();
		
		//check if dbName is in list of databases
		Boolean found = false;
		DataBase foundDb = null;
		for(int i = 0; i < databases.size(); i ++) {
			DataBase database = databases.get(i);
			if (database.name.equals(dbName)) {
				found = true;
				foundDb = database;
			}
		}
		
		if(!found) {
			throw new InvalidCommandException("Database not found");
		}else {
			this.currentDB = foundDb;
			System.out.println("Switched db to: "+dbName);
		}
	}
	// inserts
	public void insert() {// prototype insert command read
		if (!tokenMaker.check().equalsIgnoreCase("INSERT")) {// checks if it is create table
			System.out.println("Invalid insert command 1");
		} else {
			String name = tokenMaker.check();

			if (!tokenMaker.check().equalsIgnoreCase("VALUES")) {// check that table name wasnt skipped

				System.out.println("Invalid insert command 2");

			} else {
				if (!tokenMaker.check().equalsIgnoreCase("(")) {
					System.out.println("Invalid insert command 3");
				} else {
					parseInsertList();
				}
			}
		}
	}

	public void parseInsertList() {// insert version of parse list
		int i = 1;
		parseInsertValues(i);// start parsing first attribute

		while (tokens[tokenMaker.position].equalsIgnoreCase(",")) {// hopefully takes each attribute and begins parsing it
			tokenMaker.check();// advance past comma
			parseInsertValues(++i);// continue parsing attributes
		}
	}

	public void parseInsertValues(int attributeNumber) {
		System.out.println("Attribute " + attributeNumber + ": " + tokenMaker.check());
		// need to check domain key and entity integrity constraints
	}

	// beginning of describe command
	public void describe() throws IOException {
		if (!tokenMaker.check().equalsIgnoreCase("DESCRIBE") || !tokenMaker.check().equals("(")) {
			System.out.println("Invalid describe command 1");
		} else {
			String command = tokenMaker.check();
			if (command.equalsIgnoreCase("ALL")) {
				describeAll();
			} else {
				describeTable(command);
			}
		}
	}

	public void describeAll() throws IOException {
		//System.out.println("describe all test passed");
		
        for(int i = 0; i < currentDB.metaDataFiles.size(); i++) {
        	File file = currentDB.metaDataFiles.get(i);
        	
        	BufferedReader reader = new BufferedReader(new FileReader(file));
    	    String currentLine;
    	    
    	    while((currentLine = reader.readLine())!=null) {//prints out whole meta data file for table
    	    	System.out.println(currentLine);
    	    }
    	    reader.close();
    	    System.out.println();
        }
	}

	public void describeTable(String tableName) throws IOException {
		//System.out.println("describe table test passed table: " + tableName);
	    File file = new File(tableName+"MetaData-"+currentDB.name+".txt");
	
	    BufferedReader reader = new BufferedReader(new FileReader(file));
	    String currentLine;
	    
	    while((currentLine = reader.readLine())!=null) {//prints out whole meta data file for table
	    	System.out.println(currentLine);
	    }
	    reader.close();
	}

	// beginning of rename command
	public void rename() {
		tokenMaker.check();// burn a token
		String tableName = tokenMaker.check();

		if (!tokenMaker.check().equals("(")) {
			System.out.println("Invalid rename command 1");
		} else {
			renameList();
		}
	}

	public void renameList() {
		int i = 1;
		parseRenameAttributes(i);// start parsing first attribute

		while (tokens[tokenMaker.position].equals(",")) {// hopefully takes each attribute and begins parsing it
			tokenMaker.check();// advance past comma
			parseRenameAttributes(++i);// continue parsing attributes
		}
	}

	public void parseRenameAttributes(int attributeNumber) {
		// test statement
		System.out.println("Attribute " + attributeNumber + ": " + tokenMaker.check());
	}

	// begin update methods
	public void update() {
		tokenMaker.check();// burn UPDATE
		String tableName = tokenMaker.check();

		if (!tokenMaker.check().equalsIgnoreCase("SET")) {
			System.out.println("Invalid update command 1");
		} else {
			parseUpdateList();

			// start parsing where statement
		}
	}

	public void parseUpdateList() {
		int i = 1;
		parseUpdateValues(i);// start parsing first attribute

		while (tokenMaker.position < tokens.length && tokens[tokenMaker.position].equals(",")) {// hopefully takes each
																								// attribute and begins
																								// parsing it
			tokenMaker.check();// advance past comma
			parseUpdateValues(++i);// continue parsing attributes
		}
	}

	public void parseUpdateValues(int attributeNumber) {
		String name = tokenMaker.check();

		if (!tokenMaker.check().equals("=")) {
			System.out.println("Invalid update command 2");
		}

		String value = tokenMaker.check();

		System.out.println("Received constant " + attributeNumber + ": " + name + " " + value);
	}

	// select
	public void select() {
		if (!tokenMaker.check().equalsIgnoreCase("SELECT")) {
			System.out.println("Invalid select command 1");
		} else {
			selectNameList();

			if (!tokenMaker.check().equalsIgnoreCase("FROM")) {
				System.out.println("Invalid select command 2");
			} else {
				selectTablesList();

				// start where portion
			}
		}
	}

	public void selectNameList() {
		int i = 1;
		parseAttributeNames(i);// start parsing first attribute

		while (tokenMaker.position < tokens.length && tokens[tokenMaker.position].equals(",")) {// hopefully takes each
																								// attribute and begins
																								// parsing it
			tokenMaker.check();// advance past comma
			parseAttributeNames(++i);// continue parsing attributes
		}
	}

	public void parseAttributeNames(int nameNumber) {
		System.out.println("Received Attribute " + nameNumber + ": " + tokenMaker.check());
	}

	public void selectTablesList() {
		int i = 1;
		parseTableNames(i);// start parsing first attribute

		while (tokenMaker.position < tokens.length && tokens[tokenMaker.position].equals(",")) {// hopefully takes each
																								// attribute and begins
																								// parsing it
			tokenMaker.check();// advance past comma
			parseTableNames(++i);// continue parsing attributes
		}
	}

	public void parseTableNames(int tableNumber) {
		System.out.println("Received table " + tableNumber + ": " + tokenMaker.check());
	}

	// commands below this line are not as developed as the commands above yet
	// let
	public void let() {
		if (!tokenMaker.check().equalsIgnoreCase("LET")) {
			System.out.println("Invalid let command 1");
		} else {
			String tableName = tokenMaker.check();

			if (!tokenMaker.check().equalsIgnoreCase("KEY")) {
				System.out.println("Invalid let command 2");
			} else {
				String attributeName = tokenMaker.check();

				System.out.println("let test passed");
				// select part
			}
		}
	}

	// delete
	public void delete() {
		tokenMaker.check();// dont need the first if in some of the other commands since it checks in the
							// other method

		String tableName = tokenMaker.check();

		System.out.println("delete test passed");
		// where part
	}

	// input
	public void input() {
		String fileName1 = tokenMaker.check();

		System.out.println("input test passed");
	}

	// exit taken care of in DBMS.java
	
	// -------This is where new functions for the command should be added

	// Inner class that splits the original command, stores it, and increments
	// through it.
	public class TokenMaker {
		private String[] tokens;// stores all tokens
		private int position;// keeps track of where in array it currently is

		public TokenMaker(String text) {// constructor
			tokens = text.replace("(", " ( ").replace(")", " ) ").replace(",", " , ").replace(";", " ; ").replace("=", " = ").split("\\s+"); 
			
			position = 0;
		}

		// used for reading a token
		public String check() {// gives current token, increments position

			position += 1;
			return tokens[position - 1];
		}

		// turn raw input into token
		private String[] tokenize(String input) {
			input = input.replace("(", " ( ").replace(")", " ) ").replace(",", " , ").replace(";", " ; ").replace("=",
					" = ");

			return input.trim().toUpperCase().split("\\s+");
		}
		// looks at current token

		private String peek() {
			if (position >= tokens.length)
				return "EOF";
			return tokens[position];
		}

		private String peekNext() {
			if (position >= tokens.length)
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
	
	//inner exception class for throwing my errors
	public class InvalidCommandException extends Exception {
	    public InvalidCommandException(String message) {
	        super(message);
	    }
	}

}
