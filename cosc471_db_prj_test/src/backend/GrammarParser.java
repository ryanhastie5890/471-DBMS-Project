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
package backend;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Scanner;

import backend.GrammarParser.InvalidCommandException;



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

	//used in calling create backend
	private String currentPrimaryKey; //used in calling create backend to avoid passing back through 4 functions
	private ArrayList<String> currentColNames;
	private ArrayList<String> currentColTypes;
	
    private ArrayList<String> currentValues;
	
	private ArrayList<String> insertTypes;
	private int keyLocation;
	
	StorageManager storage  = new StorageManager();
    QueryExecutor  executor = new QueryExecutor(storage);
    Parser         parser   = new Parser();
	
	public GrammarParser() throws IOException {// empty constructor
        init();
	}

	public GrammarParser(String text) throws IOException {// constructor
		tokenMaker = new TokenMaker(text);
		tokens = tokenMaker.tokens;
		init();

	}

	public void setCommand(String command) {//sets command to be parsed
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
		
		//get directories
		File[] directories = root.listFiles((File::isDirectory));//get directories
		   
	     
		
		if(directories != null) {
			for(File directory : directories) {
				
				String name = directory.getName();
				
				//ignore src, bin, and settings
				if(name.equals("src")||name.equals("bin")||name.equals(".settings")) {
					continue;
				}
				
				//create database object
				DataBase db = new DataBase(name);
				this.databases.add(db);
				
				//get tables for databases
				getTableFiles(db);
			}
		}
	}
	
	public void getTableFiles(DataBase db) throws IOException {
		File dbDirectory = new File(db.name);
		
		File[] files = dbDirectory.listFiles();//find files within directory
		
		if(files != null) {
			for(File file : files) {
				String tableName = file.getName();
				
				if(tableName.endsWith(".dat")) {//if there is a table.dat file then add to table name
					tableName = tableName.substring(0, tableName.length()-4);
					db.tableNames.add(tableName);
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
			throw new InvalidCommandException("No valid command type was detected. This DBMS supports create, insert, describe, rename, update, select, let, delete, input, use, and exit");
			
		}

	}

	public void create() throws IOException, GrammarParser.InvalidCommandException {// create command
		
		
		
		this.currentColNames = new ArrayList<String>();
		this.currentColTypes = new ArrayList<String>();
		this.currentPrimaryKey = null; //reset data members
		
		if (!tokenMaker.match("CREATE")) {// checks if it is create table
			throw new InvalidCommandException(tokenMaker.peek()+" found, but CREATE expected");//invalid
		} else {
			if (!(tokenMaker.peek().equalsIgnoreCase("TABLE") || tokenMaker.peek().equalsIgnoreCase("DATABASE"))) {
				throw new InvalidCommandException(tokenMaker.peek()+" found, but TABLE or DATABASE expected");//invalid
			} else {

				String token = tokenMaker.check();
				if (token.equalsIgnoreCase("DATABASE")) {//database case
					String databaseName = tokenMaker.check();
					if(tokenMaker.match(";")) {
						//enacting db command
						DataBase db = new DataBase(databaseName);
						//currentDB = db; need to use USE now
						databases.add(db);
						
						//start creating file for database that is titled dbname-DB.txt and contains the names of tables that are under the db
						//this is for initialization to save dbs and tables between sessions
						
						//File databaseFile = new File(databaseName + "-DB.txt");
						//write db name as first line of file
						//FileWriter dbWrite = new FileWriter(databaseFile,true);
						//PrintWriter printer = new PrintWriter(dbWrite);//using to do names on new lines, may not be necessary
						
						//printer.println(databaseName);
						//printer.close();
						executor.execute(parser.createDb(databaseName));
						//System.out.println("Database created");
						
					}
					else {
						throw new InvalidCommandException(tokenMaker.peek()+" found, but ; expected");
					}
					
					
					
				} else if (token.equalsIgnoreCase("TABLE")) {//table case
					if(this.currentDB == null) {
						throw new InvalidCommandException("Please select a database");
					}
					if (!tokenMaker.match("(")) {// check that table name wasnt skipped

						String tableName = tokenMaker.check();
						
						//verify table name doesnt exist
						Boolean found = false;
						ArrayList<String> tables = this.currentDB.tableNames;
						for(int i = 0; i < tables.size(); i ++) {//check f there is already this table name
							String checkName = tables.get(i);
							if (checkName.equalsIgnoreCase(tableName)) {
								found = true;
								throw new InvalidCommandException("Table already exists");
							}
						}
						

						if (!tokenMaker.match("(")) {
							throw new InvalidCommandException(tokenMaker.peek()+" found, but ( expected");//invalid
						} else {// begin parsing inside the parentheses
							parseCreateList();

							if (!tokenMaker.match(")")) {
								throw new InvalidCommandException(tokenMaker.peek()+" found, but ) expected");
							} else {
								if (!tokenMaker.match(";")) {
									throw new InvalidCommandException(tokenMaker.peek()+" found, but ; expected");
								} else {
									//System.out.println("Create command parsing done");
									String [] nameArray = (String[]) this.currentColNames.toArray(new String[0]);
									String [] typeArray = (String[]) this.currentColTypes.toArray(new String[0]);
									//call create command
									executor.execute(parser.createTable(tableName,nameArray,typeArray ,this.currentPrimaryKey)) ;	
									this.currentDB.tableNames.add(tableName);}
							}
						}

					} else {
						throw new InvalidCommandException("Table name not found");
					}
				} else {
					throw new InvalidCommandException(tokenMaker.peek()+" found, but TABLE or DATABASE expected");
				}
			}
		}
	}

	public void parseCreateList() throws GrammarParser.InvalidCommandException {// starts parsing create command after the '('
		parseCreateDefinition();// start parsing first attribute

		while (tokenMaker.match(",")) {// hopefully takes each attribute and begins parsing it
			// tokenMaker.check();//advance past comma

			parseCreateDefinition();// continue parsing attributes
		}
	}

	public void parseCreateDefinition() throws GrammarParser.InvalidCommandException {
		String name = tokenMaker.check();// name of attribute
        this.currentColNames.add(name);
		
		parseCreateType();// type

		if (tokenMaker.peek().equalsIgnoreCase("PRIMARY") || tokenMaker.peek().equalsIgnoreCase("FOREIGN")) {
			parseCreateConstraint();// primary key stuff, add foreign key stuff later
		}
	}

	public void parseCreateType() throws GrammarParser.InvalidCommandException {
		if (tokenMaker.match("VARCHAR")) {//varchar not really used, may comment out

			if (tokenMaker.match("(")) {
				String number = tokenMaker.check();
				if (tokenMaker.match(")")) {
					this.currentColTypes.add("VARCHAR("+number+")");
					return;
				} else {
					throw new InvalidCommandException(tokenMaker.peek()+" found, but ) expected");
				}
			} else {
				throw new InvalidCommandException(tokenMaker.peek()+" found, but ( expected");
			}
		} else if (tokenMaker.match("INT") || tokenMaker.match("INTEGER")) {
          this.currentColTypes.add("INT");
		} else if(tokenMaker.match("FLOAT")) {
		   this.currentColTypes.add("FLOAT");
		}else if(tokenMaker.match("TEXT")) {
			this.currentColTypes.add("TEXT");
		}
		else {
			throw new InvalidCommandException("Invalid attribute type detected");//invalid
		}
	}

	public void parseCreateConstraint() throws GrammarParser.InvalidCommandException {
		// System.out.println(tokens[tokenMaker.position] +" and
		// "+tokens[tokenMaker.position +1]);
		if (tokenMaker.peek().equalsIgnoreCase("PRIMARY") && tokenMaker.peekNext().equalsIgnoreCase("KEY")) {
			// System.out.println("Primary key test passed");
			tokenMaker.position--;
			tokenMaker.position--;
			this.currentPrimaryKey = tokenMaker.check();//collect primary key
			//System.out.println(currentPrimaryKey);
			tokenMaker.check();
			tokenMaker.check();
			tokenMaker.check();
		} else if (tokenMaker.peek().equalsIgnoreCase("FOREIGN") && tokenMaker.peekNext().equalsIgnoreCase("KEY")) {
			tokenMaker.check();
			tokenMaker.check();
		} else {
			throw new InvalidCommandException("Invalid constraint detected");
		}
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
			//call use command
			executor.execute(parser.useDb(dbName));
			//System.out.println("Switched db to: "+dbName);
		}
	}
	// inserts
		public void insert() throws InvalidCommandException, FileNotFoundException {// prototype insert command read
			if(this.currentDB == null) {
				throw new InvalidCommandException("Please select a database");
			}
			
			this.currentValues = new ArrayList<String>();//reset list
			
			this.insertTypes = new ArrayList<String>();
			this.keyLocation = 0;
			
			if (!tokenMaker.match("INSERT")) {// checks if it is create table
				throw new InvalidCommandException(tokenMaker.peek()+" found, but INSERT expected");
			} else {
				String name = tokenMaker.check();

				if (!tokenMaker.match("VALUES")) {// check that table name wasnt skipped and values is ppresent

					throw new InvalidCommandException(tokenMaker.peek()+" found, but VALUES expected");

				} else {
					if (!tokenMaker.match("(")) {
						throw new InvalidCommandException(tokenMaker.peek()+" found, but ( expected");
					} else {
						parseInsertList(name);//check values
						
						if(!tokenMaker.match(")")) {
							throw new InvalidCommandException(tokenMaker.peek()+" found, but ) expected");
						}
						if(!tokenMaker.match(";")) {
							throw new InvalidCommandException(tokenMaker.peek()+" found, but ; expected");
						}
						
						String [] values = (String[]) this.currentValues.toArray(new String[0]);
						//call command
						//System.out.println("executing insert");
						
						executor.execute(parser.insertInto(name,values ));
				}
			}}
		}

		public void parseInsertList(String sentName) throws InvalidCommandException, FileNotFoundException {// insert version of parse list
			sentName = sentName.trim();
			//get file
	        File dbDirectory = new File(currentDB.name);
			
			File[] files = dbDirectory.listFiles();//find files within directory
			
			File tableFile = null;
			
			if(files != null) {//loop through files in db directory
				for(File file : files) {
					String tableName = file.getName();
					
					//System.out.println(sentName.toUpperCase()+".meta");
					if(tableName.equals(sentName.toUpperCase()+".meta")) {
						tableFile = file;	
					}
				}
				if(tableFile == null) {
					throw new InvalidCommandException("Could not find table");
				}
				
			}else {
				throw new InvalidCommandException("Could not find table");
			}
			
			//get position of primary key and create an array of types
			int pKey = 0;
			String [] types;
			
			//read meta data
			Scanner scn = new Scanner(new File(tableFile.getAbsolutePath()));
			
			
			int count = Integer.parseInt(scn.nextLine().trim());
			//System.out.println(count);
			types = new String[count];
			
			String keyText = scn.nextLine().trim();
			
			for(int i = 0; i < count; i++) {
				String line = scn.nextLine().trim();
				
				String []split = line.split("\\s+");
				
				String name = split[0];
				String type = split[1];
				
				type = type.toUpperCase();//formatting
				if(type.contains("INT") || type.contains("INTEGER")){
					type = "INT";
				}
				else if(type.contains("TEXT")) {
					type = "TEXT";
				}
				else if(type.contains("FLOAT")) {
					type = "FLOAT";
				}
				
				if(name.equals(keyText)) {
					pKey = i;
				}
				this.insertTypes.add(type);
			}
			this.keyLocation = pKey;
			
			int i = 1;
			if(i > count) {
				throw new InvalidCommandException("Wrong number of attributes");
			}
			parseInsertValues(i);// start parsing first attribute

			while (tokens[tokenMaker.position].equalsIgnoreCase(",")) {// hopefully takes each attribute and begins parsing it
				tokenMaker.check();// advance past comma
				
				i = i +1;
				if(i > count) {
					throw new InvalidCommandException("Wrong number of attributes");
				}
				parseInsertValues(i);// continue parsing attributes
			}
			
			
		}

		public void parseInsertValues(int attributeNumber) throws InvalidCommandException {
			//System.out.println("Attribute " + attributeNumber + ": " + tokenMaker.check());
			// need to check domain key and entity integrity constraints
			
			String currentValue = tokenMaker.check();
		
			this.currentValues.add(currentValue);
			
			//check domain constraint
			if(this.insertTypes.get(attributeNumber-1).equalsIgnoreCase("INT")||this.insertTypes.get(attributeNumber-1).equalsIgnoreCase("INTEGER")) {
				if(isInt(currentValue)) {
					
				}else {
					throw new InvalidCommandException(currentValue+" is not an integer");
				}
			}
			else if(this.insertTypes.get(attributeNumber-1).equalsIgnoreCase("TEXT")) {
				if(!isInt(currentValue)&& !isFloat(currentValue)) {
					
				}else {
					throw new InvalidCommandException(currentValue+" is not valid for text");
				}
			}
			else if(this.insertTypes.get(attributeNumber-1).equalsIgnoreCase("FLOAT")) {
				if(isFloat(currentValue)) {
					
				}
				else {
					throw new InvalidCommandException(currentValue+" is not a float");
				}
			}
			
			//check entity
			if((attributeNumber-1) == this.keyLocation) {
				if(this.currentValues.get(attributeNumber-1).equalsIgnoreCase("null")){
					throw new InvalidCommandException("Primary key can not be null");
				}
			}
			
			//need to check if unique?
				
			//this.currentValues.add(currentValue);
			
		}
		
		//checks if int
		public static boolean isInt(String string) {
		    try {
		        Integer.parseInt(string);
		        return true;
		    } catch (NumberFormatException e) {
		        return false;
		    }
		}
		
		//checks if float
		public static boolean isFloat(String string) {
		    try {
		        Double.parseDouble(string);
		        return true;
		    } catch (NumberFormatException e) {
		    	return false;
		    }
		}


	// beginning of describe command
	public void describe() throws IOException, GrammarParser.InvalidCommandException {
		if(this.currentDB == null) {
			throw new InvalidCommandException("Please select a database");
		}
		
		tokenMaker.check();//burn DESCRIBE token

		
			String command = tokenMaker.check();
			//System.out.println("Command = "+command);
			if (command.equalsIgnoreCase("ALL")) {
				
				 if(tokenMaker.match(";")) {
				    for (int i = 0; i < this.currentDB.tableNames.size(); i ++) {//loop through and describe all tables
				    	executor.execute(parser.describe(this.currentDB.tableNames.get(i)));
				    }
				 }
				 else {
					 throw new InvalidCommandException(tokenMaker.peek()+" found, but ; expected");
				 }
				
			} else {
				
					 if(tokenMaker.match(";")) {
						 executor.execute(parser.describe(command));//call one describe
					 }
					 else {
						 throw new InvalidCommandException(tokenMaker.peek()+" found, but ; expected");
					 }
				
				
			}
		}
	

	

	// beginning of rename command
	public void rename() throws GrammarParser.InvalidCommandException, IOException {
		
		if(this.currentDB == null) {
			throw new InvalidCommandException("Please select a database");
		}
		
		tokenMaker.check();// burn a token
		String tableName = tokenMaker.check();
		
		Path path = Paths.get(this.currentDB.name, (tableName.toUpperCase()+".dat"));//find table file
		
		if(Files.exists(path)&& Files.isRegularFile(path)) {
			if (!tokenMaker.check().equals("(")) {
				throw new InvalidCommandException(tokenMaker.peek()+" found, but ( expected");
			} else {
				renameList(tableName);
			}
		}else{
			throw new InvalidCommandException("Could not find table");
		}
	/*if(!currentDB.tableNames.contains(tableName)) {
			throw new InvalidCommandException("Could not find table");
		}*/

		
	}

	public void renameList(String tableName) throws GrammarParser.InvalidCommandException, IOException {
		
		int i = 1;
		parseRenameAttributes(i);// start parsing first attribute
		tokenMaker.check();

		while (tokens[tokenMaker.position].equals(",")) {// hopefully takes each attribute and begins parsing it
			tokenMaker.check();// advance past comma
			tokenMaker.check();
			parseRenameAttributes(++i);// continue parsing attributes
		}
		
		//check for ) and ;
		if(!tokenMaker.match(")")) {
			//System.out.println(tokens[tokenMaker.position]);
			throw new InvalidCommandException(tokenMaker.peek()+" found, but ) expected");
		}
		if(!tokenMaker.match(";")) {
			throw new InvalidCommandException(tokenMaker.peek()+" found, but ; expected");
		}
		
		//make sure number of attributes is correct
		//int numAttributes = -1;//-1 so table name wont count
		
		//find meta data file 
		/*File file = new File(tableName+"MetaData-"+currentDB.name+".txt");
		
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
		*/
		int numAttributes = 0;//make sure number of attributes is correct
		
		Path path = Paths.get(this.currentDB.name, (tableName.toUpperCase()+".meta"));
		
		Optional<String> line1 = Files.lines(path).findFirst();
		
		if(line1.isPresent()) {
			numAttributes = Integer.parseInt(line1.get().trim());
		}
		
		if(numAttributes!=i) {
			throw new InvalidCommandException("Incorrect number of attributes");
		}
		//collect new names
		String [] names = new String [numAttributes];
		tokenMaker.position = 0;//reset to get names
		tokenMaker.check();
		tokenMaker.check();
		tokenMaker.check();
		for(int j = 0; j < names.length; j ++)
		{
			if(tokenMaker.peek().equals(",")) {
				tokenMaker.check();
			}
			names[j] = tokenMaker.check();
		}
		
		//call rename
		executor.execute(parser.rename(tableName, names));
		
	}
	

	

	public void parseRenameAttributes(int attributeNumber) {
		// test statement
		//System.out.println("Attribute " + attributeNumber + ": " + tokenMaker.check());
	}

	// begin update methods
	public void update() throws InvalidCommandException {
		if(this.currentDB == null) {
			throw new InvalidCommandException("Please select a database");
		}
		try {
			tokenMaker.check();// burn UPDATE
			String tableName = tokenMaker.check();

			if (!tokenMaker.check().equalsIgnoreCase("SET")) {
				throw new InvalidCommandException(tokenMaker.peek()+" found, but SET expected");
			} else {
				ArrayList<String> setColumns = new ArrayList<String>();
				ArrayList<String> setValues = new ArrayList<String>();

				parseUpdateList(setColumns, setValues);

				String whereLeft = null;
				String whereOp = null;
				String whereRight = null;
				String whereConnector = null;
				String whereLeft2 = null;
				String whereOp2 = null;
				String whereRight2 = null;

				if (tokenMaker.peek().equalsIgnoreCase("WHERE")) {
					tokenMaker.check();// burn WHERE

					whereLeft = tokenMaker.check();
					whereOp = tokenMaker.check();

					if (!(whereOp.equals("=") || whereOp.equals("!=") || whereOp.equals("<")
							|| whereOp.equals(">") || whereOp.equals("<=") || whereOp.equals(">="))) {
						throw new InvalidCommandException("Invalid relational operator");
					}

					whereRight = tokenMaker.check();

					if (tokenMaker.peek().equalsIgnoreCase("AND") || tokenMaker.peek().equalsIgnoreCase("OR")) {
						whereConnector = tokenMaker.check();

						whereLeft2 = tokenMaker.check();
						whereOp2 = tokenMaker.check();

						if (!(whereOp2.equals("=") || whereOp2.equals("!=") || whereOp2.equals("<")
								|| whereOp2.equals(">") || whereOp2.equals("<=") || whereOp2.equals(">="))) {
							throw new InvalidCommandException("Invalid relational operator");
						}

						whereRight2 = tokenMaker.check();
					}
				}
				else {
					//throw new InvalidCommandException(tokenMaker.peek()+" found, but WHERE expected");
				}
				if (!tokenMaker.match(";")) {
					throw new InvalidCommandException(tokenMaker.peek()+" found, but ; expected");
				}

				String[] setColumnsArray = setColumns.toArray(new String[0]);
				String[] setValuesArray = setValues.toArray(new String[0]);

				executor.execute(parser.update(tableName, setColumnsArray, setValuesArray, whereLeft, whereOp, whereRight,whereConnector,whereLeft2,whereOp2,whereRight2));
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	public void parseUpdateList(ArrayList<String> setColumns, ArrayList<String> setValues) throws InvalidCommandException {
		parseUpdateValues(setColumns, setValues);// start parsing first attribute

		while (tokenMaker.position < tokens.length && tokens[tokenMaker.position].equals(",")) {// hopefully takes each
																								// attribute and begins
																								// parsing it
			tokenMaker.check();// advance past comma
			parseUpdateValues(setColumns, setValues);// continue parsing attributes
		}
	}

	public void parseUpdateValues(ArrayList<String> setColumns, ArrayList<String> setValues) throws InvalidCommandException {
		String name = tokenMaker.check();

		if (!tokenMaker.match("=")) {
			throw new InvalidCommandException(tokenMaker.peek()+" found, but = expected");
		}

		String value = tokenMaker.check();

		setColumns.add(name);
		setValues.add(value);
	}

		// select
	public void select() throws InvalidCommandException {
		if(this.currentDB == null) {
			throw new InvalidCommandException("Please select a database");
		}
		try {
			if (!tokenMaker.check().equalsIgnoreCase("SELECT")) {
				throw new InvalidCommandException(tokenMaker.peek()+" found, but SELECT expected");
			} else {
				ArrayList<String> selectedColumns = new ArrayList<String>();
				selectedColumns.add(tokenMaker.check());

				while (tokenMaker.peek().equals(",")) {
					tokenMaker.check();// burn comma
					selectedColumns.add(tokenMaker.check());
				}
				

				if (!tokenMaker.match("FROM")) {
					throw new InvalidCommandException(tokenMaker.peek()+" found, but FROM expected");
				} else {
					//get table names
					ArrayList<String> tableNamesList = new ArrayList<String>();
					while(tokenMaker.position < tokenMaker.tokens.length &&!tokenMaker.peek().equalsIgnoreCase("WHERE") && !tokenMaker.peek().equalsIgnoreCase(";")){
						if(!tokenMaker.peek().equalsIgnoreCase(",")) {
							tableNamesList.add(tokenMaker.check());
						}else if(tokenMaker.peek().equalsIgnoreCase(",")){
							tokenMaker.check();
						}
					}
					
					String[] tableNames = tableNamesList.toArray(new String[0]);
					//String tableName = tokenMaker.check();

					String whereLeft = null;
					String whereOp = null;
					String whereRight = null;

					String whereConnector = null;
					String whereLeft2 = null;
					String whereOp2 = null;
					String whereRight2 = null;

					if (tokenMaker.peek().equalsIgnoreCase("WHERE")) {
						tokenMaker.check();// burn WHERE

						whereLeft = tokenMaker.check();
						whereOp = tokenMaker.check();

						if (!(whereOp.equals("=") || whereOp.equals("!=") || whereOp.equals("<")
								|| whereOp.equals(">") || whereOp.equals("<=") || whereOp.equals(">="))) {
							throw new InvalidCommandException("Invalid relational operator");
						}

						whereRight = tokenMaker.check();

						if (tokenMaker.peek().equalsIgnoreCase("AND") || tokenMaker.peek().equalsIgnoreCase("OR")) {
							whereConnector = tokenMaker.check();

							whereLeft2 = tokenMaker.check();
							whereOp2 = tokenMaker.check();

							if (!(whereOp2.equals("=") || whereOp2.equals("!=") || whereOp2.equals("<")
									|| whereOp2.equals(">") || whereOp2.equals("<=") || whereOp2.equals(">="))) {
								throw new InvalidCommandException("Invalid relational operator");
							}

							whereRight2 = tokenMaker.check();
						}
					}
					else {
						//throw new InvalidCommandException(tokenMaker.peek()+" found, but WHERE expected");
					}

					

					String[] columnsArray = selectedColumns.toArray(new String[0]);

					executor.execute(parser.select(tableNames, columnsArray, whereLeft, whereOp, whereRight,
							whereConnector, whereLeft2, whereOp2, whereRight2));
					
					//executor.execute(parser.select(tableNames, columnsArray, whereLeft, whereOp, whereRight
							//));
					
					/*
					 * 
					executor.execute(parser.select(tableNames, columnsArray, whereLeft, whereOp, whereRight,
							whereConnector, whereLeft2, whereOp2, whereRight2));
					 */
				}
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
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

	// let
	public void let() throws InvalidCommandException {
		if(this.currentDB == null) {
			throw new InvalidCommandException("Please select a database");
		}
		if (!tokenMaker.check().equalsIgnoreCase("LET")) {
			throw new InvalidCommandException(tokenMaker.peek()+" found, but LET expected");
		} else {
			String newTableName = tokenMaker.check();

			if (!tokenMaker.check().equalsIgnoreCase("KEY")) {
				throw new InvalidCommandException(tokenMaker.peek()+" found, but KEY expected");
			} else {
				String keyName = tokenMaker.check();

				// **********Start of select stuff, mainly same as the select
				
				
				try {
					if (!tokenMaker.check().equalsIgnoreCase("SELECT")) {
						throw new InvalidCommandException(tokenMaker.peek()+" found, but SELECT expected");
					} else {
						ArrayList<String> selectedColumns = new ArrayList<String>();
						selectedColumns.add(tokenMaker.check());

						while (tokenMaker.peek().equals(",")) {
							tokenMaker.check();// burn comma
							selectedColumns.add(tokenMaker.check());
						}

						if (!tokenMaker.check().equalsIgnoreCase("FROM")) {
							throw new InvalidCommandException(tokenMaker.peek()+" found, but FROM expected");
						} else {
							//get table names
							ArrayList<String> tableNamesList = new ArrayList<String>();
							while(tokenMaker.position < tokenMaker.tokens.length &&!tokenMaker.peek().equalsIgnoreCase("WHERE") && !tokenMaker.peek().equalsIgnoreCase(";")){
								if(!tokenMaker.peek().equalsIgnoreCase(",")) {
									tableNamesList.add(tokenMaker.check());
								}else if(tokenMaker.peek().equalsIgnoreCase(",")){
									tokenMaker.check();
								}
							}
							
							String[] tableNames = tableNamesList.toArray(new String[0]);
							//String tableName = tokenMaker.check();

							String whereLeft = null;
							String whereOp = null;
							String whereRight = null;

							String whereConnector = null;
							String whereLeft2 = null;
							String whereOp2 = null;
							String whereRight2 = null;

							if (tokenMaker.peek().equalsIgnoreCase("WHERE")) {
								tokenMaker.check();// burn WHERE

								whereLeft = tokenMaker.check();
								whereOp = tokenMaker.check();

								if (!(whereOp.equals("=") || whereOp.equals("!=") || whereOp.equals("<")
										|| whereOp.equals(">") || whereOp.equals("<=") || whereOp.equals(">="))) {
									throw new InvalidCommandException("Invalid relational operator");
								}

								whereRight = tokenMaker.check();

								if (tokenMaker.peek().equalsIgnoreCase("AND") || tokenMaker.peek().equalsIgnoreCase("OR")) {
									whereConnector = tokenMaker.check();

									whereLeft2 = tokenMaker.check();
									whereOp2 = tokenMaker.check();

									if (!(whereOp2.equals("=") || whereOp2.equals("!=") || whereOp2.equals("<")
											|| whereOp2.equals(">") || whereOp2.equals("<=") || whereOp2.equals(">="))) {
										throw new InvalidCommandException("Invalid relational operator");
									}

									whereRight2 = tokenMaker.check();
								}
							}
							else {
								//throw new InvalidCommandException(tokenMaker.peek()+" found, but WHERE expected");
							}

							

							String[] columnsArray = selectedColumns.toArray(new String[0]);

						     //******* this is where the let call will be
							executor.execute(parser.let(newTableName,keyName,tableNames,columnsArray,whereLeft,whereOp,whereRight, whereConnector, whereLeft2, whereOp2, whereRight2));
							this.currentDB.tableNames.add(newTableName);
						}
					}
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
				
				
				//*****************End of select stuff
				
			}
		}
	}

	// delete
	public void delete() throws InvalidCommandException {
		if(this.currentDB == null) {
			throw new InvalidCommandException("Please select a database");
		}
		try {
			tokenMaker.check();// burn DELETE
			String tableName = tokenMaker.check();

			String whereLeft = null;
			String whereOp = null;
			String whereRight = null;

			String whereConnector = null;
			String whereLeft2 = null;
			String whereOp2 = null;
			String whereRight2 = null;
			
			Boolean isWhere = false;
			if (tokenMaker.peek().equalsIgnoreCase("WHERE")) {
				tokenMaker.check();// burn WHERE

				whereLeft = tokenMaker.check();
				whereOp = tokenMaker.check();

				if (!(whereOp.equals("=") || whereOp.equals("!=") || whereOp.equals("<")
						|| whereOp.equals(">") || whereOp.equals("<=") || whereOp.equals(">="))) {
					throw new InvalidCommandException("Invalid relational operator");
				}

				whereRight = tokenMaker.check();

				if (tokenMaker.peek().equalsIgnoreCase("AND") || tokenMaker.peek().equalsIgnoreCase("OR")) {
					whereConnector = tokenMaker.check();

					whereLeft2 = tokenMaker.check();
					whereOp2 = tokenMaker.check();

					if (!(whereOp2.equals("=") || whereOp2.equals("!=") || whereOp2.equals("<")
							|| whereOp2.equals(">") || whereOp2.equals("<=") || whereOp2.equals(">="))) {
						throw new InvalidCommandException("Invalid relational operator");
					}

					whereRight2 = tokenMaker.check();
				}
			}
			else {
				//throw new InvalidCommandException(tokenMaker.peek()+" found, but WHERE expected");
			}


			if (!tokenMaker.match(";")) {
				throw new InvalidCommandException(tokenMaker.peek()+" found, but ; expected");
			}

			executor.execute(parser.delete(tableName, whereLeft, whereOp, whereRight, whereConnector, whereLeft2, whereOp2,whereRight2));
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	// input
	public void input() throws IOException, InvalidCommandException {
		tokenMaker.check();
		String fileName1 = tokenMaker.check();

		if(tokenMaker.match(";")){
			runFile(fileName1);
		}else {
			String fileName2 = tokenMaker.check();
			if(tokenMaker.match(";")) {
				runFileOutput(fileName1, fileName2);
			}else {
				throw new InvalidCommandException(tokenMaker.peek()+" found, but ; expected");
			}
		}
		System.out.println("Done running input file");
	}
	
	public void runFile(String fileName) throws IOException, InvalidCommandException {//runs a txt file of commands line by line

		
		try {
		File inputFile = new File(fileName);
		Scanner reader = new Scanner(inputFile);
		
		reader.useDelimiter(";");//stops at ; now
		
		while(reader.hasNext()) {
			String command = reader.next().trim();//trim to remove new lines and whitespace
			
			if(command.equalsIgnoreCase("EXIT")) {//exits
				System.out.println("Thank you for using this DBMS");
				break;
			}
			
			command = command+";";//re add ;
			
			System.out.println();
			System.out.println("Running command: "+command);
			
			setCommand(command);
			try {
				beginParse();//perfor command
				}catch (InvalidCommandException e) {
					System.out.println("ERROR: "+e.getMessage());
				}
			
		}
		}
		catch (FileNotFoundException e) {//didnt find file
			throw new InvalidCommandException("Could not find input file");
		}
	}
	
public void runFileOutput(String fileName, String fileNameOutput) throws IOException, InvalidCommandException {//runs a txt file of commands line by line

		PrintStream originalOut = System.out;//console
		
		try {
		File inputFile = new File(fileName);
		File outputFile = new File(fileNameOutput);
		
		Scanner reader = new Scanner(inputFile);
		
		reader.useDelimiter(";");//stops at ; now
		
		//capture in output file
		PrintStream fileOut = new PrintStream(new FileOutputStream(outputFile));
		System.setOut(fileOut);
		
		while(reader.hasNext()) {
			String command = reader.next().trim();//trim to remove new lines and whitespace
			
			if(command.equalsIgnoreCase("EXIT")) {//exits
				System.out.println("Thank you for using this DBMS");
				break;
			}
			
			command = command+";";//re add ;
			System.out.println();
			System.out.println("Running command: "+command);
			
			setCommand(command);
		
			try {
			beginParse();//perfor command
			}catch (InvalidCommandException e) {
				System.out.println("ERROR: "+e.getMessage());
			}
			
		}
		reader.close();
		fileOut.close();
		}
		catch (FileNotFoundException e) {//didnt find file
			throw new InvalidCommandException("Could not find input file");
		}finally {
			System.setOut(originalOut);//restore output
		}
	}

	// exit taken care of in DBMS.java
	
	// -------This is where new functions for the command should be added

	// Inner class that splits the original command, stores it, and increments
	// through it.
	public class TokenMaker {
		private String[] tokens;// stores all tokens
		private int position;// keeps track of where in array it currently is

		public TokenMaker(String text) {// constructor
			text = text.replace(">=", " @GE@ ");
			text = text.replace("<=", " @LE@ ");
			text = text.replace("!=", " @NE@ ");

			text = text.replace("(", " ( ");
			text = text.replace(")", " ) ");
			text = text.replace(",", " , ");
			text = text.replace(";", " ; ");
			text = text.replace("=", " = ");
			text = text.replace("<", " < ");
			text = text.replace(">", " > ");

			text = text.replace("@GE@", ">=");
			text = text.replace("@LE@", "<=");
			text = text.replace("@NE@", "!=");

			tokens = text.split("\\s+");

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
		//private ArrayList<BST> tableTrees = new ArrayList<BST>();
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
/*
 * old enact create command
 * 
 * 
	public void enactCreate() throws IOException, GrammarParser.InvalidCommandException {// actually does actions based on the valid create table command. havent done
								// database yet
		tokenMaker.position = 2;// start at tablename

		String tableName = tokenMaker.check();
		if(currentDB.tableNames.contains(tableName)) {
			throw new InvalidCommandException("Table already exists in database");
		}
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
	
	old rename command
	
	public void enactRename(File file) throws IOException {
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
	    
	    while((currentLine = reader.readLine())!=null) {
	    	
	    	if(lineNumber >= 2) {//not table line so split
	    		String[] words = currentLine.split("\\s+");
	    		
	    		if(words.length > 0) {
	    			words[0] = tokenMaker.check();//replace the name
	    			
	    			tokenMaker.check();//burn comma
	    		}
	    		
	    		currentLine = String.join(" ", words);//join back
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
	}
old describe commands

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
 * 
 * 
 * old copy of read database files:
 * public void readDatabaseFiles() throws IOException {//----read all databases, create database, add files
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
	
	old copy of get table files
	
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
 */

