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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class GrammarParser {

	private TokenMaker tokenMaker;// utilization of inner class
	private String[] tokens;// tokens from inner class

	// database storage
	private ArrayList<DataBase> databases = new ArrayList<DataBase>();
	private DataBase currentDB = null;

	public GrammarParser() {// empty constructor

	}

	public GrammarParser(String text) {// constructor
		tokenMaker = new TokenMaker(text);
		tokens = tokenMaker.tokens;

	}

	public void setCommand(String command) {
		tokenMaker = new TokenMaker(command);
		tokens = tokenMaker.tokens;
	}
	
	//this is initialization that pulls all databases and files
	public void init() {
		
	}

	public void beginParse() throws IOException, GrammarParser.InvalidCommandException {// parses the initial input
		String current = tokens[tokenMaker.position];

		// add an else if for each command and then call a function that parses the
		// relevant command
		if (current.equals("CREATE")) {
			create();
		} else if (current.equals("INSERT")) {
			insert();
		} else if (current.equals("DESCRIBE")) {
			describe();
		} else if (current.equals("RENAME")) {
			rename();
		} else if (current.equals("UPDATE")) {
			update();
		} else if (current.equals("SELECT")) {
			select();
		} else if (current.equals("LET")) {
			let();
		} else if (current.equals("DELETE")) {
			delete();
		} else if (current.equals("INPUT")) {
			input();
		} else if (current.equals("USE")) {
			use();
		} else {
			throw new InvalidCommandException("Invalid command detected");
			
		}

	}

	public void create() throws IOException, GrammarParser.InvalidCommandException {// create command
		if (!tokenMaker.match("CREATE")) {// checks if it is create table
			throw new InvalidCommandException("CREATE missing or incorrect");
		} else {
			if (!(tokenMaker.peek().equals("TABLE") || tokenMaker.peek().equals("DATABASE"))) {
				throw new InvalidCommandException("TABLE or DATABASE not found or incorrect");
			} else {

				String token = tokenMaker.check();
				if (token.equals("DATABASE")) {
					DataBase db = new DataBase(tokenMaker.check());
					//currentDB = db; need to use USE now
					databases.add(db);
					System.out.println("Database created");
				} else if (token.equals("TABLE")) {
					if (!tokenMaker.match("(")) {// check that table name wasnt skipped

						String tableName = tokenMaker.check();
						
						//verify table name doesnt exist
						Boolean found = false;
						if (currentDB == null) {
							throw new InvalidCommandException("No database selected. Use USE command first.");
						}
						ArrayList<String> tables = this.currentDB.tableNames;
				
						for(int i = 0; i < tables.size(); i ++) {
							String checkName = tables.get(i);
							if (checkName.equals(tableName)) {
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
									System.out.println("Create command parsing done");
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

		if (tokenMaker.peek().equals("PRIMARY") || tokenMaker.peek().equals("FOREIGN")) {
			parseCreateConstraint();// primary key stuff, add foreign key stuff later
		}
	}

//  support assignment data types
public void parseCreateType() throws GrammarParser.InvalidCommandException {
    if (tokenMaker.match("INTEGER")) {
        return;
    } else if (tokenMaker.match("TEXT")) {
        return;
    } else if (tokenMaker.match("FLOAT")) {
        return;
    } else {
        throw new InvalidCommandException("Invalid attribute type detected");
    }
}

	public void parseCreateConstraint() throws GrammarParser.InvalidCommandException {
		// System.out.println(tokens[tokenMaker.position] +" and
		// "+tokens[tokenMaker.position +1]);
		if (tokenMaker.peek().equals("PRIMARY") && tokenMaker.peekNext().equals("KEY")) {
			// System.out.println("Primary key test passed");
			tokenMaker.check();
			tokenMaker.check();
		} else if (tokenMaker.peek().equals("FOREIGN") && tokenMaker.peekNext().equals("KEY")) {
			tokenMaker.check();
			tokenMaker.check();
		} else {
			throw new InvalidCommandException("Invalid constraint detected");
		}
	}

	public void enactCreate() {// actually does actions based on the valid create table command. havent done
								// database yet
		tokenMaker.position = 2;// start at tablename

		String tableName = tokenMaker.check();
		tokenMaker.check();// burn (

		// create tables and store in data members

		// meta data
		File metaDataFile = new File(tableName + "MetaData.txt");
		currentDB.metaDataFiles.add(metaDataFile);

		// record data
		File recordFile = new File(tableName + "Record.txt");
		currentDB.recordFiles.add(recordFile);

		// index
		File indexFile = new File(tableName + "Index.txt");
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

		try (FileWriter writer = new FileWriter(metaDataFile)) {
			writer.write(tableName + " , ");

			while (!tokenMaker.match(";")) {
				if (!(tokenMaker.match("(") || tokenMaker.match(")"))) {
					writer.write(tokenMaker.check() + " ");
				} else {

				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

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
			// semicolon enforcement
			}
		}
			if (!tokenMaker.match(";")) {
				throw new InvalidCommandException("Semicolon missing after USE");
			}
		
		if(!found) {
			throw new InvalidCommandException("Database not found");
		}else {
			this.currentDB = foundDb;
			System.out.println("Switched db to: "+dbName);
		}
	}
// INSERT parsing
public void insert() {
	if (!tokenMaker.check().equals("INSERT")) {
		System.out.println("Invalid insert command 1");
	} else {
		String tableName = tokenMaker.check();

		if (!tokenMaker.check().equals("VALUES")) {
			System.out.println("Invalid insert command 2");
		} else {
			if (!tokenMaker.check().equals("(")) {
				System.out.println("Invalid insert command 3");
			} else {
				parseInsertList();

				
				if (!tokenMaker.check().equals(")")) {
					System.out.println("Invalid insert command 4");
				}
				
				else if (!tokenMaker.check().equals(";")) {
					System.out.println("Invalid insert command 5");
				} else {
					System.out.println("Insert command parsing done for table: " + tableName);
				}
			}
		}
	}
}

// safer insert list parsing
public void parseInsertList() {
	int i = 1;
	parseInsertValues(i);

	while (tokenMaker.position < tokens.length && tokens[tokenMaker.position].equals(",")) {
		tokenMaker.check(); // advance past comma
		parseInsertValues(++i);
	}
}

// insert value parser
public void parseInsertValues(int attributeNumber) {
	String value = tokenMaker.check();
	System.out.println("Value " + attributeNumber + ": " + value);
	// later: check domain constraints, key constraints, entity integrity constraints
}

	// beginning of describe command
// NEW CODE ADDED: full DESCRIBE parsing
public void describe() {
	if (!tokenMaker.check().equals("DESCRIBE")) {
		System.out.println("Invalid describe command 1");
		return;
	}

	if (!tokenMaker.check().equals("(")) {
		System.out.println("Invalid describe command 2");
		return;
	}

	String command = tokenMaker.check();

	if (!tokenMaker.check().equals(")")) {
		System.out.println("Invalid describe command 3");
		return;
	}

	if (!tokenMaker.check().equals(";")) {
		System.out.println("Invalid describe command 4");
		return;
	}

	if (command.equals("ALL")) {
		describeAll();
	} else {
		describeTable(command);
	}
}

// you can keep these if you want, but here are fuller versions

// describe all placeholder
public void describeAll() {
	System.out.println("DESCRIBE ALL parsing done");
	// later: loop through currentDB tables and print metadata
}

//RENAME parsing
public void rename() {
	if (!tokenMaker.check().equals("RENAME")) {
		System.out.println("Invalid rename command 1");
		return;
	}

	String tableName = tokenMaker.check();

	if (!tokenMaker.check().equals("(")) {
		System.out.println("Invalid rename command 2");
		return;
	}

	renameList();

	if (!tokenMaker.check().equals(")")) {
		System.out.println("Invalid rename command 3");
		return;
	}

	if (!tokenMaker.check().equals(";")) {
		System.out.println("Invalid rename command 4");
		return;
	}

	System.out.println("Rename command parsing done for table: " + tableName);
}

//  rename list parsing
public void renameList() {
	int i = 1;
	parseRenameAttributes(i);

	while (tokenMaker.position < tokens.length && tokens[tokenMaker.position].equals(",")) {
		tokenMaker.check(); // advance past comma
		parseRenameAttributes(++i);
	}
}

// NEW CODE ADDED: rename attribute parser
public void parseRenameAttributes(int attributeNumber) {
	String newName = tokenMaker.check();
	System.out.println("Rename attribute " + attributeNumber + " to: " + newName);
}

	// begin update methods
// 
public void update() {
    tokenMaker.check(); // burn UPDATE
    String tableName = tokenMaker.check();

    if (!tokenMaker.check().equals("SET")) {
        System.out.println("Invalid update command 1");
    } else {
        parseUpdateList();

        try {
            parseWhereClause();
            if (!tokenMaker.match(";")) {
                throw new InvalidCommandException("Missing semicolon in UPDATE");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
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
// WHERE condition parsing
public void parseWhereClause() throws InvalidCommandException {
    if (tokenMaker.match("WHERE")) {
        parseCondition();
    }
}

public void parseCondition() throws InvalidCommandException {
    parseSingleCondition();

    while (tokenMaker.peek().equals("AND") || tokenMaker.peek().equals("OR")) {
        tokenMaker.check(); // consume AND/OR
        parseSingleCondition();
    }
}

public void parseSingleCondition() throws InvalidCommandException {
    String left = tokenMaker.check();

    String op = tokenMaker.check();
    if (!(op.equals("=") || op.equals("<") || op.equals(">") ||
          op.equals("<=") || op.equals(">=") || op.equals("!="))) {
        throw new InvalidCommandException("Invalid relational operator");
    }

    String right = tokenMaker.check();
}
	// select
// FIX 3: select should actually use WHERE + ;
public void select() {
    if (!tokenMaker.check().equals("SELECT")) {
        System.out.println("Invalid select command 1");
    } else {
        selectNameList();

        if (!tokenMaker.check().equals("FROM")) {
            System.out.println("Invalid select command 2");
        } else {
            selectTablesList();

            try {
                parseWhereClause();
                if (!tokenMaker.match(";")) {
                    throw new InvalidCommandException("Missing semicolon in SELECT");
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
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
		if (!tokenMaker.check().equals("LET")) {
			System.out.println("Invalid let command 1");
		} else {
			String tableName = tokenMaker.check();

			if (!tokenMaker.check().equals("KEY")) {
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
		tokenMaker.check(); // DELETE
		String tableName = tokenMaker.check();

		// NEW CODE ADDED
		try {
			parseWhereClause();

			if (!tokenMaker.match(";")) {
				throw new InvalidCommandException("Missing semicolon in DELETE");
			}

			System.out.println("delete test passed");
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	// input
	public void input() throws InvalidCommandException {
		if (!tokenMaker.match("INPUT")) {
			throw new InvalidCommandException("INPUT missing or incorrect");
		}

		String fileName = tokenMaker.advance();

		// NEW CODE ADDED: optional OUTPUT
		if (tokenMaker.match("OUTPUT")) {
			String outFile = tokenMaker.advance();
			System.out.println("Writing output to: " + outFile);
		}

		if (!tokenMaker.match(";")) {
			throw new InvalidCommandException("Missing semicolon in INPUT");
		}

		System.out.println("Input test passed, file name: " + fileName);
	}

	// exit taken care of in DBMS.java
	
	// -------This is where new functions for the command should be added

	// Inner class that splits the original command, stores it, and increments
	// through it.
	public class TokenMaker {
		private String[] tokens;// stores all tokens
		private int position;// keeps track of where in array it currently is

// NEW CODE ADDED: improved tokenizer (case-insensitive + semicolon support)
public TokenMaker(String text) {
    tokens = text
        .replace(">=", " >= ")
        .replace("<=", " <= ")
        .replace("!=", " != ")
        .replace("(", " ( ")
        .replace(")", " ) ")
        .replace(",", " , ")
        .replace(";", " ; ")
        .replace("=", " = ")
        .replace("<", " < ")
        .replace(">", " > ")
        .trim()
        .toUpperCase()
        .split("\\s+");
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
			if (position + 1>= tokens.length)
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
			if (peek().equals(expected)) {
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
