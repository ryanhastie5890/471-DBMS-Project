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
public class GrammarParser {//just checking synatx so far
	
	private TokenMaker tokenMaker;//utilization of inner class
	private String[] tokens;//tokens from inner class
	
	
	public GrammarParser(String text) {//constructor
		tokenMaker = new TokenMaker(text);
		tokens = tokenMaker.tokens;
	}
	
	public void beginParse() {//parses the initial input
		String current = tokens[tokenMaker.position];
		
		//add an else if for each command and then call a function that parses the relevant command
		if(current.equals("CREATE")) {
			create();
		}
		else if(current.equals("INSERT")) {
			insert();
		}
		else if(current.equals("DESCRIBE")) {
			describe();
		}
		else if(current.equals("RENAME")) {
			rename();
		}
		else if(current.equals("UPDATE")) {
			update();
		}
		else if(current.equals("SELECT")){
			select();
		}
		else if(current.equals("LET")) {
			let();
		}
		else if(current.equals("DELETE")) {
			delete();
		}
		else if(current.equals("INPUT")) {
			input();
		}
		else if(current.equals("EXIT")) {
			exit();
		}
		else {
			System.out.println("Invalid create command 1");//invalid command detected
		}
		
	}
	
	public void create() {//create command
		if(!tokenMaker.check().equals("CREATE") || !tokenMaker.check().equals("TABLE")) {//checks if it is create table
			System.out.println("Invalid create command 2");
		}
		else {
			System.out.println("Test Passed");//havent tested after this, still a work in progress
			
			
			if(tokens[tokenMaker.position]!= "("){//check that table name wasnt skipped
			
				String tableName = tokenMaker.check();//
				
				if(!tokenMaker.check().equals("(")) {
					System.out.println("Invalid create command 3");
				}
				else {//begin parsing inside the parentheses
					parseCreateList();
				}
				
			}
			else {
				System.out.println("Table must have a name");
			}
			}
	}
	
	//methods work in progress, untested so far
	public void parseCreateList() {//starts parsing create command after the '('
		parseCreateDefinition();//start parsing first attribute
		
		while(tokens[tokenMaker.position].equals(",")) {//hopefully takes each attribute and begins parsing it
		    tokenMaker.check();//advance past comma
			parseCreateDefinition();//continue parsing attributes
		}
	}
	
	public void parseCreateDefinition() {
		String name = tokenMaker.check();//name of attribute
		
		parseCreateType();//type
		
		if(tokens[tokenMaker.position].equals("PRIMARY")) {
		parseCreateConstraint();//primary key stuff, add foreign key stuff later
		}
	}
	
	public void parseCreateType() {
		if(tokens[tokenMaker.position].equals("VARCHAR")) {
			tokenMaker.check();
			if(tokenMaker.check().equals("(")) {
				String number = tokenMaker.check();
				if(tokenMaker.check().equals(")")) {
					return;
				}
				else {
					System.out.println("Invalid create command 4");
				}
			}
			else {
				System.out.println("Invalid create command 5");
			}
		}
		else if(tokens[tokenMaker.position].equals("INT")) {
			tokenMaker.check();
		}
		else {
			System.out.println("Invalid create command 7");		}
	}
	
	public void parseCreateConstraint() {
		System.out.println(tokens[tokenMaker.position] +" and "+tokens[tokenMaker.position +1]);
		if(tokenMaker.check().equals("PRIMARY") && tokenMaker.check().equals("KEY")) {
			System.out.println("Primary key test passed");
		}else {
			System.out.println("Invalid create command 6");
		}
	}
	
	//insert currently doesnt read a command that spans multiple lines, only works for a single line command atm
	public void insert() {//prototype insert command read
		if(!tokenMaker.check().equals("INSERT")) {//checks if it is create table
			System.out.println("Invalid insert command 1");
		}
		else {
			String name = tokenMaker.check();
			
			if(!tokenMaker.check().equals("VALUES")){//check that table name wasnt skipped
			
				System.out.println("Invalid insert command 2");
				
			}
			else {
				if(!tokenMaker.check().equals("(")) {
					System.out.println("Invalid insert command 3");
				}
				else {
				parseInsertList();
				}
			}
			}
	}
	
	public void parseInsertList() {//insert version of parse list
		int i = 1;
        parseInsertValues(i);//start parsing first attribute
		
		while(tokens[tokenMaker.position].equals(",")) {//hopefully takes each attribute and begins parsing it
		    tokenMaker.check();//advance past comma
			parseInsertValues(++i);//continue parsing attributes
		}
	}
	
	public void parseInsertValues(int attributeNumber) {
		System.out.println("Attribute "+attributeNumber+": "+tokenMaker.check());
		//need to check domain key and entity integrity constraints
	}
	
	
	//beginning of describe command
	public void describe() {
		if(!tokenMaker.check().equals("DESCRIBE") || !tokenMaker.check().equals("(")) {
			System.out.println("Invalid describe command 1");
		}
		else {
			String command = tokenMaker.check();
			if(command.equals("ALL")) {
				describeAll();
			}
			else {
				describeTable(command);
			}
		}
	}
	
	public void describeAll() {
		System.out.println("describe all test passed");
	}
	public void describeTable(String tableName) {
		System.out.println("describe table test passed table: "+tableName);
	}
	
	//beginning of rename command
	public void rename() {
		tokenMaker.check();//burn a token
		String tableName = tokenMaker.check();
		
		if(!tokenMaker.check().equals("(")) {
			System.out.println("Invalid rename command 1");
		}else {
			renameList();
		}
	}
	
	public void renameList() {
		int i = 1;
        parseRenameAttributes(i);//start parsing first attribute
		
		while(tokens[tokenMaker.position].equals(",")) {//hopefully takes each attribute and begins parsing it
		    tokenMaker.check();//advance past comma
			parseRenameAttributes(++i);//continue parsing attributes
		}
	}
	
	public void parseRenameAttributes(int attributeNumber) {
		//test statement
		System.out.println("Attribute "+attributeNumber+": "+tokenMaker.check());
	}
	
	//begin update methods
	public void update() {
		tokenMaker.check();//burn UPDATE
		String tableName = tokenMaker.check();
		
		if(!tokenMaker.check().equals("SET")) {
			System.out.println("Invalid update command 1");
		}else {
			parseUpdateList();
			
			//start parsing where statement
		}
	}
	
	public void parseUpdateList() {
		int i = 1;
        parseUpdateValues(i);//start parsing first attribute
		
        
		while(tokenMaker.position < tokens.length && tokens[tokenMaker.position].equals(",")) {//hopefully takes each attribute and begins parsing it
		    tokenMaker.check();//advance past comma
			parseUpdateValues(++i);//continue parsing attributes
		}
	}
	
	public void parseUpdateValues(int attributeNumber) {
		String name = tokenMaker.check();
		
		if(!tokenMaker.check().equals("=")) {
			System.out.println("Invalid update command 2");
		}
		
		String value = tokenMaker.check();
		
		System.out.println("Received constant "+attributeNumber+": "+name+" "+value);
	}
	
	//select
	public void select() {
		if(!tokenMaker.check().equals("SELECT")) {
			System.out.println("Invalid select command 1");
		}
		else {
			selectNameList();
			
			if(!tokenMaker.check().equals("FROM")) {
				System.out.println("Invalid select command 2");
			}
			else {
				selectTablesList();
				
				//start where portion
			}
		}
	}
	
	public void selectNameList() {
		int i = 1;
        parseAttributeNames(i);//start parsing first attribute
		
		while(tokenMaker.position < tokens.length &&tokens[tokenMaker.position].equals(",")) {//hopefully takes each attribute and begins parsing it
		    tokenMaker.check();//advance past comma
			parseAttributeNames(++i);//continue parsing attributes
		}
	}
	
	public void parseAttributeNames(int nameNumber) {
		System.out.println("Received Attribute "+nameNumber+": "+tokenMaker.check());
	}
	
	public void selectTablesList() {
		int i = 1;
        parseTableNames(i);//start parsing first attribute
		
		while(tokenMaker.position < tokens.length &&tokens[tokenMaker.position].equals(",")) {//hopefully takes each attribute and begins parsing it
		    tokenMaker.check();//advance past comma
			parseTableNames(++i);//continue parsing attributes
		}
	}
	
	public void parseTableNames(int tableNumber) {
		System.out.println("Received table "+tableNumber+": "+tokenMaker.check());
	}
	
	//commands below this line are not as developed as the commands above yet
	//let
	public void let() {
		if(!tokenMaker.check().equals("LET")) {
			System.out.println("Invalid let command 1");
		}
		else {
			String tableName = tokenMaker.check();
			
			if(!tokenMaker.check().equals("KEY")) {
				System.out.println("Invalid let command 2");
			}else{
				String attributeName = tokenMaker.check();
				
				System.out.println("let test passed");
				//select part
			}
		}
	}
	
	//delete
	public void delete() {
		tokenMaker.check();//dont need the first if in some of the other commands since it checks in the other method
	
		String tableName = tokenMaker.check();
		
		System.out.println("delete test passed");
		//where part
	}
	//input
	public void input() {
		String fileName1 = tokenMaker.check();
		
		System.out.println("input test passed");
	}
	//exit
	public void exit() {
		System.out.println("exit test passed");
	}
	//-------This is where new functions for the command should be added

	
	//Inner class that splits the original command, stores it, and increments through it.
	public class TokenMaker{
		private String[] tokens;//stores all tokens
		private int position;//keeps track of where in array it currently is
		
		public TokenMaker(String text) {//constructor
			tokens = text.replace("("," ( ").replace(")", " ) ").replace(","," , ").replace("=", " = ").split("\\s+"); //add white spaces to ( ) and , then split at white space
		    position = 0;
		}
		
		//used for reading a token
		public String check() {//gives current token, increments position
			position += 1;
			return tokens[position-1];
		}
	
	}
	

}
