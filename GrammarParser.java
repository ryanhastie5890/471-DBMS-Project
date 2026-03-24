/*
 * This is the grammar parser class for parsing the commands
 * 
 * TokenMaker inner class controls the initial token creation  of the command as well as advancing through the tokens in the command
 * 
 * beginParse starts examining the command and calls the relevant functions depending on which command it is
 * 
 * ************Need to implement recursive descent parser to start reading the part of the command that comes after the initial call
 * ************For example i think it is needed for the part after CREATE TABLE in CREATE TABLE users(id int PRIMARY KEY, name varchar(10)) but im not sure yet
 */

//tokens[tokenMaker.position] doesnt advance position. tokenMaker.check() does
/*
 * Invalid create command # are just for testing right now, eventually should throw error
 * 
 * many things are currently just for testing and debugging
 * 
 * should probably replace tokens[tokenMaker.position] with a method in TokenMaker that just checks whats in the array but doesnt advance position
 * 
 * can also create method that check if token matches expected just to clean code up a bit
 * 
 * not taking action on command just yet, mainly focusing on validating syntax
 * 
 * very rough draft right now
 * 
 * -Ryan 
 */
public class GrammarParser {
	
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
		else {
			System.out.println("Invalid create command 1");//invalid command detected
		}
		
	}
	
	public void create() {//create command
		if(!tokenMaker.check().equals("CREATE") || !tokenMaker.check().equals("TABLE")) {//checks if it is create table
			System.out.println("Invalid create command 2");
		}
		else {//-----------PICK UP FROM HERE----- start implementing the creation of tables. so far this just verifies the command
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
		
		if(!tokens[tokenMaker.position].equals(",")) {
		parseCreateConstraint();//primary key foreign key stuff
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
	}
	
	public void parseCreateConstraint() {
		System.out.println(tokens[tokenMaker.position] +" and "+tokens[tokenMaker.position +1]);
		if(tokenMaker.check().equals("PRIMARY") && tokenMaker.check().equals("KEY")) {
			System.out.println("Primary key test passed");
		}else {
			System.out.println("Invalid create command 6");
		}
	}
	
	//-------This is where new functions for the command should be added
	
	//Inner class that splits the original command, stores it, and increments through it.
	public class TokenMaker{
		private String[] tokens;//stores all tokens
		private int position;//keeps track of where in array it currently is
		
		public TokenMaker(String text) {//constructor
			tokens = text.replace("("," ( ").replace(")", " ) ").replace(","," , ").split("\\s+"); //add white spaces to ( ) and , then split at white space
		    position = 0;
		}
		
		//used for reading a token
		public String check() {//gives current token, increments position
			position += 1;
			return tokens[position-1];
		}
	
	}
	

}
