package backend;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

/**
 * Entry point for the DBMS.
 *
 * Right now it drives the hardcoded test queries in Parser.
 * Later: swap Parser() for a version that reads from stdin or a file.
 */
public class Main {
    
	public static void main(String [] args) throws IOException, GrammarParser.InvalidCommandException {
		Scanner scn = new Scanner(System.in);
		
		System.out.println("Welcome to DBMS");
		
		
		GrammarParser parser = new GrammarParser();
		Boolean done = false;
		while(!done) {
			System.out.println("Enter Command:");
			
			Boolean doneReading = true;
			String command = "";
			while(doneReading) {
			
			String currentLine = scn.nextLine();
			command = command + currentLine+" ";
			if(currentLine.contains(";")) {
				doneReading=false;//stop reading after ; hit
			}
			}
			
			String upper = command.toUpperCase();
			if(upper.contains("EXIT")) {//if exit then stop
				done = true;
				System.out.println("Thank you for using this DBMS");
				continue;
			}
			
			parser.setCommand(command); 
			try {
			parser.beginParse();//run command
			}
			catch (GrammarParser.InvalidCommandException e){
				System.out.println("Command Failed, error message: "+e.getMessage());
			}
		}
	}

	
	
	
	
	
	/*hardcoded
	 * 
	 *
	public static void main(String[] args) {
        StorageManager storage  = new StorageManager();
        QueryExecutor  executor = new QueryExecutor(storage);
        Parser         parser   = new Parser();

        System.out.println("=== DBMS Started ===\n");

        while (parser.hasNext()) {
            ParsedQuery q = parser.getNextQuery();
            System.out.print("> [" + q.queryType + "] ");
            if (q.tableName != null) System.out.print("table=" + q.tableName + " ");
            if (q.dbName    != null) System.out.print("db="    + q.dbName    + " ");
            System.out.println();

            executor.execute(q);
            System.out.println();
        }

        System.out.println("=== Done ===");
    }
    */
}
