/*
 * This is the main DBMS file for the 471 Project by Ishaq Ahmed, Justin Haynes, Ryan Hastie, Faisal Abusara
 * 
 * This file will hold the logic for the dbms
 */

import java.io.IOException;
import java.util.Scanner;

public class DBMS {
	
	public static void main(String [] args) throws IOException {
		Scanner scn = new Scanner(System.in);
		
		//BST test
		BST bst = new BST();
		int[] arr = {2,31,24,12};
		bst.createTree(arr);
		bst.insertValue(55);
		bst.deletion(24);
		bst.deletion(31);
		bst.printAll();
		
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
				doneReading=false;
			}
			}
			
			if(command.contains("EXIT")) {
				done = true;
				System.out.println("Thank you for using this DBMS");
				continue;
			}
			parser.setCommand(command); 
			parser.beginParse();
		}
	}

}
