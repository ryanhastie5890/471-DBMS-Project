/*
 * This is the main DBMS file for the 471 Project by Ishaq Ahmed, Justin Haynes, and Ryan Hastie
 * 
 * This file will hold the logic for the dbms
 */

import java.util.Scanner;

public class DBMS {
	
	public static void main(String [] args) {
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
			GrammarParser parser = new GrammarParser(command);
			parser.beginParse();
		}
	}

}
