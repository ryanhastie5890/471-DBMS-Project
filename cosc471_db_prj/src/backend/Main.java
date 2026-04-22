package backend;

import java.io.IOException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        try (Scanner scn = new Scanner(System.in)) {
            System.out.println("Welcome to DBMS");

            GrammarParser parser = new GrammarParser();
            boolean done = false;

            while (!done) {
                System.out.println("Enter Command:");

                StringBuilder command = new StringBuilder();
                boolean reading = true;

                while (reading && scn.hasNextLine()) {
                    String currentLine = scn.nextLine();
                    command.append(currentLine).append(" ");
                    if (currentLine.contains(";")) {
                        reading = false;
                    }
                }

                String fullCommand = command.toString().trim();
                if (fullCommand.isEmpty()) {
                    continue;
                }

                parser.setCommand(fullCommand);

                try {
                    String firstToken = fullCommand.split("\\s+")[0];
                    if (firstToken.equalsIgnoreCase("EXIT")) {
                        done = true;
                        System.out.println("Thank you for using this DBMS");
                        continue;
                    }

                    parser.beginParse();
                } catch (GrammarParser.InvalidCommandException e) {
                    System.out.println("Command Failed, error message: " + e.getMessage());
                } catch (Exception e) {
                    System.out.println("Command Failed, unexpected error: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.out.println("Startup failed: " + e.getMessage());
        }
    }
}