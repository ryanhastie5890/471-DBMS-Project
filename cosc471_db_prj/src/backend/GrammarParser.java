package backend;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Scanner;

public class GrammarParser {

    private TokenMaker tokenMaker;
    private String[] tokens;

    // database storage
    private ArrayList<DataBase> databases = new ArrayList<DataBase>();
    private DataBase currentDB = null;

    // used during create/insert parsing
    private String currentPrimaryKey;
    private ArrayList<String> currentColNames;
    private ArrayList<String> currentColTypes;
    private ArrayList<String> currentValues;
    private ArrayList<String> insertTypes;
    private int keyLocation;

    StorageManager storage = new StorageManager();
    QueryExecutor executor = new QueryExecutor(storage);

    public GrammarParser() throws IOException {
        init();
    }

    public GrammarParser(String text) throws IOException {
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
        readDatabaseFiles();
    }

    public void readDatabaseFiles() throws IOException {
        databases.clear();

        File root = new File(".");
        File[] directories = root.listFiles(File::isDirectory);

        if (directories != null) {
            for (File directory : directories) {
                String name = directory.getName();

                if (name.equals("src") || name.equals("bin") || name.equals(".settings") || name.equals(".git")) {
                    continue;
                }

                DataBase db = new DataBase(name);
                databases.add(db);
                getTableFiles(db);
            }
        }
    }

    public void getTableFiles(DataBase db) throws IOException {
        File dbDirectory = new File(db.name);
        File[] files = dbDirectory.listFiles();

        if (files != null) {
            for (File file : files) {
                String tableName = file.getName();
                if (tableName.endsWith(".dat")) {
                    tableName = tableName.substring(0, tableName.length() - 4);
                    db.tableNames.add(tableName);
                }
            }
        }
    }

    public void beginParse() throws IOException, GrammarParser.InvalidCommandException {
        String current = tokens[tokenMaker.position];

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
            throw new InvalidCommandException(
                "No valid command type was detected. This DBMS supports create, insert, describe, rename, update, select, let, delete, input, use, and exit"
            );
        }
    }

    // ----------------------------------------------------------------
    // CREATE
    // ----------------------------------------------------------------
    public void create() throws IOException, GrammarParser.InvalidCommandException {
        this.currentColNames = new ArrayList<String>();
        this.currentColTypes = new ArrayList<String>();
        this.currentPrimaryKey = null;

        if (!tokenMaker.match("CREATE")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but CREATE expected");
        }

        if (!(tokenMaker.peek().equalsIgnoreCase("TABLE") || tokenMaker.peek().equalsIgnoreCase("DATABASE"))) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but TABLE or DATABASE expected");
        }

        String token = tokenMaker.check();

        if (token.equalsIgnoreCase("DATABASE")) {
            String databaseName = tokenMaker.check();

            if (!tokenMaker.match(";")) {
                throw new InvalidCommandException(tokenMaker.peek() + " found, but ; expected");
            }

            DataBase db = new DataBase(databaseName);
            databases.add(db);

            ParsedQuery q = new ParsedQuery();
            q.queryType = "CREATE_DB";
            q.dbName = databaseName;
            executor.execute(q);
            return;
        }

        // CREATE TABLE
        if (currentDB == null) {
            throw new InvalidCommandException("No database selected. Use USE dbname; first.");
        }

        if (tokenMaker.match("(")) {
            throw new InvalidCommandException("Table name not found");
        }

        String tableName = tokenMaker.check();

        for (String checkName : currentDB.tableNames) {
            if (checkName.equalsIgnoreCase(tableName)) {
                throw new InvalidCommandException("Table already exists");
            }
        }

        if (!tokenMaker.match("(")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but ( expected");
        }

        parseCreateList();

        if (!tokenMaker.match(")")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but ) expected");
        }

        if (!tokenMaker.match(";")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but ; expected");
        }

        ParsedQuery q = new ParsedQuery();
        q.queryType = "CREATE_TABLE";
        q.tableName = tableName;
        q.columnNames.addAll(this.currentColNames);
        q.columnTypes.addAll(this.currentColTypes);
        q.primaryKey = this.currentPrimaryKey;

        executor.execute(q);
        currentDB.tableNames.add(tableName);
    }

    public void parseCreateList() throws GrammarParser.InvalidCommandException {
        parseCreateDefinition();

        while (tokenMaker.match(",")) {
            parseCreateDefinition();
        }
    }

    public void parseCreateDefinition() throws GrammarParser.InvalidCommandException {
        String name = tokenMaker.check();
        this.currentColNames.add(name);

        parseCreateType();

        if (tokenMaker.peek().equalsIgnoreCase("PRIMARY") || tokenMaker.peek().equalsIgnoreCase("FOREIGN")) {
            parseCreateConstraint();
        }
    }

    public void parseCreateType() throws GrammarParser.InvalidCommandException {
        if (tokenMaker.match("VARCHAR")) {
            if (tokenMaker.match("(")) {
                String number = tokenMaker.check();
                if (tokenMaker.match(")")) {
                    this.currentColTypes.add("VARCHAR(" + number + ")");
                    return;
                } else {
                    throw new InvalidCommandException(tokenMaker.peek() + " found, but ) expected");
                }
            } else {
                throw new InvalidCommandException(tokenMaker.peek() + " found, but ( expected");
            }
        } else if (tokenMaker.match("INT") || tokenMaker.match("INTEGER")) {
            this.currentColTypes.add("INTEGER");
        } else if (tokenMaker.match("FLOAT")) {
            this.currentColTypes.add("FLOAT");
        } else if (tokenMaker.match("TEXT")) {
            this.currentColTypes.add("TEXT");
        } else {
            throw new InvalidCommandException("Invalid attribute type detected");
        }
    }

    public void parseCreateConstraint() throws GrammarParser.InvalidCommandException {
        if (tokenMaker.peek().equalsIgnoreCase("PRIMARY") && tokenMaker.peekNext().equalsIgnoreCase("KEY")) {
            tokenMaker.position--;
            tokenMaker.position--;
            this.currentPrimaryKey = tokenMaker.check();
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

    // ----------------------------------------------------------------
    // USE
    // ----------------------------------------------------------------
    public void use() throws GrammarParser.InvalidCommandException {
        tokenMaker.check(); // burn USE
        String dbName = tokenMaker.advance();

        if (!tokenMaker.match(";")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but ; expected");
        }

        Boolean found = false;
        DataBase foundDb = null;

        for (DataBase database : databases) {
            if (database.name.equalsIgnoreCase(dbName)) {
                found = true;
                foundDb = database;
                break;
            }
        }

        if (!found) {
            throw new InvalidCommandException("Database not found");
        }

        this.currentDB = foundDb;

        ParsedQuery q = new ParsedQuery();
        q.queryType = "USE";
        q.dbName = dbName;
        executor.execute(q);
    }

    // ----------------------------------------------------------------
    // INSERT
    // ----------------------------------------------------------------
    public void insert() throws InvalidCommandException, FileNotFoundException {
        this.currentValues = new ArrayList<String>();
        this.insertTypes = new ArrayList<String>();
        this.keyLocation = 0;

        if (currentDB == null) {
            throw new InvalidCommandException("No database selected. Use USE dbname; first.");
        }

        if (!tokenMaker.match("INSERT")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but INSERT expected");
        }

        String name = tokenMaker.check();

        if (!tokenMaker.match("VALUES")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but VALUES expected");
        }

        if (!tokenMaker.match("(")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but ( expected");
        }

        parseInsertList(name);

        if (!tokenMaker.match(")")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but ) expected");
        }

        if (!tokenMaker.match(";")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but ; expected");
        }

        ParsedQuery q = new ParsedQuery();
        q.queryType = "INSERT";
        q.tableName = name;
        q.values.addAll(this.currentValues);
        executor.execute(q);
    }

    public void parseInsertList(String sentName) throws InvalidCommandException, FileNotFoundException {
        sentName = sentName.trim();

        File dbDirectory = new File(currentDB.name);
        File[] files = dbDirectory.listFiles();
        File tableFile = null;

        if (files != null) {
            for (File file : files) {
                String tableName = file.getName();
                if (tableName.equals(sentName.toUpperCase() + ".meta")) {
                    tableFile = file;
                }
            }
            if (tableFile == null) {
                throw new InvalidCommandException("Could not find table");
            }
        } else {
            throw new InvalidCommandException("Could not find table");
        }

        int pKey = 0;
        Scanner scn = new Scanner(new File(tableFile.getAbsolutePath()));

        int count = Integer.parseInt(scn.nextLine().trim());
        String keyText = scn.nextLine().trim();

        for (int i = 0; i < count; i++) {
            String line = scn.nextLine().trim();
            String[] split = line.split("\\s+");

            String name = split[0];
            String type = split[1].toUpperCase();

            if (type.contains("INT") || type.contains("INTEGER")) {
                type = "INT";
            } else if (type.contains("TEXT")) {
                type = "TEXT";
            } else if (type.contains("FLOAT")) {
                type = "FLOAT";
            }

            if (name.equalsIgnoreCase(keyText)) {
                pKey = i;
            }
            this.insertTypes.add(type);
        }
        scn.close();

        this.keyLocation = pKey;

        int i = 1;
        if (i > count) {
            throw new InvalidCommandException("Wrong number of attributes");
        }

        parseInsertValues(i);

        while (tokenMaker.peek().equalsIgnoreCase(",")) {
            tokenMaker.check();
            i = i + 1;
            if (i > count) {
                throw new InvalidCommandException("Wrong number of attributes");
            }
            parseInsertValues(i);
        }
    }

    public void parseInsertValues(int attributeNumber) throws InvalidCommandException {
        String currentValue = tokenMaker.check();
        this.currentValues.add(currentValue);

        if (this.insertTypes.get(attributeNumber - 1).equalsIgnoreCase("INT")) {
            if (!isInt(currentValue)) {
                throw new InvalidCommandException(currentValue + " is not an integer");
            }
        } else if (this.insertTypes.get(attributeNumber - 1).equalsIgnoreCase("TEXT")) {
            if (isInt(currentValue) || isFloat(currentValue)) {
                throw new InvalidCommandException(currentValue + " is not valid for text");
            }
        } else if (this.insertTypes.get(attributeNumber - 1).equalsIgnoreCase("FLOAT")) {
            if (!isFloat(currentValue)) {
                throw new InvalidCommandException(currentValue + " is not a float");
            }
        }

        if ((attributeNumber - 1) == this.keyLocation) {
            if (this.currentValues.get(attributeNumber - 1).equalsIgnoreCase("null")) {
                throw new InvalidCommandException("Primary key can not be null");
            }
        }
    }

    public static boolean isInt(String string) {
        try {
            Integer.parseInt(string);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static boolean isFloat(String string) {
        try {
            Double.parseDouble(string);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // ----------------------------------------------------------------
    // DESCRIBE
    // ----------------------------------------------------------------
    public void describe() throws IOException, GrammarParser.InvalidCommandException {
        tokenMaker.check(); // burn DESCRIBE

        if (currentDB == null) {
            throw new InvalidCommandException("No database selected. Use USE dbname; first.");
        }

        String target = tokenMaker.check();

        if (!tokenMaker.match(";")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but ; expected");
        }

        if (target.equalsIgnoreCase("ALL")) {
            for (String tableName : this.currentDB.tableNames) {
                ParsedQuery q = new ParsedQuery();
                q.queryType = "DESCRIBE";
                q.tableName = tableName;
                executor.execute(q);
            }
        } else {
            ParsedQuery q = new ParsedQuery();
            q.queryType = "DESCRIBE";
            q.tableName = target;
            executor.execute(q);
        }
    }

    // ----------------------------------------------------------------
    // RENAME
    // ----------------------------------------------------------------
    public void rename() throws GrammarParser.InvalidCommandException, IOException {
        if (currentDB == null) {
            throw new InvalidCommandException("No database selected. Use USE dbname; first.");
        }

        tokenMaker.check(); // burn RENAME
        String tableName = tokenMaker.check();

        Path path = Paths.get(this.currentDB.name, (tableName.toUpperCase() + ".dat"));

        if (Files.exists(path) && Files.isRegularFile(path)) {
            if (!tokenMaker.check().equals("(")) {
                throw new InvalidCommandException(tokenMaker.peek() + " found, but ( expected");
            } else {
                renameList(tableName);
            }
        } else {
            throw new InvalidCommandException("Could not find table");
        }
    }

    public void renameList(String tableName) throws GrammarParser.InvalidCommandException, IOException {
        int i = 1;
        parseRenameAttributes(i);
        tokenMaker.check();

        while (tokenMaker.position < tokens.length && tokens[tokenMaker.position].equals(",")) {
            tokenMaker.check();
            tokenMaker.check();
            parseRenameAttributes(++i);
        }

        if (!tokenMaker.match(")")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but ) expected");
        }
        if (!tokenMaker.match(";")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but ; expected");
        }

        int numAttributes = 0;
        Path path = Paths.get(this.currentDB.name, (tableName.toUpperCase() + ".meta"));
        Optional<String> line1 = Files.lines(path).findFirst();

        if (line1.isPresent()) {
            numAttributes = Integer.parseInt(line1.get().trim());
        }

        if (numAttributes != i) {
            throw new InvalidCommandException("Incorrect number of attributes");
        }

        String[] names = new String[numAttributes];
        tokenMaker.position = 0;
        tokenMaker.check();
        tokenMaker.check();
        tokenMaker.check();

        for (int j = 0; j < names.length; j++) {
            if (tokenMaker.peek().equals(",")) {
                tokenMaker.check();
            }
            names[j] = tokenMaker.check();
        }

        ParsedQuery q = new ParsedQuery();
        q.queryType = "RENAME";
        q.tableName = tableName;
        for (String n : names) {
            q.newColumnNames.add(n);
        }
        executor.execute(q);
    }

    public void parseRenameAttributes(int attributeNumber) {
        // intentionally left simple
    }

    // ----------------------------------------------------------------
    // UPDATE
    // ----------------------------------------------------------------
    public void update() {
        try {
            tokenMaker.check(); // burn UPDATE
            String tableName = tokenMaker.check();

            if (!tokenMaker.check().equalsIgnoreCase("SET")) {
                throw new InvalidCommandException(tokenMaker.peek() + " found, but SET expected");
            }

            ArrayList<String> setColumns = new ArrayList<String>();
            ArrayList<String> setValues = new ArrayList<String>();
            parseUpdateList(setColumns, setValues);

            ConditionBundle cond = parseOptionalWhere();

            if (!tokenMaker.match(";")) {
                throw new InvalidCommandException(tokenMaker.peek() + " found, but ; expected");
            }

            ParsedQuery q = new ParsedQuery();
            q.queryType = "UPDATE";
            q.tableName = tableName;
            q.setColumns.addAll(setColumns);
            q.setValues.addAll(setValues);
            applyConditionToQuery(q, cond);

            executor.execute(q);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void parseUpdateList(ArrayList<String> setColumns, ArrayList<String> setValues) throws InvalidCommandException {
        parseUpdateValues(setColumns, setValues);

        while (tokenMaker.position < tokens.length && tokens[tokenMaker.position].equals(",")) {
            tokenMaker.check();
            parseUpdateValues(setColumns, setValues);
        }
    }

    public void parseUpdateValues(ArrayList<String> setColumns, ArrayList<String> setValues) throws InvalidCommandException {
        String name = tokenMaker.check();

        if (!tokenMaker.match("=")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but = expected");
        }

        String value = tokenMaker.check();
        setColumns.add(name);
        setValues.add(value);
    }

    // ----------------------------------------------------------------
    // SELECT
    // ----------------------------------------------------------------
    public void select() {
        try {
            if (!tokenMaker.check().equalsIgnoreCase("SELECT")) {
                throw new InvalidCommandException(tokenMaker.peek() + " found, but SELECT expected");
            }

            ParsedQuery q = new ParsedQuery();
            q.queryType = "SELECT";

            parseSelectColumnsOrAggregate(q);

            if (!tokenMaker.match("FROM")) {
                throw new InvalidCommandException(tokenMaker.peek() + " found, but FROM expected");
            }

            while (tokenMaker.position < tokenMaker.tokens.length
                    && !tokenMaker.peek().equalsIgnoreCase("WHERE")
                    && !tokenMaker.peek().equalsIgnoreCase(";")) {
                if (!tokenMaker.peek().equalsIgnoreCase(",")) {
                    q.fromTables.add(tokenMaker.check());
                } else {
                    tokenMaker.check();
                }
            }

            ConditionBundle cond = parseOptionalWhere();
            applyConditionToQuery(q, cond);

            if (!tokenMaker.match(";")) {
                throw new InvalidCommandException(tokenMaker.peek() + " found, but ; expected");
            }

            executor.execute(q);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void parseSelectColumnsOrAggregate(ParsedQuery q) throws InvalidCommandException {
        String next = tokenMaker.peek();

        if (next.equalsIgnoreCase("COUNT")
                || next.equalsIgnoreCase("MIN")
                || next.equalsIgnoreCase("MAX")
                || next.equalsIgnoreCase("AVERAGE")) {

            q.aggregateFunc = tokenMaker.check().toUpperCase();

            if (!tokenMaker.match("(")) {
                throw new InvalidCommandException(tokenMaker.peek() + " found, but ( expected");
            }

            q.aggregateColumn = tokenMaker.check();

            if (!tokenMaker.match(")")) {
                throw new InvalidCommandException(tokenMaker.peek() + " found, but ) expected");
            }

            return;
        }

        q.selectColumns.add(tokenMaker.check());

        while (tokenMaker.peek().equals(",")) {
            tokenMaker.check();
            q.selectColumns.add(tokenMaker.check());
        }
    }

    public void selectNameList() {
        int i = 1;
        parseAttributeNames(i);

        while (tokenMaker.position < tokens.length && tokens[tokenMaker.position].equals(",")) {
            tokenMaker.check();
            parseAttributeNames(++i);
        }
    }

    public void parseAttributeNames(int nameNumber) {
        System.out.println("Received Attribute " + nameNumber + ": " + tokenMaker.check());
    }

    public void selectTablesList() {
        int i = 1;
        parseTableNames(i);

        while (tokenMaker.position < tokens.length && tokens[tokenMaker.position].equals(",")) {
            tokenMaker.check();
            parseTableNames(++i);
        }
    }

    public void parseTableNames(int tableNumber) {
        System.out.println("Received table " + tableNumber + ": " + tokenMaker.check());
    }

    // ----------------------------------------------------------------
    // LET
    // ----------------------------------------------------------------
    public void let() throws InvalidCommandException {
        if (!tokenMaker.check().equalsIgnoreCase("LET")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but LET expected");
        }

        String newTableName = tokenMaker.check();

        if (!tokenMaker.check().equalsIgnoreCase("KEY")) {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but KEY expected");
        }

        String keyName = tokenMaker.check();

        try {
            if (!tokenMaker.check().equalsIgnoreCase("SELECT")) {
                throw new InvalidCommandException(tokenMaker.peek() + " found, but SELECT expected");
            }

            ParsedQuery q = new ParsedQuery();
            q.queryType = "LET";
            q.letTableName = newTableName;
            q.letKey = keyName;

            parseSelectColumnsOrAggregate(q);

            if (!tokenMaker.match("FROM")) {
                throw new InvalidCommandException(tokenMaker.peek() + " found, but FROM expected");
            }

            while (tokenMaker.position < tokenMaker.tokens.length
                    && !tokenMaker.peek().equalsIgnoreCase("WHERE")
                    && !tokenMaker.peek().equalsIgnoreCase(";")) {
                if (!tokenMaker.peek().equalsIgnoreCase(",")) {
                    q.fromTables.add(tokenMaker.check());
                } else {
                    tokenMaker.check();
                }
            }

            ConditionBundle cond = parseOptionalWhere();
            applyConditionToQuery(q, cond);

            if (!tokenMaker.match(";")) {
                throw new InvalidCommandException(tokenMaker.peek() + " found, but ; expected");
            }

            executor.execute(q);
            if (currentDB != null) {
                currentDB.tableNames.add(newTableName);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    // ----------------------------------------------------------------
    // DELETE
    // ----------------------------------------------------------------
    public void delete() {
        try {
            tokenMaker.check(); // burn DELETE
            String tableName = tokenMaker.check();

            ConditionBundle cond = parseOptionalWhere();

            if (!tokenMaker.match(";")) {
                throw new InvalidCommandException(tokenMaker.peek() + " found, but ; expected");
            }

            ParsedQuery q = new ParsedQuery();
            q.queryType = "DELETE";
            q.tableName = tableName;
            applyConditionToQuery(q, cond);

            executor.execute(q);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    // ----------------------------------------------------------------
    // INPUT
    // ----------------------------------------------------------------
    public void input() throws IOException, InvalidCommandException {
        tokenMaker.check(); // burn INPUT
        String fileName1 = tokenMaker.check();

        if (tokenMaker.match(";")) {
            runFile(fileName1);
        } else if (tokenMaker.match("OUTPUT")) {
            String fileName2 = tokenMaker.check();
            if (!tokenMaker.match(";")) {
                throw new InvalidCommandException(tokenMaker.peek() + " found, but ; expected");
            }
            runFileOutput(fileName1, fileName2);
        } else {
            throw new InvalidCommandException(tokenMaker.peek() + " found, but OUTPUT or ; expected");
        }

        System.out.println("Done running input file");
    }

    public void runFile(String fileName) throws IOException, InvalidCommandException {
        try {
            File inputFile = new File(fileName);
            Scanner reader = new Scanner(inputFile);
            reader.useDelimiter(";");

            while (reader.hasNext()) {
                String command = reader.next().trim();

                if (command.length() == 0) {
                    continue;
                }

                if (command.equalsIgnoreCase("EXIT")) {
                    System.out.println("Thank you for using this DBMS");
                    break;
                }

                command = command + ";";

                System.out.println();
                System.out.println("Running command: " + command);

                setCommand(command);
                try {
                    beginParse();
                } catch (InvalidCommandException e) {
                    System.out.println("ERROR: " + e.getMessage());
                }
            }
            reader.close();
        } catch (FileNotFoundException e) {
            throw new InvalidCommandException("Could not find input file");
        }
    }

    public void runFileOutput(String fileName, String fileNameOutput) throws IOException, InvalidCommandException {
        PrintStream originalOut = System.out;

        try {
            File inputFile = new File(fileName);
            File outputFile = new File(fileNameOutput);

            Scanner reader = new Scanner(inputFile);
            reader.useDelimiter(";");

            PrintStream fileOut = new PrintStream(new FileOutputStream(outputFile));
            System.setOut(fileOut);

            while (reader.hasNext()) {
                String command = reader.next().trim();

                if (command.length() == 0) {
                    continue;
                }

                if (command.equalsIgnoreCase("EXIT")) {
                    System.out.println("Thank you for using this DBMS");
                    break;
                }

                command = command + ";";
                System.out.println();
                System.out.println("Running command: " + command);

                setCommand(command);

                try {
                    beginParse();
                } catch (InvalidCommandException e) {
                    System.out.println("ERROR: " + e.getMessage());
                }
            }

            reader.close();
            fileOut.close();
        } catch (FileNotFoundException e) {
            throw new InvalidCommandException("Could not find input file");
        } finally {
            System.setOut(originalOut);
        }
    }

    // ----------------------------------------------------------------
    // WHERE helpers
    // ----------------------------------------------------------------
    private ConditionBundle parseOptionalWhere() throws InvalidCommandException {
        ConditionBundle cond = new ConditionBundle();

        if (!tokenMaker.peek().equalsIgnoreCase("WHERE")) {
            return cond;
        }

        tokenMaker.check(); // burn WHERE

        cond.whereLeft = tokenMaker.check();
        cond.whereOp = tokenMaker.check();

        if (!isValidRelOp(cond.whereOp)) {
            throw new InvalidCommandException("Invalid relational operator");
        }

        cond.whereRight = tokenMaker.check();

        if (tokenMaker.peek().equalsIgnoreCase("AND") || tokenMaker.peek().equalsIgnoreCase("OR")) {
            cond.whereConnector = tokenMaker.check();

            cond.whereLeft2 = tokenMaker.check();
            cond.whereOp2 = tokenMaker.check();

            if (!isValidRelOp(cond.whereOp2)) {
                throw new InvalidCommandException("Invalid relational operator");
            }

            cond.whereRight2 = tokenMaker.check();
        }

        return cond;
    }

    private void applyConditionToQuery(ParsedQuery q, ConditionBundle cond) {
        q.whereLeft = cond.whereLeft;
        q.whereOp = cond.whereOp;
        q.whereRight = cond.whereRight;
        q.whereConnector = cond.whereConnector;
        q.whereLeft2 = cond.whereLeft2;
        q.whereOp2 = cond.whereOp2;
        q.whereRight2 = cond.whereRight2;
    }

    private boolean isValidRelOp(String op) {
        return op.equals("=") || op.equals("!=") || op.equals("<")
                || op.equals(">") || op.equals("<=") || op.equals(">=");
    }

    private class ConditionBundle {
        String whereLeft = null;
        String whereOp = null;
        String whereRight = null;
        String whereConnector = null;
        String whereLeft2 = null;
        String whereOp2 = null;
        String whereRight2 = null;
    }

    // ----------------------------------------------------------------
    // TOKEN MAKER
    // ----------------------------------------------------------------
    public class TokenMaker {
        private String[] tokens;
        private int position;

        public TokenMaker(String text) {
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

            tokens = text.trim().split("\\s+");
            position = 0;
        }

        public String check() {
            position += 1;
            return tokens[position - 1];
        }

        private String peek() {
            if (position >= tokens.length) return "EOF";
            return tokens[position];
        }

        private String peekNext() {
            if (position + 1 >= tokens.length) return "EOF";
            return tokens[position + 1];
        }

        private String advance() {
            if (position >= tokens.length) return "EOF";
            return tokens[position++];
        }

        private boolean match(String expected) {
            if (peek().equalsIgnoreCase(expected)) {
                advance();
                return true;
            }
            return false;
        }
    }

    // ----------------------------------------------------------------
    // DB + EXCEPTION INNER CLASSES
    // ----------------------------------------------------------------
    public class DataBase {
        String name;
        private ArrayList<String> tableNames = new ArrayList<String>();

        public DataBase(String name) {
            this.name = name;
        }
    }

    public class InvalidCommandException extends Exception {
        public InvalidCommandException(String message) {
            super(message);
        }
    }
}