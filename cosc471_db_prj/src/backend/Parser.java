package backend;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

/**
 * For now: hardcoded test queries so we can develop and verify each feature.
 * Later: replace getNextQuery() with real tokeniser + recursive-descent parser.
 *
 * Add your test cases inside buildTestQueries().
 */
public class Parser {

    private final Queue<ParsedQuery> queue = new LinkedList<>();

    public Parser() {
        buildTestQueries();
    }

    // ---------------------------------------------------------------
    // Add test queries here — one method call per statement
    // ---------------------------------------------------------------
    private void buildTestQueries() {
        // Keep this light. Your real program now uses GrammarParser from stdin.
        // These are only here if you want a quick hardcoded test mode.

        queue.add(createDb("university"));
        queue.add(useDb("university"));

        queue.add(createTable(
            "student",
            new String[]{"id", "name", "gpa"},
            new String[]{"INTEGER", "TEXT", "FLOAT"},
            "id"
        ));

        queue.add(createTable(
            "course",
            new String[]{"cname", "credits"},
            new String[]{"TEXT", "INTEGER"},
            null
        ));

        queue.add(insertInto("student", new String[]{"1", "\"John Doe\"", "3.5"}));
        queue.add(insertInto("student", new String[]{"2", "\"Jane Smith\"", "3.9"}));
        queue.add(insertInto("course", new String[]{"\"Math101\"", "3"}));

        queue.add(select(
            new String[]{"student"},
            new String[]{"*"},
            null, null, null
        ));

        queue.add(select(
            new String[]{"student"},
            new String[]{"name", "gpa"},
            "id", "=", "1"
        ));

        queue.add(select(
            new String[]{"student", "course"},
            new String[]{"*"},
            "id", ">", "credits"
        ));

        queue.add(select(
            new String[]{"student"},
            new String[]{"name", "gpa"},
            "id", ">=", "1",
            "AND",
            "gpa", ">=", "3.5"
        ));

        queue.add(update(
            "student",
            new String[]{"gpa"},
            new String[]{"4.0"},
            "id", "=", "1"
        ));

        queue.add(update(
            "student",
            new String[]{"name", "gpa"},
            new String[]{"\"ishaq ahmed\"", "1.1"},
            "name", "=", "\"Jane Smith\""
        ));

        queue.add(update(
            "student",
            new String[]{"gpa"},
            new String[]{"3.7"},
            "id", ">=", "1",
            "AND",
            "name", "!=", "\"Nobody\""
        ));

        queue.add(delete("student", "id", "=", "2"));
        queue.add(delete("course", null, null, null));

        queue.add(describe("student"));
        queue.add(describe("ALL"));

        queue.add(rename("student", new String[]{"sid", "sname", "sgpa"}));

        queue.add(let(
            "test",
            "id",
            new String[]{"student"},
            new String[]{"*"},
            "id", ">", "1"
        ));

        queue.add(let(
            "test2",
            "id",
            new String[]{"student", "course"},
            new String[]{"*"},
            "id", "=", "credits"
        ));

        queue.add(let(
            "test3",
            "id",
            new String[]{"student"},
            new String[]{"id", "name"},
            "id", ">=", "1",
            "AND",
            "name", "!=", "\"zzz\""
        ));

        queue.add(selectAggregate("COUNT", "*", new String[]{"student"}, null, null, null));
        queue.add(selectAggregate("MAX", "gpa", new String[]{"student"}, null, null, null));
        queue.add(selectAggregate("MIN", "gpa", new String[]{"student"}, "id", ">=", "1"));
        queue.add(selectAggregate("AVERAGE", "gpa", new String[]{"student"}, "id", ">=", "1"));

        queue.add(input("project-test-data.txt", null));
        queue.add(input("project-test-data.txt", "file2.output"));
    }

    // ---------------------------------------------------------------
    // Query-builder helpers
    // ---------------------------------------------------------------
    public static ParsedQuery createDb(String dbName) {
        ParsedQuery q = new ParsedQuery();
        q.queryType = "CREATE_DB";
        q.dbName = dbName;
        return q;
    }

    public static ParsedQuery useDb(String dbName) {
        ParsedQuery q = new ParsedQuery();
        q.queryType = "USE";
        q.dbName = dbName;
        return q;
    }

    public static ParsedQuery createTable(String tableName,
                                          String[] colNames,
                                          String[] colTypes,
                                          String primaryKey) {
        ParsedQuery q = new ParsedQuery();
        q.queryType = "CREATE_TABLE";
        q.tableName = tableName;
        q.columnNames = new ArrayList<>(Arrays.asList(colNames));
        q.columnTypes = new ArrayList<>(Arrays.asList(colTypes));
        q.primaryKey = primaryKey;
        return q;
    }

    public static ParsedQuery insertInto(String tableName, String[] values) {
        ParsedQuery q = new ParsedQuery();
        q.queryType = "INSERT";
        q.tableName = tableName;
        q.values = new ArrayList<>(Arrays.asList(values));
        return q;
    }

    // ---------------------------------------------------------------
    // SELECT helpers
    // ---------------------------------------------------------------
    public static ParsedQuery select(String[] tableNames, String[] cols,
                                     String whereLeft, String whereOp, String whereRight) {
        ParsedQuery q = new ParsedQuery();
        q.queryType = "SELECT";
        q.fromTables = new ArrayList<>(Arrays.asList(tableNames));
        q.selectColumns = new ArrayList<>(Arrays.asList(cols));
        q.whereLeft = whereLeft;
        q.whereOp = whereOp;
        q.whereRight = whereRight;
        return q;
    }

    public static ParsedQuery select(String[] tableNames, String[] cols,
                                     String whereLeft, String whereOp, String whereRight,
                                     String whereConnector,
                                     String whereLeft2, String whereOp2, String whereRight2) {
        ParsedQuery q = select(tableNames, cols, whereLeft, whereOp, whereRight);
        q.whereConnector = whereConnector;
        q.whereLeft2 = whereLeft2;
        q.whereOp2 = whereOp2;
        q.whereRight2 = whereRight2;
        return q;
    }

    public static ParsedQuery selectAggregate(String aggregateFunc, String aggregateColumn,
                                              String[] tableNames,
                                              String whereLeft, String whereOp, String whereRight) {
        ParsedQuery q = new ParsedQuery();
        q.queryType = "SELECT";
        q.aggregateFunc = aggregateFunc;
        q.aggregateColumn = aggregateColumn;
        q.fromTables = new ArrayList<>(Arrays.asList(tableNames));
        q.whereLeft = whereLeft;
        q.whereOp = whereOp;
        q.whereRight = whereRight;
        return q;
    }

    public static ParsedQuery selectAggregate(String aggregateFunc, String aggregateColumn,
                                              String[] tableNames,
                                              String whereLeft, String whereOp, String whereRight,
                                              String whereConnector,
                                              String whereLeft2, String whereOp2, String whereRight2) {
        ParsedQuery q = selectAggregate(aggregateFunc, aggregateColumn, tableNames, whereLeft, whereOp, whereRight);
        q.whereConnector = whereConnector;
        q.whereLeft2 = whereLeft2;
        q.whereOp2 = whereOp2;
        q.whereRight2 = whereRight2;
        return q;
    }

    // ---------------------------------------------------------------
    // UPDATE helpers
    // ---------------------------------------------------------------
    public static ParsedQuery update(String tableName, String[] setCols, String[] setVals,
                                     String whereLeft, String whereOp, String whereRight) {
        ParsedQuery q = new ParsedQuery();
        q.queryType = "UPDATE";
        q.tableName = tableName;
        q.setColumns = new ArrayList<>(Arrays.asList(setCols));
        q.setValues = new ArrayList<>(Arrays.asList(setVals));
        q.whereLeft = whereLeft;
        q.whereOp = whereOp;
        q.whereRight = whereRight;
        return q;
    }

    public static ParsedQuery update(String tableName, String[] setCols, String[] setVals,
                                     String whereLeft, String whereOp, String whereRight,
                                     String whereConnector,
                                     String whereLeft2, String whereOp2, String whereRight2) {
        ParsedQuery q = update(tableName, setCols, setVals, whereLeft, whereOp, whereRight);
        q.whereConnector = whereConnector;
        q.whereLeft2 = whereLeft2;
        q.whereOp2 = whereOp2;
        q.whereRight2 = whereRight2;
        return q;
    }

    // ---------------------------------------------------------------
    // DELETE helpers
    // ---------------------------------------------------------------
    public static ParsedQuery delete(String tableName, String whereLeft, String whereOp, String whereRight) {
        ParsedQuery q = new ParsedQuery();
        q.queryType = "DELETE";
        q.tableName = tableName;
        q.whereLeft = whereLeft;
        q.whereOp = whereOp;
        q.whereRight = whereRight;
        return q;
    }

    public static ParsedQuery delete(String tableName, String whereLeft, String whereOp, String whereRight,
                                     String whereConnector,
                                     String whereLeft2, String whereOp2, String whereRight2) {
        ParsedQuery q = delete(tableName, whereLeft, whereOp, whereRight);
        q.whereConnector = whereConnector;
        q.whereLeft2 = whereLeft2;
        q.whereOp2 = whereOp2;
        q.whereRight2 = whereRight2;
        return q;
    }

    // ---------------------------------------------------------------
    // DESCRIBE / RENAME
    // ---------------------------------------------------------------
    public static ParsedQuery describe(String tableName) {
        ParsedQuery q = new ParsedQuery();
        q.queryType = "DESCRIBE";
        q.tableName = tableName;
        return q;
    }

    public static ParsedQuery rename(String tableName, String[] newColNames) {
        ParsedQuery q = new ParsedQuery();
        q.queryType = "RENAME";
        q.tableName = tableName;
        q.newColumnNames = new ArrayList<>(Arrays.asList(newColNames));
        return q;
    }

    // ---------------------------------------------------------------
    // LET helpers
    // ---------------------------------------------------------------
    public static ParsedQuery let(String letTableName, String letKey,
                                  String[] fromTable, String[] selectCols,
                                  String whereLeft, String whereOp, String whereRight) {
        ParsedQuery q = new ParsedQuery();
        q.queryType = "LET";
        q.letTableName = letTableName;
        q.letKey = letKey;
        q.fromTables = new ArrayList<>(Arrays.asList(fromTable));
        q.selectColumns = new ArrayList<>(Arrays.asList(selectCols));
        q.whereLeft = whereLeft;
        q.whereOp = whereOp;
        q.whereRight = whereRight;
        return q;
    }

    public static ParsedQuery let(String letTableName, String letKey,
                                  String[] fromTable, String[] selectCols,
                                  String whereLeft, String whereOp, String whereRight,
                                  String whereConnector,
                                  String whereLeft2, String whereOp2, String whereRight2) {
        ParsedQuery q = let(letTableName, letKey, fromTable, selectCols, whereLeft, whereOp, whereRight);
        q.whereConnector = whereConnector;
        q.whereLeft2 = whereLeft2;
        q.whereOp2 = whereOp2;
        q.whereRight2 = whereRight2;
        return q;
    }

    // ---------------------------------------------------------------
    // INPUT helper
    // ---------------------------------------------------------------
    public static ParsedQuery input(String inputFile, String outputFile) {
        ParsedQuery q = new ParsedQuery();
        q.queryType = "INPUT";
        q.inputFile = inputFile;
        q.outputFile = outputFile;
        return q;
    }

    // ---------------------------------------------------------------
    // Iteration interface used by Main
    // ---------------------------------------------------------------
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    public ParsedQuery getNextQuery() {
        return queue.poll();
    }
}