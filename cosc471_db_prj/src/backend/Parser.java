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

        // 1. CREATE DATABASE university
        queue.add(createDb("university"));

        // 2. USE university
        queue.add(useDb("university"));

        // 3. CREATE TABLE student (id integer primary key, name text, gpa float)
        queue.add(createTable(
            "student",
            new String[]{"id",      "name", "gpa"},
            new String[]{"INTEGER", "TEXT", "FLOAT"},
            "id"
        ));

        // 4. CREATE TABLE course (cname text, credits integer)  — no PK
        queue.add(createTable(
            "course",
            new String[]{"cname", "credits"},
            new String[]{"TEXT",  "INTEGER"},
            null
        ));
        
        queue.add(insertInto("student", new String[]{"1", "John Doe", "3.5"}));
        queue.add(insertInto("student", new String[]{"2", "Jane Smith", "3.9"}));
        queue.add(insertInto("course",  new String[]{"Math101", "3"}));
        
        queue.add(select("student", new String[]{"name"}, null, null, null));
        queue.add(select("student", new String[]{"name", "gpa"}, "id", "=", "1"));
        queue.add(select("course",  new String[]{"*"}, null, null, null));
        
        queue.add(update("student", new String[]{"gpa"}, new String[]{"4.0"}, "id", "=", "1"));
        queue.add(update("student", new String[]{"name", "gpa"}, new String[]{"\"ishaq ahmed\"", "1.1"}, "name", "=", "\"Jane Smith\""));
        queue.add(select("student", new String[]{"*"}, null, null, null));
        
        queue.add(delete("student", "id", "=", "2"));
        queue.add(delete("course", null, null, null));  // no WHERE — deletes all and removes schema
        
        queue.add(describe("student"));
        queue.add(describe("ALL"));
        
        queue.add(rename("student", new String[]{"sid", "sname", "sgpa"}));
    }

    // ---------------------------------------------------------------
    // Query-builder helpers (keep these — they'll be called by the
    // real parser once we write it)
    // ---------------------------------------------------------------

    public static ParsedQuery createDb(String dbName) {
        ParsedQuery q = new ParsedQuery();
        q.queryType = "CREATE_DB";
        q.dbName    = dbName;
        return q;
    }

    public static ParsedQuery useDb(String dbName) {
        ParsedQuery q = new ParsedQuery();
        q.queryType = "USE";
        q.dbName    = dbName;
        return q;
    }

    public static ParsedQuery createTable(String tableName,
                                          String[] colNames,
                                          String[] colTypes,
                                          String primaryKey) {
        ParsedQuery q    = new ParsedQuery();
        q.queryType      = "CREATE_TABLE";
        q.tableName      = tableName;
        q.columnNames    = new ArrayList<>(Arrays.asList(colNames));
        q.columnTypes    = new ArrayList<>(Arrays.asList(colTypes));
        q.primaryKey     = primaryKey;
        return q;
    }

    // ---------------------------------------------------------------
    // Future helpers (stubbed — fill in as we implement each query)
    // ---------------------------------------------------------------

    public static ParsedQuery insertInto(String tableName, String[] values) {
        ParsedQuery q = new ParsedQuery();
        q.queryType   = "INSERT";
        q.tableName   = tableName;
        q.values      = new ArrayList<>(Arrays.asList(values));
        return q;
    }

    public static ParsedQuery select(String tableName, String[] cols,
                                     String whereLeft, String whereOp, String whereRight) {
        ParsedQuery q     = new ParsedQuery();
        q.queryType       = "SELECT";
        q.fromTables      = new ArrayList<>(Arrays.asList(tableName));
        q.selectColumns   = new ArrayList<>(Arrays.asList(cols));
        q.whereLeft       = whereLeft;
        q.whereOp         = whereOp;
        q.whereRight      = whereRight;
        return q;
    }
    
    public static ParsedQuery update(String tableName, String[] setCols, String[] setVals,
            String whereLeft, String whereOp, String whereRight) {
    		ParsedQuery q  = new ParsedQuery();
    		q.queryType    = "UPDATE";
    		q.tableName    = tableName;
    		q.setColumns   = new ArrayList<>(Arrays.asList(setCols));
    		q.setValues    = new ArrayList<>(Arrays.asList(setVals));
    		q.whereLeft    = whereLeft;
    		q.whereOp      = whereOp;
    		q.whereRight   = whereRight;
    		return q;
    }
    
    public static ParsedQuery delete(String tableName, String whereLeft, String whereOp, String whereRight) {
        ParsedQuery q  = new ParsedQuery();
        q.queryType    = "DELETE";
        q.tableName    = tableName;
        q.whereLeft    = whereLeft;
        q.whereOp      = whereOp;
        q.whereRight   = whereRight;
        return q;
    }

    public static ParsedQuery describe(String tableName) {
        ParsedQuery q = new ParsedQuery();
        q.queryType   = "DESCRIBE";
        q.tableName   = tableName;
        return q;
    }
    
    public static ParsedQuery rename(String tableName, String[] newColNames) {
        ParsedQuery q      = new ParsedQuery();
        q.queryType        = "RENAME";
        q.tableName        = tableName;
        q.newColumnNames   = new ArrayList<>(Arrays.asList(newColNames));
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
