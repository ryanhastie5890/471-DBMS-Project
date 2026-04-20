package backend;

import java.util.List;
import java.util.ArrayList;

public class ParsedQuery {
    public String queryType;         // "CREATE_TABLE", "INSERT", "SELECT", "UPDATE", "DELETE", "CREATE_DB", "USE", "DESCRIBE", "RENAME", "LET", "INPUT", "EXIT"

    // CREATE TABLE + general
    public String tableName;
    public List<String> columnNames  = new ArrayList<>();
    public List<String> columnTypes  = new ArrayList<>();  // "INTEGER", "TEXT", "FLOAT"
    public String primaryKey;                               // null if no PK

    // CREATE DATABASE / USE
    public String dbName;

    // INSERT
    public List<String> values = new ArrayList<>();

    // SELECT
    public List<String> selectColumns = new ArrayList<>();  // ["*"] or ["name","id"]
    public List<String> fromTables    = new ArrayList<>();  // supports joins

    // WHERE clause (first condition)
    public String whereLeft;
    public String whereOp;       // "<", ">", "<=", ">=", "=", "!="
    public String whereRight;

    // Second condition (AND / OR)
    public String whereConnector;   // "AND" / "OR" / null
    public String whereLeft2;
    public String whereOp2;
    public String whereRight2;

    // UPDATE
    public List<String> setColumns = new ArrayList<>();
    public List<String> setValues  = new ArrayList<>();
    // reuses where fields above

    // RENAME
    public List<String> newColumnNames = new ArrayList<>();

    // LET
    public String letTableName;
    public String letKey;

    // Aggregates (grad students)
    public String aggregateFunc;    // "COUNT", "MIN", "MAX", "AVERAGE" — null if not aggregate
    public String aggregateColumn;  // "*" for COUNT, or column name

    // INPUT command
    public String inputFile;
    public String outputFile;       // null if no OUTPUT clause
}
