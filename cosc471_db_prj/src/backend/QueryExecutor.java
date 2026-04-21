package backend;

import java.io.*;
import java.util.*;

/**
 * Routes a ParsedQuery to the correct execution method.
 * Each executeXxx() method will grow as we add more query types.
 */
public class QueryExecutor {

    private final StorageManager storage;

    public QueryExecutor(StorageManager storage) {
        this.storage = storage;
    }

    // ---------------------------------------------------------------
    // Main dispatch
    // ---------------------------------------------------------------
    public void execute(ParsedQuery q) {
        try {
            switch (q.queryType.toUpperCase()) {
                case "CREATE_DB":    executeCreateDb(q);    break;
                case "USE":          executeUse(q);         break;
                case "CREATE_TABLE": executeCreateTable(q); break;
                case "INSERT": executeInsert(q); break;
                case "SELECT": executeSelect(q); break;
                case "UPDATE": executeUpdate(q); break;
                case "DELETE": executeDelete(q); break;
                case "DESCRIBE": executeDescribe(q); break;
                case "RENAME": executeRename(q); break;
                case "LET": executeLet(q); break;
                // Future: INSERT, SELECT, UPDATE, DELETE, DESCRIBE, RENAME, LET, INPUT, EXIT
                default:
                    System.out.println("Unknown query type: " + q.queryType);
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    // ---------------------------------------------------------------
    // CREATE DATABASE
    // ---------------------------------------------------------------
    private void executeCreateDb(ParsedQuery q) throws IOException {
        File dir = new File(q.dbName);
        if (dir.exists()) {
            System.out.println("Database '" + q.dbName + "' already exists.");
        } else {
            dir.mkdirs();
            System.out.println("Database '" + q.dbName + "' created.");
        }
    }

    // ---------------------------------------------------------------
    // USE
    // ---------------------------------------------------------------
    private void executeUse(ParsedQuery q) {
        File dir = new File(q.dbName);
        if (!dir.exists()) {
            System.out.println("Database '" + q.dbName + "' does not exist.");
            return;
        }
        storage.setDatabase(q.dbName);
        System.out.println("Using database '" + q.dbName + "'.");
    }

    // ---------------------------------------------------------------
    // CREATE TABLE
    // ---------------------------------------------------------------
    private void executeCreateTable(ParsedQuery q) throws IOException {
        if (storage.getDbDirectory() == null) {
            System.out.println("Error: No database selected. Use 'USE dbname;' first.");
            return;
        }
        if (storage.tableExists(q.tableName)) {
            System.out.println("Error: Table '" + q.tableName + "' already exists.");
            return;
        }
        storage.createTable(q);
    }
    
    private void executeInsert(ParsedQuery q) throws IOException, ClassNotFoundException {
        if (storage.getDbDirectory() == null) {
            System.out.println("Error: No database selected.");
            return;
        }
        if (!storage.tableExists(q.tableName)) {
            System.out.println("Error: Table '" + q.tableName + "' does not exist.");
            return;
        }
        storage.insertRecord(q.tableName, q.values);
        System.out.println("1 row inserted into '" + q.tableName.toUpperCase() + "'.");
    }
    
    // **********************************************************************************************************
    // SELECT
    private void executeSelect(ParsedQuery q) throws IOException, ClassNotFoundException {
        if (storage.getDbDirectory() == null) {
            System.out.println("Error: No database selected.");
            return;
        }
        String tableName = q.fromTables.get(0);
        if (!storage.tableExists(tableName)) {
            System.out.println("Error: Table '" + tableName + "' does not exist.");
            return;
        }

        TableMeta meta = storage.loadMeta(tableName);
        List<String[]> rows;

        // If WHERE on primary key — use BST search directly
        if (q.whereLeft != null && meta.primaryKey != null
                && q.whereLeft.equalsIgnoreCase(meta.primaryKey)
                && q.whereOp.equals("=")) {
            rows = new ArrayList<>();
            BSTIndex bst = storage.loadIndex(tableName);
            long offset = bst.search(q.whereRight);
            if (offset != -1L) {
                try (RandomAccessFile raf = new RandomAccessFile(
                        storage.getDbDirectory() + File.separator + tableName.toUpperCase() + ".dat", "r")) {
                    String[] row = storage.readRecordAt(raf, meta, offset);
                    if (row != null)
                    	rows.add(row);
                }
            }
        } else {
            // Full scan (in-order if PK exists, sequential otherwise)
            rows = storage.readAllRecords(tableName);
            if (q.whereLeft != null) 
            	rows = applyWhere(rows, meta, q);
        }

        if (rows.isEmpty()) {
            System.out.println("Nothing found");
            return;
        }

        // Determine columns to print
        List<Integer> colIdxs = new ArrayList<>();
        if (q.selectColumns.get(0).equals("*")) {
            for (int i = 0; i < meta.columnNames.size(); i++) colIdxs.add(i);
        } else {
            for (String col : q.selectColumns) {
                int idx = meta.getColumnIndex(col);
                if (idx == -1) { System.out.println("Error: Unknown column '" + col + "'"); return; }
                colIdxs.add(idx);
            }
        }

        // Print header
        StringBuilder header = new StringBuilder();
        for (int idx : colIdxs) 
        	header.append(meta.columnNames.get(idx)).append("\t");
        System.out.println(header.toString().trim());

        // Print rows
        int rowNum = 1;
        for (String[] row : rows) {
            StringBuilder line = new StringBuilder(rowNum++ + ". ");
            for (int idx : colIdxs) 
            	line.append(row[idx]).append("\t");
            System.out.println(line.toString().trim());
        }
    }

    private List<String[]> applyWhere(List<String[]> rows, TableMeta meta, ParsedQuery q) {
        List<String[]> result = new ArrayList<>();
        for (String[] row : rows) {
            if (matchesCondition(row, meta, q.whereLeft, q.whereOp, q.whereRight)) {
                if (q.whereConnector == null) {
                    result.add(row);
                } else if (q.whereConnector.equalsIgnoreCase("AND")) {
                    if (matchesCondition(row, meta, q.whereLeft2, q.whereOp2, q.whereRight2))
                        result.add(row);
                } else if (q.whereConnector.equalsIgnoreCase("OR")) {
                    result.add(row);
                }
            } else if (q.whereConnector != null && q.whereConnector.equalsIgnoreCase("OR")) {
                if (matchesCondition(row, meta, q.whereLeft2, q.whereOp2, q.whereRight2))
                    result.add(row);
            }
        }
        return result;
    }

    private boolean matchesCondition(String[] row, TableMeta meta, String left, String op, String right) {
        int idx = meta.getColumnIndex(left);
        if (idx == -1) return false;
        String colType = meta.columnTypes.get(idx);
        String cellVal = row[idx];
        // Strip quotes from right side if TEXT
        String compareVal = right.startsWith("\"") ? right.substring(1, right.length() - 1) : right;
        int cmp;
        try {
            if (colType.equalsIgnoreCase("INTEGER")) {
                cmp = Long.compare(Long.parseLong(cellVal.trim()), Long.parseLong(compareVal.trim()));
            } else if (colType.equalsIgnoreCase("FLOAT")) {
                cmp = Double.compare(Double.parseDouble(cellVal.trim()), Double.parseDouble(compareVal.trim()));
            } else {
                cmp = cellVal.trim().compareToIgnoreCase(compareVal.trim());
            }
        } catch (NumberFormatException e) { return false; }

        switch (op) {
            case "=":  return cmp == 0;
            case "!=": return cmp != 0;
            case "<":  return cmp <  0;
            case ">":  return cmp >  0;
            case "<=": return cmp <= 0;
            case ">=": return cmp >= 0;
            default:   return false;
        }
    }
    
    // *******************************************************************************************
    // update 
    private void executeUpdate(ParsedQuery q) throws IOException, ClassNotFoundException {
        if (storage.getDbDirectory() == null) {
            System.out.println("Error: No database selected.");
            return;
        }
        if (!storage.tableExists(q.tableName)) {
            System.out.println("Error: Table '" + q.tableName + "' does not exist.");
            return;
        }

        TableMeta meta = storage.loadMeta(q.tableName);

        // Build list of (offset, currentRow) pairs to update
        List<long[]> offsetList = new ArrayList<>();
        List<String[]> rowList  = new ArrayList<>();

        if (q.whereLeft != null && meta.primaryKey != null
                && q.whereLeft.equalsIgnoreCase(meta.primaryKey)
                && q.whereOp.equals("=")) {
            // BST direct lookup
            BSTIndex bst = storage.loadIndex(q.tableName);
            long offset  = bst.search(q.whereRight);
            if (offset != -1L) {
                try (RandomAccessFile raf = new RandomAccessFile(
                        storage.getDbDirectory() + File.separator + q.tableName.toUpperCase() + ".dat", "r")) {
                    String[] row = storage.readRecordAt(raf, meta, offset);
                    if (row != null) { 
                    	offsetList.add(new long[]{offset}); rowList.add(row); 
                    }
                }
            }
        } else {
            // In-order BST traversal or sequential scan via readAllRecords
            List<String[]> allRows = storage.readAllRecords(q.tableName);
            // We need offsets too — re-fetch them
            if (meta.primaryKey != null) {
                BSTIndex bst = storage.loadIndex(q.tableName);
                List<long[]> offsets = bst.inOrderOffsets();
                try (RandomAccessFile raf = new RandomAccessFile(
                        storage.getDbDirectory() + File.separator + q.tableName.toUpperCase() + ".dat", "r")) {
                    for (long[] entry : offsets) {
                        String[] row = storage.readRecordAt(raf, meta, entry[0]);
                        if (row != null && (q.whereLeft == null || matchesCondition(row, meta, q.whereLeft, q.whereOp, q.whereRight))) {
                            offsetList.add(entry); rowList.add(row);
                        }
                    }
                }
            } else {
                int recSize = storage.recordSize(meta);
                try (RandomAccessFile raf = new RandomAccessFile(
                        storage.getDbDirectory() + File.separator + q.tableName.toUpperCase() + ".dat", "r")) {
                    long pos = 0;
                    while (pos + 1 + recSize <= raf.length()) {
                        String[] row = storage.readRecordAt(raf, meta, pos);
                        if (row != null && (q.whereLeft == null || matchesCondition(row, meta, q.whereLeft, q.whereOp, q.whereRight))) {
                            offsetList.add(new long[]{pos}); rowList.add(row);
                        }
                        pos += 1 + recSize;
                    }
                }
            }
        }

        if (offsetList.isEmpty()) { System.out.println("Nothing found"); return; }

        // Apply updates
        BSTIndex bst = meta.primaryKey != null ? storage.loadIndex(q.tableName) : null;
        int count = 0;
        for (int i = 0; i < offsetList.size(); i++) {
            long offset    = offsetList.get(i)[0];
            String[] row   = rowList.get(i);
            String oldPKVal = meta.primaryKey != null ? row[meta.getColumnIndex(meta.primaryKey)] : null;

            // Apply SET values to the row
            for (int j = 0; j < q.setColumns.size(); j++) {
                int colIdx = meta.getColumnIndex(q.setColumns.get(j));
                if (colIdx == -1) {
                	System.out.println("Error: Unknown column '" + q.setColumns.get(j) + "'"); 
                	return; 
                }
                String val = q.setValues.get(j);
                if (val.startsWith("\"") && val.endsWith("\"")) 
                	val = val.substring(1, val.length() - 1);
                row[colIdx] = val;
            }

            storage.updateRecordAt(q.tableName, offset, Arrays.asList(row), meta);

            // Update BST if PK value changed
            if (bst != null && oldPKVal != null) {
                String newPKVal = row[meta.getColumnIndex(meta.primaryKey)];
                if (!oldPKVal.equalsIgnoreCase(newPKVal)) {
                    bst.delete(oldPKVal);
                    bst.insert(newPKVal, offset);
                }
            }
            count++;
        }

        if (bst != null) storage.saveIndex(q.tableName, bst);
        System.out.println(count + " row(s) updated in '" + q.tableName.toUpperCase() + "'.");
    }
    
    // *******************************************************************************************************************
    // DELETE logic
    
    private void executeDelete(ParsedQuery q) throws IOException, ClassNotFoundException {
        if (storage.getDbDirectory() == null) {
            System.out.println("Error: No database selected.");
            return;
        }
        if (!storage.tableExists(q.tableName)) {
            System.out.println("Error: Table '" + q.tableName + "' does not exist.");
            return;
        }

        TableMeta meta = storage.loadMeta(q.tableName);

        // No WHERE — delete all tuples and remove schema entirely
        if (q.whereLeft == null) {
            new java.io.File(storage.getDbDirectory() + File.separator + q.tableName.toUpperCase() + ".meta").delete();
            new java.io.File(storage.getDbDirectory() + File.separator + q.tableName.toUpperCase() + ".dat").delete();
            new java.io.File(storage.getDbDirectory() + File.separator + q.tableName.toUpperCase() + ".idx").delete();
            System.out.println("Table '" + q.tableName.toUpperCase() + "' deleted.");
            return;
        }

        // WITH WHERE — soft delete matching records
        List<long[]> offsetList = new ArrayList<>();
        List<String[]> rowList  = new ArrayList<>();

        if (meta.primaryKey != null && q.whereLeft.equalsIgnoreCase(meta.primaryKey) && q.whereOp.equals("=")) {
            // BST direct lookup
            BSTIndex bst = storage.loadIndex(q.tableName);
            long offset  = bst.search(q.whereRight);
            if (offset != -1L) {
                try (RandomAccessFile raf = new RandomAccessFile(
                        storage.getDbDirectory() + File.separator + q.tableName.toUpperCase() + ".dat", "r")) {
                    String[] row = storage.readRecordAt(raf, meta, offset);
                    if (row != null) { offsetList.add(new long[]{offset}); rowList.add(row); }
                }
            }
        } else {
            // In-order BST traversal or sequential scan
            if (meta.primaryKey != null) {
                BSTIndex bst = storage.loadIndex(q.tableName);
                List<long[]> offsets = bst.inOrderOffsets();
                try (RandomAccessFile raf = new RandomAccessFile(
                        storage.getDbDirectory() + File.separator + q.tableName.toUpperCase() + ".dat", "r")) {
                    for (long[] entry : offsets) {
                        String[] row = storage.readRecordAt(raf, meta, entry[0]);
                        if (row != null && matchesCondition(row, meta, q.whereLeft, q.whereOp, q.whereRight)) {
                            offsetList.add(entry); rowList.add(row);
                        }
                    }
                }
            } else {
                int recSize = storage.recordSize(meta);
                try (RandomAccessFile raf = new RandomAccessFile(
                        storage.getDbDirectory() + File.separator + q.tableName.toUpperCase() + ".dat", "r")) {
                    long pos = 0;
                    while (pos + 1 + recSize <= raf.length()) {
                        String[] row = storage.readRecordAt(raf, meta, pos);
                        if (row != null && matchesCondition(row, meta, q.whereLeft, q.whereOp, q.whereRight)) {
                            offsetList.add(new long[]{pos}); rowList.add(row);
                        }
                        pos += 1 + recSize;
                    }
                }
            }
        }

        if (offsetList.isEmpty()) { 
        	System.out.println("Nothing found"); 
        	return; 
        }

        // Soft delete — mark records as dead and remove from BST
        BSTIndex bst = meta.primaryKey != null ? storage.loadIndex(q.tableName) : null;
        int count = 0;
        for (int i = 0; i < offsetList.size(); i++) {
            long offset = offsetList.get(i)[0];
            storage.markDeleted(q.tableName, offset);
            if (bst != null) {
                String pkVal = rowList.get(i)[meta.getColumnIndex(meta.primaryKey)];
                bst.delete(pkVal);
            }
            count++;
        }

        if (bst != null) storage.saveIndex(q.tableName, bst);
        System.out.println(count + " row(s) deleted from '" + q.tableName.toUpperCase() + "'.");
    }
    
    // ************************************************************************************************************
    // DESCRIBE logic
    
    private void executeDescribe(ParsedQuery q) throws IOException {
        if (storage.getDbDirectory() == null) {
            System.out.println("Error: No database selected.");
            return;
        }

        if (q.tableName.equalsIgnoreCase("ALL")) {
            File dir = new File(storage.getDbDirectory());
            String[] metaFiles = dir.list((d, name) -> name.endsWith(".meta"));
            if (metaFiles == null || metaFiles.length == 0) {
                System.out.println("No tables found.");
                return;
            }
            for (String metaFile : metaFiles) {
                String tName = metaFile.replace(".meta", "");
                storage.describeTable(tName);
            }
        } else {
            if (!storage.tableExists(q.tableName)) {
                System.out.println("Error: Table '" + q.tableName + "' does not exist.");
                return;
            }
            storage.describeTable(q.tableName);
        }
    }
    
    // ***************************************************************************************************************
    // LET Logic
    private void executeLet(ParsedQuery q) throws IOException, ClassNotFoundException {
        if (storage.getDbDirectory() == null) {
            System.out.println("Error: No database selected.");
            return;
        }
        String fromTable = q.fromTables.get(0);
        if (!storage.tableExists(fromTable)) {
            System.out.println("Error: Table '" + fromTable + "' does not exist.");
            return;
        }
        // Check letKey is in selectColumns
        boolean keyFound = false;
        for (String col : q.selectColumns) {
            if (col.equalsIgnoreCase(q.letKey)) { keyFound = true; break; }
        }
        if (!keyFound) {
            System.out.println("Error: Key '" + q.letKey + "' is not in selected columns.");
            return;
        }
        // Run SELECT to get matching rows
        TableMeta sourceMeta = storage.loadMeta(fromTable);
        List<String[]> rows;
        if (q.whereLeft != null && sourceMeta.primaryKey != null
                && q.whereLeft.equalsIgnoreCase(sourceMeta.primaryKey)
                && q.whereOp.equals("=")) {
            rows = new ArrayList<>();
            BSTIndex bst = storage.loadIndex(fromTable);
            long offset  = bst.search(q.whereRight);
            if (offset != -1L) {
                try (RandomAccessFile raf = new RandomAccessFile(
                        storage.getDbDirectory() + File.separator + fromTable.toUpperCase() + ".dat", "r")) {
                    String[] row = storage.readRecordAt(raf, sourceMeta, offset);
                    if (row != null) rows.add(row);
                }
            }
        } else {
            rows = storage.readAllRecords(fromTable);
            if (q.whereLeft != null) rows = applyWhere(rows, sourceMeta, q);
        }
        if (rows.isEmpty()) {
            System.out.println("Nothing found. Table '" + q.letTableName + "' not created.");
            return;
        }
        // Figure out which column indices to keep
        List<Integer> colIdxs  = new ArrayList<>();
        List<String>  colNames = new ArrayList<>();
        List<String>  colTypes = new ArrayList<>();
        if (q.selectColumns.get(0).equals("*")) {
            for (int i = 0; i < sourceMeta.columnNames.size(); i++) {
                colIdxs.add(i);
                colNames.add(sourceMeta.columnNames.get(i));
                colTypes.add(sourceMeta.columnTypes.get(i));
            }
        } else {
            for (String col : q.selectColumns) {
                int idx = sourceMeta.getColumnIndex(col);
                if (idx == -1) { System.out.println("Error: Unknown column '" + col + "'"); return; }
                colIdxs.add(idx);
                colNames.add(sourceMeta.columnNames.get(idx));
                colTypes.add(sourceMeta.columnTypes.get(idx));
            }
        }
        // Create new table
        if (storage.tableExists(q.letTableName)) {
            System.out.println("Error: Table '" + q.letTableName + "' already exists.");
            return;
        }
        ParsedQuery createQ   = new ParsedQuery();
        createQ.queryType     = "CREATE_TABLE";
        createQ.tableName     = q.letTableName;
        createQ.columnNames   = colNames;
        createQ.columnTypes   = colTypes;
        createQ.primaryKey    = q.letKey;
        storage.createTable(createQ);
        // Insert rows into new table
        int count = 0;
        for (String[] row : rows) {
            List<String> newRow = new ArrayList<>();
            for (int idx : colIdxs) newRow.add(row[idx]);
            try {
                storage.insertRecord(q.letTableName, newRow);
                count++;
            } catch (IllegalArgumentException e) {
                System.out.println("Warning: Skipped row — " + e.getMessage());
            }
        }
        System.out.println("Table '" + q.letTableName.toUpperCase() + "' created with " + count + " row(s).");
    }
    
    // ***************************************************************************************************************
    // RENAME
    
    private void executeRename(ParsedQuery q) throws IOException {
        if (storage.getDbDirectory() == null) {
            System.out.println("Error: No database selected.");
            return;
        }
        if (!storage.tableExists(q.tableName)) {
            System.out.println("Error: Table '" + q.tableName + "' does not exist.");
            return;
        }

        TableMeta meta = storage.loadMeta(q.tableName);

        if (q.newColumnNames.size() != meta.columnNames.size()) {
            System.out.println("Error: Column count mismatch. Table has "
                    + meta.columnNames.size() + " columns, got " + q.newColumnNames.size() + ".");
            return;
        }

        // Update PK name if it was renamed
        String newPK = null;
        if (meta.primaryKey != null) {
            int pkIdx = meta.getColumnIndex(meta.primaryKey);
            newPK = q.newColumnNames.get(pkIdx).toUpperCase();
        }

        // Build updated ParsedQuery to reuse writeMeta
        ParsedQuery updated      = new ParsedQuery();
        updated.tableName        = q.tableName;
        updated.columnNames      = q.newColumnNames;
        updated.columnTypes      = meta.columnTypes;
        updated.primaryKey       = newPK;

        String metaPath = storage.getDbDirectory() + File.separator + q.tableName.toUpperCase() + ".meta";
        
        storage.rewriteMeta(q.tableName, updated);

        System.out.println("Table '" + q.tableName.toUpperCase() + "' columns renamed successfully.");
    }
}
