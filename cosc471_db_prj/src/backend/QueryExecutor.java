package backend;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Routes a ParsedQuery to the correct execution method.
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
                case "CREATE_DB":
                    executeCreateDb(q);
                    break;
                case "USE":
                    executeUse(q);
                    break;
                case "CREATE_TABLE":
                    executeCreateTable(q);
                    break;
                case "INSERT":
                    executeInsert(q);
                    break;
                case "SELECT":
                    executeSelect(q);
                    break;
                case "UPDATE":
                    executeUpdate(q);
                    break;
                case "DELETE":
                    executeDelete(q);
                    break;
                case "DESCRIBE":
                    executeDescribe(q);
                    break;
                case "RENAME":
                    executeRename(q);
                    break;
                case "LET":
                    executeLet(q);
                    break;
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

    // ---------------------------------------------------------------
    // INSERT
    // ---------------------------------------------------------------
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

    // ---------------------------------------------------------------
    // SELECT
    // ---------------------------------------------------------------
    private void executeSelect(ParsedQuery q) throws IOException, ClassNotFoundException {
        if (storage.getDbDirectory() == null) {
            System.out.println("Error: No database selected.");
            return;
        }

        if (q.fromTables == null || q.fromTables.isEmpty()) {
            System.out.println("Error: No table specified in FROM.");
            return;
        }

        List<TableMeta> metas = new ArrayList<>();
        for (String tableName : q.fromTables) {
            if (!storage.tableExists(tableName)) {
                System.out.println("Error: Table '" + tableName + "' does not exist.");
                return;
            }
            metas.add(storage.loadMeta(tableName));
        }

        QueryData data = fetchRowsAndMeta(q, metas);

        if (data.rows.isEmpty()) {
            System.out.println("Nothing found");
            return;
        }

        if (q.aggregateFunc != null) {
            printAggregate(data.rows, data.meta, q);
        } else {
            printRows(data.rows, data.meta, q);
        }
    }

    // ---------------------------------------------------------------
    // UPDATE
    // ---------------------------------------------------------------
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

        List<long[]> offsetList = new ArrayList<>();
        List<String[]> rowList = new ArrayList<>();

        if (canUsePrimaryKeyDirectLookup(meta, q)) {
            BSTIndex bst = storage.loadIndex(q.tableName);
            long offset = bst.search(stripQuotes(q.whereRight));
            if (offset != -1L) {
                try (RandomAccessFile raf = new RandomAccessFile(
                        storage.getDbDirectory() + File.separator + q.tableName.toUpperCase() + ".dat", "r")) {
                    String[] row = storage.readRecordAt(raf, meta, offset);
                    if (row != null && rowMatchesQuery(row, meta, q)) {
                        offsetList.add(new long[]{offset});
                        rowList.add(row);
                    }
                }
            }
        } else {
            gatherMatchingRowsWithOffsets(q.tableName, meta, q, offsetList, rowList);
        }

        if (offsetList.isEmpty()) {
            System.out.println("Nothing found");
            return;
        }

        BSTIndex bst = meta.primaryKey != null ? storage.loadIndex(q.tableName) : null;
        int count = 0;

        for (int i = 0; i < offsetList.size(); i++) {
            long offset = offsetList.get(i)[0];
            String[] row = rowList.get(i);
            String oldPKVal = meta.primaryKey != null ? row[meta.getColumnIndex(meta.primaryKey)] : null;

            for (int j = 0; j < q.setColumns.size(); j++) {
                int colIdx = meta.getColumnIndex(q.setColumns.get(j));
                if (colIdx == -1) {
                    System.out.println("Error: Unknown column '" + q.setColumns.get(j) + "'");
                    return;
                }
                String val = stripQuotes(q.setValues.get(j));
                row[colIdx] = val;
            }

            storage.updateRecordAt(q.tableName, offset, Arrays.asList(row), meta);

            if (bst != null && oldPKVal != null) {
                String newPKVal = row[meta.getColumnIndex(meta.primaryKey)];
                if (!oldPKVal.equalsIgnoreCase(newPKVal)) {
                    bst.delete(oldPKVal);
                    bst.insert(newPKVal, offset);
                }
            }
            count++;
        }

        if (bst != null) {
            storage.saveIndex(q.tableName, bst);
        }
        System.out.println(count + " row(s) updated in '" + q.tableName.toUpperCase() + "'.");
    }

    // ---------------------------------------------------------------
    // DELETE
    // ---------------------------------------------------------------
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

        // No WHERE: remove the relation schema and files
        if (q.whereLeft == null) {
            storage.dropTable(q.tableName);
            System.out.println("Table '" + q.tableName.toUpperCase() + "' deleted.");
            return;
        }

        List<long[]> offsetList = new ArrayList<>();
        List<String[]> rowList = new ArrayList<>();

        if (canUsePrimaryKeyDirectLookup(meta, q)) {
            BSTIndex bst = storage.loadIndex(q.tableName);
            long offset = bst.search(stripQuotes(q.whereRight));
            if (offset != -1L) {
                try (RandomAccessFile raf = new RandomAccessFile(
                        storage.getDbDirectory() + File.separator + q.tableName.toUpperCase() + ".dat", "r")) {
                    String[] row = storage.readRecordAt(raf, meta, offset);
                    if (row != null && rowMatchesQuery(row, meta, q)) {
                        offsetList.add(new long[]{offset});
                        rowList.add(row);
                    }
                }
            }
        } else {
            gatherMatchingRowsWithOffsets(q.tableName, meta, q, offsetList, rowList);
        }

        if (offsetList.isEmpty()) {
            System.out.println("Nothing found");
            return;
        }

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

        if (bst != null) {
            storage.saveIndex(q.tableName, bst);
        }
        System.out.println(count + " row(s) deleted from '" + q.tableName.toUpperCase() + "'.");
    }

    // ---------------------------------------------------------------
    // DESCRIBE
    // ---------------------------------------------------------------
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

    // ---------------------------------------------------------------
    // LET
    // ---------------------------------------------------------------
    private void executeLet(ParsedQuery q) throws IOException, ClassNotFoundException {
        if (storage.getDbDirectory() == null) {
            System.out.println("Error: No database selected.");
            return;
        }

        if (q.fromTables == null || q.fromTables.isEmpty()) {
            System.out.println("Error: No table specified in FROM.");
            return;
        }

        for (String fromTable : q.fromTables) {
            if (!storage.tableExists(fromTable)) {
                System.out.println("Error: Table '" + fromTable + "' does not exist.");
                return;
            }
        }

        List<TableMeta> metas = new ArrayList<>();
        for (String t : q.fromTables) {
            metas.add(storage.loadMeta(t));
        }

        QueryData data = fetchRowsAndMeta(q, metas);
        List<String[]> rows = data.rows;
        TableMeta combinedMeta = data.meta;

        if (rows.isEmpty()) {
            System.out.println("Nothing found. Table '" + q.letTableName + "' not created.");
            return;
        }

        if (q.aggregateFunc != null) {
            System.out.println("Error: LET does not support aggregate SELECT results.");
            return;
        }

        boolean keyFound = false;
        if (q.selectColumns.size() == 1 && q.selectColumns.get(0).equals("*")) {
            keyFound = combinedMeta.getColumnIndex(q.letKey) != -1;
        } else {
            for (String col : q.selectColumns) {
                if (col.equalsIgnoreCase(q.letKey)) {
                    keyFound = true;
                    break;
                }
            }
        }

        if (!keyFound) {
            System.out.println("Error: Key '" + q.letKey + "' is not in selected columns.");
            return;
        }

        List<Integer> colIdxs = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        List<String> colTypes = new ArrayList<>();

        if (q.selectColumns.size() == 1 && q.selectColumns.get(0).equals("*")) {
            for (int i = 0; i < combinedMeta.columnNames.size(); i++) {
                colIdxs.add(i);
                colNames.add(combinedMeta.columnNames.get(i));
                colTypes.add(combinedMeta.columnTypes.get(i));
            }
        } else {
            for (String col : q.selectColumns) {
                int idx = combinedMeta.getColumnIndex(col);
                if (idx == -1) {
                    System.out.println("Error: Unknown column '" + col + "'");
                    return;
                }
                colIdxs.add(idx);
                colNames.add(combinedMeta.columnNames.get(idx));
                colTypes.add(combinedMeta.columnTypes.get(idx));
            }
        }

        if (storage.tableExists(q.letTableName)) {
            System.out.println("Error: Table '" + q.letTableName + "' already exists.");
            return;
        }

        ParsedQuery createQ = new ParsedQuery();
        createQ.queryType = "CREATE_TABLE";
        createQ.tableName = q.letTableName;
        createQ.columnNames = colNames;
        createQ.columnTypes = colTypes;
        createQ.primaryKey = q.letKey;
        storage.createTable(createQ);

        int count = 0;
        for (String[] row : rows) {
            List<String> newRow = new ArrayList<>();
            for (int idx : colIdxs) {
                newRow.add(row[idx]);
            }
            try {
                storage.insertRecord(q.letTableName, newRow);
                count++;
            } catch (IllegalArgumentException e) {
                System.out.println("Warning: Skipped row — " + e.getMessage());
            }
        }

        System.out.println("Table '" + q.letTableName.toUpperCase() + "' created with " + count + " row(s).");
    }

    // ---------------------------------------------------------------
    // RENAME
    // ---------------------------------------------------------------
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

        String newPK = null;
        if (meta.primaryKey != null) {
            int pkIdx = meta.getColumnIndex(meta.primaryKey);
            newPK = q.newColumnNames.get(pkIdx).toUpperCase();
        }

        ParsedQuery updated = new ParsedQuery();
        updated.tableName = q.tableName;
        updated.columnNames = q.newColumnNames;
        updated.columnTypes = meta.columnTypes;
        updated.primaryKey = newPK;

        storage.rewriteMeta(q.tableName, updated);
        System.out.println("Table '" + q.tableName.toUpperCase() + "' columns renamed successfully.");
    }

    // ---------------------------------------------------------------
    // Helpers: query fetching
    // ---------------------------------------------------------------
    private QueryData fetchRowsAndMeta(ParsedQuery q, List<TableMeta> metas) throws IOException, ClassNotFoundException {
        QueryData data = new QueryData();

        if (q.fromTables.size() == 1) {
            TableMeta meta = metas.get(0);
            String tableName = q.fromTables.get(0);
            List<String[]> rows;

            if (canUsePrimaryKeyDirectLookup(meta, q)) {
                rows = new ArrayList<>();
                BSTIndex bst = storage.loadIndex(tableName);
                long offset = bst.search(stripQuotes(q.whereRight));
                if (offset != -1L) {
                    try (RandomAccessFile raf = new RandomAccessFile(
                            storage.getDbDirectory() + File.separator + tableName.toUpperCase() + ".dat", "r")) {
                        String[] row = storage.readRecordAt(raf, meta, offset);
                        if (row != null && rowMatchesQuery(row, meta, q)) {
                            rows.add(row);
                        }
                    }
                }
            } else {
                rows = storage.readAllRecords(tableName);
                if (q.whereLeft != null) {
                    rows = applyWhere(rows, meta, q);
                }
            }

            data.rows = rows;
            data.meta = meta;
            return data;
        }

        List<String[]> combined = storage.readAllRecords(q.fromTables.get(0));

        for (int t = 1; t < q.fromTables.size(); t++) {
            List<String[]> nextRows = storage.readAllRecords(q.fromTables.get(t));
            List<String[]> product = new ArrayList<>();
            for (String[] row1 : combined) {
                for (String[] row2 : nextRows) {
                    String[] merged = new String[row1.length + row2.length];
                    System.arraycopy(row1, 0, merged, 0, row1.length);
                    System.arraycopy(row2, 0, merged, row1.length, row2.length);
                    product.add(merged);
                }
            }
            combined = product;
        }

        List<String> allColNames = new ArrayList<>();
        List<String> allColTypes = new ArrayList<>();
        for (TableMeta m : metas) {
            allColNames.addAll(m.columnNames);
            allColTypes.addAll(m.columnTypes);
        }
        TableMeta combinedMeta = new TableMeta("COMBINED", allColNames, allColTypes, null);

        if (q.whereLeft != null) {
            combined = applyWhere(combined, combinedMeta, q);
        }

        data.rows = combined;
        data.meta = combinedMeta;
        return data;
    }

    private boolean canUsePrimaryKeyDirectLookup(TableMeta meta, ParsedQuery q) {
        return q.whereLeft != null
                && q.whereConnector == null
                && meta.primaryKey != null
                && q.whereLeft.equalsIgnoreCase(meta.primaryKey)
                && "=".equals(q.whereOp);
    }

    private void gatherMatchingRowsWithOffsets(String tableName, TableMeta meta, ParsedQuery q,
                                               List<long[]> offsetList, List<String[]> rowList)
            throws IOException, ClassNotFoundException {

        if (meta.primaryKey != null) {
            BSTIndex bst = storage.loadIndex(tableName);
            List<long[]> offsets = bst.inOrderOffsets();
            try (RandomAccessFile raf = new RandomAccessFile(
                    storage.getDbDirectory() + File.separator + tableName.toUpperCase() + ".dat", "r")) {
                for (long[] entry : offsets) {
                    String[] row = storage.readRecordAt(raf, meta, entry[0]);
                    if (row != null && rowMatchesQuery(row, meta, q)) {
                        offsetList.add(entry);
                        rowList.add(row);
                    }
                }
            }
        } else {
            int recSize = storage.recordSize(meta);
            try (RandomAccessFile raf = new RandomAccessFile(
                    storage.getDbDirectory() + File.separator + tableName.toUpperCase() + ".dat", "r")) {
                long pos = 0;
                while (pos + 1 + recSize <= raf.length()) {
                    String[] row = storage.readRecordAt(raf, meta, pos);
                    if (row != null && rowMatchesQuery(row, meta, q)) {
                        offsetList.add(new long[]{pos});
                        rowList.add(row);
                    }
                    pos += 1 + recSize;
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Helpers: printing rows
    // ---------------------------------------------------------------
    private void printRows(List<String[]> rows, TableMeta meta, ParsedQuery q) {
        List<Integer> colIdxs = resolveSelectedColumns(meta, q);
        if (colIdxs == null) {
            return;
        }

        StringBuilder header = new StringBuilder();
        for (int idx : colIdxs) {
            header.append(meta.columnNames.get(idx)).append("\t");
        }
        System.out.println(header.toString().trim());

        int rowNum = 1;
        for (String[] row : rows) {
            StringBuilder line = new StringBuilder(rowNum++ + ". ");
            for (int idx : colIdxs) {
                line.append(row[idx]).append("\t");
            }
            System.out.println(line.toString().trim());
        }
    }

    private List<Integer> resolveSelectedColumns(TableMeta meta, ParsedQuery q) {
        List<Integer> colIdxs = new ArrayList<>();

        if (q.selectColumns == null || q.selectColumns.isEmpty()) {
            System.out.println("Error: No columns selected.");
            return null;
        }

        if (q.selectColumns.size() == 1 && q.selectColumns.get(0).equals("*")) {
            for (int i = 0; i < meta.columnNames.size(); i++) {
                colIdxs.add(i);
            }
            return colIdxs;
        }

        for (String col : q.selectColumns) {
            boolean found = false;
            for (int i = 0; i < meta.columnNames.size(); i++) {
                if (meta.columnNames.get(i).equalsIgnoreCase(col)) {
                    colIdxs.add(i);
                    found = true;
                    break;
                }
            }
            if (!found) {
                System.out.println("Error: Unknown column '" + col + "'");
                return null;
            }
        }
        return colIdxs;
    }

    // ---------------------------------------------------------------
    // Helpers: aggregate
    // ---------------------------------------------------------------
    private void printAggregate(List<String[]> rows, TableMeta meta, ParsedQuery q) {
        String func = q.aggregateFunc.toUpperCase();

        if (func.equals("COUNT")) {
            if (!"*".equals(q.aggregateColumn)) {
                int idx = meta.getColumnIndex(q.aggregateColumn);
                if (idx == -1) {
                    System.out.println("Error: Unknown column '" + q.aggregateColumn + "'");
                    return;
                }
            }
            System.out.println("COUNT");
            System.out.println("1. " + rows.size());
            return;
        }

        int idx = meta.getColumnIndex(q.aggregateColumn);
        if (idx == -1) {
            System.out.println("Error: Unknown column '" + q.aggregateColumn + "'");
            return;
        }

        String type = meta.columnTypes.get(idx);

        if (func.equals("MIN") || func.equals("MAX")) {
            String best = rows.get(0)[idx];

            for (int i = 1; i < rows.size(); i++) {
                String current = rows.get(i)[idx];
                int cmp = compareValues(best, current, type);

                if (func.equals("MIN") && cmp > 0) {
                    best = current;
                } else if (func.equals("MAX") && cmp < 0) {
                    best = current;
                }
            }

            System.out.println(func + "(" + meta.columnNames.get(idx) + ")");
            System.out.println("1. " + best);
            return;
        }

        if (func.equals("AVERAGE")) {
            if (!(type.equalsIgnoreCase("INTEGER") || type.equalsIgnoreCase("FLOAT") || type.equalsIgnoreCase("INT"))) {
                System.out.println("Error: AVERAGE requires a numeric column.");
                return;
            }

            double sum = 0.0;
            int count = 0;
            for (String[] row : rows) {
                try {
                    sum += Double.parseDouble(row[idx].trim());
                    count++;
                } catch (NumberFormatException e) {
                    // skip bad numeric data
                }
            }

            if (count == 0) {
                System.out.println("Nothing found");
                return;
            }

            double avg = sum / count;
            System.out.println("AVERAGE(" + meta.columnNames.get(idx) + ")");
            System.out.println("1. " + avg);
            return;
        }

        System.out.println("Error: Unsupported aggregate '" + func + "'");
    }

    // ---------------------------------------------------------------
    // Helpers: where logic
    // ---------------------------------------------------------------
    private List<String[]> applyWhere(List<String[]> rows, TableMeta meta, ParsedQuery q) {
        List<String[]> result = new ArrayList<>();

        for (String[] row : rows) {
            if (rowMatchesQuery(row, meta, q)) {
                result.add(row);
            }
        }

        return result;
    }

    private boolean rowMatchesQuery(String[] row, TableMeta meta, ParsedQuery q) {
        if (q.whereLeft == null) {
            return true;
        }

        boolean first = matchesCondition(row, meta, q.whereLeft, q.whereOp, q.whereRight);

        if (q.whereConnector == null) {
            return first;
        }

        boolean second = matchesCondition(row, meta, q.whereLeft2, q.whereOp2, q.whereRight2);

        if (q.whereConnector.equalsIgnoreCase("AND")) {
            return first && second;
        } else if (q.whereConnector.equalsIgnoreCase("OR")) {
            return first || second;
        }

        return first;
    }

    private boolean matchesCondition(String[] row, TableMeta meta, String left, String op, String right) {
        int idx = meta.getColumnIndex(left);
        if (idx == -1) {
            return false;
        }

        String colType = meta.columnTypes.get(idx);
        String cellVal = row[idx];

        int rightIdx = meta.getColumnIndex(right);
        String compareVal;

        if (rightIdx != -1) {
            compareVal = row[rightIdx];
        } else {
            compareVal = stripQuotes(right);
        }

        int cmp;
        try {
            if (colType.equalsIgnoreCase("INTEGER") || colType.equalsIgnoreCase("INT")) {
                cmp = Long.compare(Long.parseLong(cellVal.trim()), Long.parseLong(compareVal.trim()));
            } else if (colType.equalsIgnoreCase("FLOAT")) {
                cmp = Double.compare(Double.parseDouble(cellVal.trim()), Double.parseDouble(compareVal.trim()));
            } else {
                cmp = cellVal.trim().compareToIgnoreCase(compareVal.trim());
            }
        } catch (NumberFormatException e) {
            return false;
        }

        switch (op) {
            case "=":
                return cmp == 0;
            case "!=":
                return cmp != 0;
            case "<":
                return cmp < 0;
            case ">":
                return cmp > 0;
            case "<=":
                return cmp <= 0;
            case ">=":
                return cmp >= 0;
            default:
                return false;
        }
    }

    private int compareValues(String a, String b, String type) {
        if (type.equalsIgnoreCase("INTEGER") || type.equalsIgnoreCase("INT")) {
            return Long.compare(Long.parseLong(a.trim()), Long.parseLong(b.trim()));
        } else if (type.equalsIgnoreCase("FLOAT")) {
            return Double.compare(Double.parseDouble(a.trim()), Double.parseDouble(b.trim()));
        } else {
            return a.trim().compareToIgnoreCase(b.trim());
        }
    }

    private String stripQuotes(String s) {
        if (s == null) {
            return null;
        }
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    // ---------------------------------------------------------------
    // Helpers: delete entire table files
    // ---------------------------------------------------------------
    private void deleteTableFiles(String tableName) {
        String dbDir = storage.getDbDirectory();
        String upper = tableName.toUpperCase();

        File meta = new File(dbDir + File.separator + upper + ".meta");
        File dat = new File(dbDir + File.separator + upper + ".dat");
        File idx = new File(dbDir + File.separator + upper + ".idx");

        if (meta.exists()) {
            meta.delete();
        }
        if (dat.exists()) {
            dat.delete();
        }
        if (idx.exists()) {
            idx.delete();
        }
    }

    // ---------------------------------------------------------------
    // Small container
    // ---------------------------------------------------------------
    private static class QueryData {
        List<String[]> rows;
        TableMeta meta;
    }
}