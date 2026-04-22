package backend;

import java.io.*;
import java.util.*;

/**
 * Handles all disk I/O for the DBMS.
 *
 * Per table, three files live inside the active database folder:
 *   tablename.meta  – schema: col count, col names, types, PK name (plain text)
 *   tablename.dat   – fixed-width binary records
 *   tablename.idx   – serialized BSTIndex blob (only meaningful when PK exists)
 *
 * Fixed-width record layout (all fields padded / truncated to their max size):
 *   INTEGER : 4 bytes  (int, big-endian via DataOutputStream)
 *   FLOAT   : 8 bytes  (double, big-endian)
 *   TEXT    : 100 bytes (UTF-8, space-padded on the right, trimmed on read)
 *
 * A 1-byte "alive" flag precedes each record: 0x01 = live, 0x00 = deleted.
 */
public class StorageManager {

    // ---------------------------------------------------------------
    // Column sizes in bytes
    // ---------------------------------------------------------------
    public static final int INTEGER_SIZE = 4;
    public static final int FLOAT_SIZE   = 8;
    public static final int TEXT_SIZE    = 100;

    // Current active database directory
    private String dbDirectory;

    public StorageManager() {
        this.dbDirectory = null;
    }

    public void setDatabase(String dbName) {
        this.dbDirectory = dbName;
        new File(dbDirectory).mkdirs();
    }

    public String getDbDirectory() { return dbDirectory; }

    // ---------------------------------------------------------------
    // CREATE TABLE — writes all 3 files
    // ---------------------------------------------------------------
    public void createTable(ParsedQuery q) throws IOException {
        requireDb();
        String base = basePath(q.tableName);

        // 1. .meta file
        writeMeta(base + ".meta", q);

        // 2. .dat file — create empty
        new FileOutputStream(base + ".dat").close();

        // 3. .idx file — empty BST (serialized)
        String pkType = getPKType(q);
        BSTIndex bst = new BSTIndex(pkType != null ? pkType : "TEXT");
        bst.saveTo(base + ".idx");

        System.out.println("Table '" + q.tableName.toUpperCase() + "' created.");
    }

    // ---------------------------------------------------------------
    // Meta read / write
    // ---------------------------------------------------------------
    private void writeMeta(String metaPath, ParsedQuery q) throws IOException {
        try (PrintWriter pw = new PrintWriter(new FileWriter(metaPath))) {
            pw.println(q.columnNames.size());                 // number of columns
            pw.println(q.primaryKey != null ? q.primaryKey.toUpperCase() : "NONE");
            for (int i = 0; i < q.columnNames.size(); i++) {
                pw.println(q.columnNames.get(i).toUpperCase() + " " + q.columnTypes.get(i).toUpperCase());
            }
        }
    }

    /**
     * Returns a TableMeta object describing the schema of a table.
     */
    public TableMeta loadMeta(String tableName) throws IOException {
        requireDb();
        String metaPath = basePath(tableName) + ".meta";
        try (BufferedReader br = new BufferedReader(new FileReader(metaPath))) {
            int colCount   = Integer.parseInt(br.readLine().trim());
            String pkLine  = br.readLine().trim();
            String pk      = pkLine.equals("NONE") ? null : pkLine;

            List<String> names = new ArrayList<>();
            List<String> types = new ArrayList<>();
            for (int i = 0; i < colCount; i++) {
                String[] parts = br.readLine().trim().split("\\s+");
                names.add(parts[0]);
                types.add(parts[1]);
            }
            return new TableMeta(tableName.toUpperCase(), names, types, pk);
        }
    }

    // ---------------------------------------------------------------
    // INSERT — appends a fixed-width record to .dat, updates BST if PK
    // ---------------------------------------------------------------
    public void insertRecord(String tableName, List<String> values) throws IOException, ClassNotFoundException {
        requireDb();
        TableMeta meta = loadMeta(tableName);

        if (values.size() != meta.columnNames.size()) {
            throw new IllegalArgumentException("Value count mismatch: expected "
                    + meta.columnNames.size() + ", got " + values.size());
        }

     // Check key constraint if PK exists
        if (meta.primaryKey != null) {
            int pkIdx = meta.getColumnIndex(meta.primaryKey);
            String pkVal = values.get(pkIdx) != null ? values.get(pkIdx).trim() : "";
            if (pkVal.isEmpty()) {
                throw new IllegalArgumentException("Primary key cannot be null or empty.");
            }
            BSTIndex bst = loadIndex(tableName);
            if (bst.search(pkVal) != -1L) {
                throw new IllegalArgumentException("Duplicate primary key: " + pkVal);
            }
        }

        String datPath = basePath(tableName) + ".dat";
        long offset;
        try (RandomAccessFile raf = new RandomAccessFile(datPath, "rw")) {
            raf.seek(raf.length());
            offset = raf.getFilePointer();
            raf.writeByte(0x01);  // alive flag
            for (int i = 0; i < meta.columnNames.size(); i++) {
                writeField(raf, meta.columnTypes.get(i), values.get(i).trim());
            }
        }

        // Update BST index if table has a PK
        if (meta.primaryKey != null) {
            int pkIdx = meta.getColumnIndex(meta.primaryKey);
            String pkVal = values.get(pkIdx).trim();
            BSTIndex bst = loadIndex(tableName);
            bst.insert(pkVal, offset);
            bst.saveTo(basePath(tableName) + ".idx");
        }
    }

    // ---------------------------------------------------------------
    // Read all live records (returns List<String[]>; each entry = one row)
    // ---------------------------------------------------------------
    public List<String[]> readAllRecords(String tableName) throws IOException, ClassNotFoundException {
        requireDb();
        TableMeta meta = loadMeta(tableName);
        List<String[]> rows = new ArrayList<>();

        if (meta.primaryKey != null) {
            // Must traverse in BST in-order order
            BSTIndex bst = loadIndex(tableName);
            List<long[]> offsets = bst.inOrderOffsets();
            try (RandomAccessFile raf = new RandomAccessFile(basePath(tableName) + ".dat", "r")) {
                for (long[] entry : offsets) {
                    String[] row = readRecordAt(raf, meta, entry[0]);
                    if (row != null) rows.add(row);
                }
            }
        } else {
            // No PK — scan sequentially
            try (RandomAccessFile raf = new RandomAccessFile(basePath(tableName) + ".dat", "r")) {
                int recSize = recordSize(meta);
                long fileLen = raf.length();
                long pos = 0;
                while (pos + 1 + recSize <= fileLen) {
                    String[] row = readRecordAt(raf, meta, pos);
                    if (row != null) rows.add(row);
                    pos += 1 + recSize;
                }
            }
        }
        return rows;
    }

    /**
     * Read one record at given offset.
     * Returns null if the record is deleted (alive flag = 0x00).
     */
    public String[] readRecordAt(RandomAccessFile raf, TableMeta meta, long offset) throws IOException {
        raf.seek(offset);
        int alive = raf.readByte() & 0xFF;
        if (alive == 0x00) return null;

        String[] row = new String[meta.columnNames.size()];
        for (int i = 0; i < meta.columnNames.size(); i++) {
            row[i] = readField(raf, meta.columnTypes.get(i));
        }
        return row;
    }

    // ---------------------------------------------------------------
    // Mark a record as deleted (soft-delete)
    // ---------------------------------------------------------------
    public void markDeleted(String tableName, long offset) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(basePath(tableName) + ".dat", "rw")) {
            raf.seek(offset);
            raf.writeByte(0x00);
        }
    }

    // ---------------------------------------------------------------
    // Update a record in place (same offset, same fixed width)
    // ---------------------------------------------------------------
    public void updateRecordAt(String tableName, long offset, List<String> newValues, TableMeta meta) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(basePath(tableName) + ".dat", "rw")) {
            raf.seek(offset + 1);  // skip alive flag
            for (int i = 0; i < meta.columnNames.size(); i++) {
                writeField(raf, meta.columnTypes.get(i), newValues.get(i).trim());
            }
        }
    }

    // ---------------------------------------------------------------
    // DESCRIBE — prints schema to stdout
    // ---------------------------------------------------------------
    public void describeTable(String tableName) throws IOException {
        TableMeta meta = loadMeta(tableName);
        System.out.println(meta.tableName);
        for (int i = 0; i < meta.columnNames.size(); i++) {
            String pk = (meta.primaryKey != null && meta.columnNames.get(i).equalsIgnoreCase(meta.primaryKey))
                        ? " PRIMARY KEY" : "";
            System.out.println(meta.columnNames.get(i) + ": " + meta.columnTypes.get(i) + pk);
        }
        System.out.println();
    }
    
    public void rewriteMeta(String tableName, ParsedQuery q) throws IOException {
        writeMeta(basePath(tableName) + ".meta", q);
    }

    // ---------------------------------------------------------------
    // BST index helpers
    // ---------------------------------------------------------------
    public BSTIndex loadIndex(String tableName) throws IOException, ClassNotFoundException {
        return BSTIndex.loadFrom(basePath(tableName) + ".idx");
    }

    public void saveIndex(String tableName, BSTIndex bst) throws IOException {
        bst.saveTo(basePath(tableName) + ".idx");
    }

    // ---------------------------------------------------------------
    // Table existence check
    // ---------------------------------------------------------------
    public boolean tableExists(String tableName) {
        return new File(basePath(tableName) + ".meta").exists();
    }

    // ---------------------------------------------------------------
    // Field-level I/O
    // ---------------------------------------------------------------
    private void writeField(RandomAccessFile raf, String type, String value) throws IOException {
        switch (type.toUpperCase()) {
            case "INTEGER": {
                int v = value.isEmpty() ? 0 : (int) Double.parseDouble(value);
                raf.writeInt(v);
                break;
            }
            case "FLOAT": {
                double v = value.isEmpty() ? 0.0 : Double.parseDouble(value);
                raf.writeDouble(v);
                break;
            }
            case "TEXT":
            default: {
                // Strip surrounding quotes if present
                String s = value;
                if (s.startsWith("\"") && s.endsWith("\"")) s = s.substring(1, s.length() - 1);
                byte[] bytes = s.getBytes("UTF-8");
                byte[] padded = new byte[TEXT_SIZE];
                int len = Math.min(bytes.length, TEXT_SIZE);
                System.arraycopy(bytes, 0, padded, 0, len);
                raf.write(padded);
                break;
            }
        }
    }

    private String readField(RandomAccessFile raf, String type) throws IOException {
        switch (type.toUpperCase()) {
            case "INTEGER": return String.valueOf(raf.readInt());
            case "FLOAT":   return String.valueOf(raf.readDouble());
            case "TEXT":
            default: {
                byte[] buf = new byte[TEXT_SIZE];
                raf.readFully(buf);
                // trim trailing nulls / spaces
                int end = buf.length;
                while (end > 0 && (buf[end - 1] == 0 || buf[end - 1] == ' ')) end--;
                return new String(buf, 0, end, "UTF-8");
            }
        }
    }

    // ---------------------------------------------------------------
    // Utility
    // ---------------------------------------------------------------
    public int recordSize(TableMeta meta) {
        int size = 0;
        for (String t : meta.columnTypes) {
            switch (t.toUpperCase()) {
                case "INTEGER": size += INTEGER_SIZE; break;
                case "FLOAT":   size += FLOAT_SIZE;   break;
                default:        size += TEXT_SIZE;    break;
            }
        }
        return size;
    }

    private String basePath(String tableName) {
        return dbDirectory + File.separator + tableName.toUpperCase();
    }

    private void requireDb() {
        if (dbDirectory == null)
            throw new IllegalStateException("No database selected. Use 'USE dbname;' first.");
    }

    private String getPKType(ParsedQuery q) {
        if (q.primaryKey == null) return null;
        for (int i = 0; i < q.columnNames.size(); i++) {
            if (q.columnNames.get(i).equalsIgnoreCase(q.primaryKey))
                return q.columnTypes.get(i);
        }
        return "TEXT";
    }
}
