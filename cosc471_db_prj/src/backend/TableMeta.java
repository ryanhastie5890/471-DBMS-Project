package backend;

import java.util.List;

/** Holds the schema of one table (loaded from .meta file). */
public class TableMeta {
    public String       tableName;
    public List<String> columnNames;
    public List<String> columnTypes;
    public String       primaryKey;   // null if none

    public TableMeta(String tableName, List<String> columnNames,
                     List<String> columnTypes, String primaryKey) {
        this.tableName   = tableName;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.primaryKey  = primaryKey;
    }

    /** Returns the 0-based index of the column with the given name (case-insensitive), or -1. */
    public int getColumnIndex(String colName) {
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(colName)) return i;
        }
        return -1;
    }
}
