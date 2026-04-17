package backend;

/**
 * Entry point for the DBMS.
 *
 * Right now it drives the hardcoded test queries in Parser.
 * Later: swap Parser() for a version that reads from stdin or a file.
 */
public class Main {
    public static void main(String[] args) {
        StorageManager storage  = new StorageManager();
        QueryExecutor  executor = new QueryExecutor(storage);
        Parser         parser   = new Parser();

        System.out.println("=== DBMS Started ===\n");

        while (parser.hasNext()) {
            ParsedQuery q = parser.getNextQuery();
            System.out.print("> [" + q.queryType + "] ");
            if (q.tableName != null) System.out.print("table=" + q.tableName + " ");
            if (q.dbName    != null) System.out.print("db="    + q.dbName    + " ");
            System.out.println();

            executor.execute(q);
            System.out.println();
        }

        System.out.println("=== Done ===");
    }
}
