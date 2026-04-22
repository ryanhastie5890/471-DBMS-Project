package backend;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Binary Search Tree index on a table's primary key.
 *
 * The whole tree is serialized as a single blob into tablename.idx
 * using Java ObjectOutputStream / ObjectInputStream.
 *
 * Key comparison is case-insensitive string comparison by default.
 * Numeric types (INTEGER / FLOAT) compare by numeric value.
 */
public class BSTIndex implements Serializable {
    private static final long serialVersionUID = 1L;

    private BSTNode root;
    private String  keyType;   // "INTEGER", "FLOAT", or "TEXT"

    public BSTIndex(String keyType) {
        this.root    = null;
        this.keyType = keyType.toUpperCase();
    }

    // ---------------------------------------------------------------
    // Comparison helper — case-insensitive, numeric-aware
    // ---------------------------------------------------------------
    private int compareKeys(String a, String b) {
        if (keyType.equals("INTEGER")) {
            return Long.compare(Long.parseLong(a.trim()), Long.parseLong(b.trim()));
        } else if (keyType.equals("FLOAT")) {
            return Double.compare(Double.parseDouble(a.trim()), Double.parseDouble(b.trim()));
        } else {
            return a.trim().compareToIgnoreCase(b.trim());
        }
    }

    // ---------------------------------------------------------------
    // Insert
    // ---------------------------------------------------------------
    public void insert(String key, long recordOffset) {
        root = insertRec(root, key, recordOffset);
    }

    private BSTNode insertRec(BSTNode node, String key, long recordOffset) {
        if (node == null) return new BSTNode(key, recordOffset);
        int cmp = compareKeys(key, node.key);
        if (cmp < 0)      node.left  = insertRec(node.left,  key, recordOffset);
        else if (cmp > 0) node.right = insertRec(node.right, key, recordOffset);
        else              node.recordOffset = recordOffset;  // duplicate PK — update offset
        return node;
    }

    // ---------------------------------------------------------------
    // Search — returns record offset, or -1 if not found
    // ---------------------------------------------------------------
    public long search(String key) {
        BSTNode node = searchRec(root, key);
        return (node == null) ? -1L : node.recordOffset;
    }

    private BSTNode searchRec(BSTNode node, String key) {
        if (node == null) return null;
        int cmp = compareKeys(key, node.key);
        if (cmp == 0) return node;
        return (cmp < 0) ? searchRec(node.left, key) : searchRec(node.right, key);
    }

    // ---------------------------------------------------------------
    // In-order traversal — returns list of (key, offset) pairs
    // Used for non-key searches on a PK-indexed table
    // ---------------------------------------------------------------
    public List<long[]> inOrderOffsets() {
        List<long[]> result = new ArrayList<>();
        inOrderRec(root, result);
        return result;
    }

    private void inOrderRec(BSTNode node, List<long[]> result) {
        if (node == null) return;
        inOrderRec(node.left, result);
        result.add(new long[]{ node.recordOffset });
        inOrderRec(node.right, result);
    }

    // ---------------------------------------------------------------
    // Delete
    // ---------------------------------------------------------------
    public void delete(String key) {
        root = deleteRec(root, key);
    }

    private BSTNode deleteRec(BSTNode node, String key) {
        if (node == null) return null;
        int cmp = compareKeys(key, node.key);
        if (cmp < 0) {
            node.left  = deleteRec(node.left,  key);
        } else if (cmp > 0) {
            node.right = deleteRec(node.right, key);
        } else {
            // Node to delete found
            if (node.left == null)  return node.right;
            if (node.right == null) return node.left;
            // Two children: replace with in-order successor
            BSTNode successor = findMin(node.right);
            node.key          = successor.key;
            node.recordOffset = successor.recordOffset;
            node.right        = deleteRec(node.right, successor.key);
        }
        return node;
    }

    private BSTNode findMin(BSTNode node) {
        while (node.left != null) node = node.left;
        return node;
    }

    // ---------------------------------------------------------------
    // Serialization helpers
    // ---------------------------------------------------------------

    /** Save the whole BST to a .idx file. */
    public void saveTo(String idxFilePath) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new FileOutputStream(idxFilePath))) {
            oos.writeObject(this);
        }
    }

    /** Load a BST from a .idx file. */
    public static BSTIndex loadFrom(String idxFilePath) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(
                new FileInputStream(idxFilePath))) {
            return (BSTIndex) ois.readObject();
        }
    }
}
