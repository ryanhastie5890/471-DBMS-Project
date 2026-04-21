package backend;

import java.io.Serializable;

/**
 * One node in the BST index.
 * key          – the primary-key value (stored as String; compare numerically when type is INTEGER/FLOAT)
 * recordOffset – byte offset of the matching record inside the .dat file
 * left / right – child pointers (null = no child)
 */
public class BSTNode implements Serializable {
    private static final long serialVersionUID = 1L;

    public String  key;
    public long    recordOffset;
    public BSTNode left;
    public BSTNode right;

    public BSTNode(String key, long recordOffset) {
        this.key          = key;
        this.recordOffset = recordOffset;
        this.left         = null;
        this.right        = null;
    }
}
