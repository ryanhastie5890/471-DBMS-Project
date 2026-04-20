//Binary search tree for tables
public class BST {

	private Node root;
	
	public BST() {
		root = null;
	}
	
	//create method for first creation
	public void createTree(int [] values) {
		for(int i = 0; i < values.length; i++	) {
			insertValue(values[i]);
		}
				}
	
	//insert method
	public void insertValue(int value) {
	if(root == null) {
		root = new Node(value,null);
	}
	else {
	insertRecursion(value, root);
	}
	}
	
	//recursive part of insert
	public void insertRecursion(int value, Node current) {
		if(value <= current.value) {
			if(current.left == null) {
				current.left = new Node(value,current);
			}
			else {
				insertRecursion(value,current.left);
			}
		}
		else if ( value > current.value) {
			if(current.right == null) {
				current.right = new Node(value,current);
			}
			else {
				insertRecursion(value, current.right);
			}
		}
	}
	
	//search 
	public Node search(int value) {
		if(root != null && root.value == value) {
			return root;
		}
		else if(root != null) {
			return searchRecursion(value,root);
		}
		else {
			return null;
		}
	}
	
	public Node searchRecursion(int value, Node current) {
		if(current.value == value) {
			return current;
		}
		else if(value < current.value) {
			if(current.left == null) {
				return null;
			}
			else {
				return searchRecursion(value,current.left);
			}
				
		}
		else {
			if(current.right == null) {
				return null;
			}
			else {
				return searchRecursion(value,current.right);
			}
		}
	}
	
	//deletion **************UPDATE PARENTS
	public Node deletion(int value) {
		Node delete = search(value);
		
		if(delete == null) {
			return null;
		}
		
		if(delete.left==null &&delete.right==null ) {
			if(value <= delete.parent.value) {
				delete.parent.left = null;
				return delete;
			}
			else {
				delete.parent.right = null;
				return delete;
			}
		}
		else if(delete.left!= null && delete.right == null) {
			if(value <= delete.parent.value) {
				delete.left.parent = delete.parent;
				delete.parent.left = delete.left;
				return delete;
			}
			else {
				delete.left.parent = delete.parent;
				delete.parent.right = delete.left;
				return delete;
			}
		}
		else if(delete.left == null && delete.right != null) {
			if(value <= delete.parent.value) {
				delete.right.parent = delete.parent;
				delete.parent.left = delete.right;
				return delete;
			}
			else {
				delete.right.parent = delete.parent;
				delete.parent.right = delete.right;
				return delete;
			}
		}else {
			Node successor = findSuccessor(delete.right);
			successor.parent = delete.parent;
			successor.left = delete.left;
			if(value <= delete.parent.value) {
				delete.parent.left = successor;
				return delete;
			}
			else {
				delete.parent.right = successor;
				return delete;
			}
		}
	}
	
	public Node findSuccessor(Node start) {
		Node current = start;
		
		while(current.left != null) {
			current = current.left;
		}
		
		return current;
	}
	
	//print
	public void printAll() {
		if(root.left != null && root.right != null) {
		System.out.println("Node: "+root.value +" Left: "+root.left.value+ " Right: "+root.right.value);
		}
		else if(root.left != null && root.right == null) {
			System.out.println("Node: "+root.value +" Left: "+root.left.value+ " Right: NULL");
		}
		else if(root.left == null && root.right != null) {
			System.out.println("Node: "+root.value +" Left: NULL"+ " Right: "+root.right.value);
		}
		else {
			System.out.println("Node: "+root.value +" Left: NULL"+" Right: NULL");
		}
		if(root.left != null) {
			printAllRecursion(root.left);
		}
		if(root.right != null) {
			printAllRecursion(root.right);
		}
	}
	public void printAllRecursion(Node node) {
		if(node.left != null && node.right != null) {
		System.out.println("Node: "+node.value +" Left: "+node.left.value+ " Right: "+node.right.value);
		}
		else if(node.left != null && node.right == null) {
			System.out.println("Node: "+node.value +" Left: "+node.left.value+ " Right: NULL");
		}
		else if(node.left == null && node.right != null) {
			System.out.println("Node: "+node.value +" Left: NULL"+ " Right: "+node.right.value);
		}
		else {
			System.out.println("Node: "+node.value +" Left: NULL"+" Right: NULL");
		}
		if(node.left != null) {
			printAllRecursion(node.left);
		}
		if(node.right != null) {
			printAllRecursion(node.right);
		}
	}
	public class Node{//node inner class
		private int value;
	    private Node left;
	    private Node right;
	    private Node parent;
	    
	    public Node(int val, Node par) {
	    	value = val;
	    	left = null;
	    	right = null;
	    	parent = par;
	    }
	}
}