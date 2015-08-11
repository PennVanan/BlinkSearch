package com.blink.utils;

import com.blink.indexer.storage.IndexValue;

public class IndexTrie{

	//inner class defining node of the Trie
	Node root;
	class Node{
		Node[] nodes;
		boolean is_end;
		IndexValue data;

		//working with basic ASCII for now
		public Node() {
			nodes = new Node[256];
		}
	}	




	//method to get value
	public IndexValue getValue(String word){
		word = word.toLowerCase();

		if(!lookup(word))
			return null;

		Node tbr = getLastNode(word);
		return tbr.data;
	}





	//public method to add word and it's data or replace data
	public boolean add(String word, IndexValue data){
		//saving only lower case words
		word = word.toLowerCase();

		if(root==null)
			root=new Node();

		int node_index=word.charAt(0);

		if(root.nodes[node_index]==null)
			root.nodes[node_index]=new Node();

		if(word.length()>1)		//if the word is longer than a single character
			root.nodes[node_index]=add(word.substring(1), data, root.nodes[node_index]);
		else{					//if the word is a single character
			root.nodes[node_index].is_end=true;
			root.nodes[node_index].data=data;
		}

		//return successful
		return true;
	}

	//overloaded add
	private Node add(String word, IndexValue data, Node working_node){
		int node_index = word.charAt(0);

		if(working_node.nodes[node_index]==null){
			working_node.nodes[node_index]=new Node();
			if(word.length()>1)		//if the word is longer than a single character
				working_node.nodes[node_index]=add(word.substring(1), data, working_node.nodes[node_index]);
			else{					//if the word is a single character
				working_node.nodes[node_index].is_end=true;
				working_node.nodes[node_index].data=data;
			}
		}

		//returning the newly created node
		return working_node;
	}







	//method to look up the specified word
	public boolean lookup(String word){
		word = word.toLowerCase();

		int node_index=word.charAt(0);
		if(root.nodes[node_index]==null)
			return false;
		else{
			if(word.length()==1 && root.nodes[node_index].is_end){
				return true;
			}			
			return lookup(word.substring(1), root.nodes[node_index]);
		}
	}

	//overloaded lookup
	private boolean lookup(String word, Node working_node){
		int node_index=word.charAt(0);
		if(working_node.nodes[node_index]==null)
			return false;
		else{
			if(word.length()==1 && working_node.nodes[node_index].is_end){
				return true;
			}
			return lookup(word.substring(1), working_node.nodes[node_index]);
		}
	}





	//method to get last node of a word (i.e. where the word actually ends)
	private Node getLastNode(String word){
		int node_index=word.charAt(0);

		if(word.length()==1 && root.nodes[node_index].is_end){
			return root.nodes[node_index];
		}

		return getLastNode(word.substring(1), root.nodes[node_index]);
	}

	//overloaded getLastNode
	private Node getLastNode(String word, Node working_node){
		int node_index=word.charAt(0);

		if(word.length()==1 && working_node.nodes[node_index].is_end){
			return working_node.nodes[node_index];
		}

		return getLastNode(word.substring(1), working_node.nodes[node_index]);
	}
}

