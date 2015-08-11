package com.blink.indexer.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

public class DynamoStorage {
	static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(new ProfileCredentialsProvider()));	 	 
	static String invertedIndexTableName = "InvertedIndex";

	public void addEntry(String word, ArrayList<IndexValue> index_values) {
		Table table = dynamoDB.getTable(invertedIndexTableName);
		Map<String, List<Double>> posting_list = new HashMap<String, List<Double>>();
		
		//creating map and lists to put in DDB											
		for(IndexValue iv: index_values){
			posting_list.put(Long.toString(iv.docID), iv.locations);
		}
		
		try {
			Item item = new Item()                
			.withPrimaryKey("word", word)                	     
			.withMap("posting_list", posting_list);
			table.putItem(item);
		}
		catch(Exception e){
			System.out.println("Error adding entry to DynamoDB " + e.toString());
		}
	}
}
