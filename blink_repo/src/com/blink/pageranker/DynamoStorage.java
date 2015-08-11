package com.blink.pageranker;

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
	static String parentLinksTable = "parentLinks"; 
	static String pageRankTable = "pageRanks"; 
	
	public void addEntryMR1(String link, String parent_links){
		Table table = dynamoDB.getTable(parentLinksTable); 
		
		//creating map and lists to put in DDB	 
		
		try {
			Item item = new Item()                
			.withPrimaryKey("link", link)          
			.withString("parent_links", parent_links);
			table.putItem(item);
		}
		catch(Exception e){
			System.out.println("Error adding entry to DynamoDB - MapReduce 1" + e.toString());
		}
	}
	
	
	public void updatePageRank(String link, float rank){
		Table table = dynamoDB.getTable(pageRankTable);  
		try {
			Item item = new Item()                
			.withPrimaryKey("link", link)          
			.withFloat("rank", rank);
			table.putItem(item);
		}
		catch(Exception e){
			System.out.println("Error adding entry to DynamoDB - MapReduce 1" + e.toString());
		}
	}
}
