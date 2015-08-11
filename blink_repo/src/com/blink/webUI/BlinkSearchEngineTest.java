package com.blink.webUI;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;

public class BlinkSearchEngineTest {
	
	BlinkSearchEngine blink;

	@Before
	public void setUp() throws Exception {
		blink = new BlinkSearchEngine();
		File stopWordFile = new File("WebContent/static/stopwords_en.txt");
		try {
			FileReader fr = new FileReader(stopWordFile);
			BufferedReader reader = new BufferedReader(fr);
			blink.stopWords = new ArrayList<String>();
			String word = "";
			while ((word = reader.readLine()) != null) {
				blink.stopWords.add(word);
			}
			reader.close();
		} catch (Exception e) {
			System.out.println("Error generating stopwords");
		}
		
		blink.setDynamoDB();
	}

	@Test
	public void testGetIPLocation() throws Exception {
		Document doc = blink.getIPLocation("68.80.51.83");
		assertTrue(doc!= null);
	}
	
	@Test
	public void testGetIPLocationInJSON() throws Exception {
		JSONObject json = blink.getIPLocationInJson("68.80.51.83");
		assertTrue(json != null);
		assertTrue(json.get("zip_code").equals("19406"));
	}
	
	@Test
	public void testGetYelpResults() {
		JSONObject results = blink.getYelpResults("Red Lobster", "19406");
		assertTrue(results != null);
		System.out.println(results.toString());
		System.out.println(results.get("businesses"));
		JSONArray bus = (JSONArray) results.get("businesses");
		JSONObject first = bus.getJSONObject(4);
		System.out.println(first.get("name"));
		//System.out.println(results.keySet().toString());
	}
	
	@Test
	public void testRemoveStopWords() {
		String query1 = "To be or not to be";
		assertTrue(blink.removeStopWordsInQuery(query1).equals("To be or not to be"));
		String query2 = "Into the void";
		assertTrue(blink.removeStopWordsInQuery(query2).equals("void"));
		String query3 = "My dog is great";
		assertTrue(blink.removeStopWordsInQuery(query3).equals("dog great"));
	}
	
	@Test
	public void testAddItemToDynamoDb() {
		//blink.addItemsToIndexer();
		blink.addItemsToCrawlerTable();
	}
	
	@Test
	public void testGetItemFromIndexer() {
		Item item = blink.getItemFromIndexer("small");
		assertTrue(item != null);
		//assertTrue(item.get("tfidf").toString().equals("0.33"));
		Map<String,List<BigDecimal>> posting_list = item.getMap("posting_list");
		for (String key : posting_list.keySet()) {
			System.out.println(key);
			List<BigDecimal> positions = posting_list.get(key);
			for (BigDecimal position : positions) {
				System.out.println(position.intValueExact());
			}
		}
		
		//What happens if there is no match?
		Item item2 = blink.getItemFromIndexer("nope");
		assertTrue(item2 == null);
	}
	
	@Test
	public void testGetItemFromCrawlerTable() {
		Item item = blink.getItemFromCrawlerTable("doc1");
		assertTrue(item != null);
		assertTrue(item.get("pagerank").toString().equals("0.11"));
	}
	
	@Test
	public void testGetMatchingDocuments() {
		Map<String,MatchingDocument> results = blink.getMatchingDocuments("small ugly cats");
		assertTrue(results.keySet().size() == 2);
		MatchingDocument one = results.get("doc1");
		assertTrue(one.getWordPositions().keySet().contains("small"));
		assertTrue(one.getWordPositions().keySet().contains("ugly"));
		assertFalse(one.getWordPositions().keySet().contains("dogs"));
		ArrayList<Integer> positions = one.getWordPositions().get("small");
		assertTrue(positions.contains(5));
		assertTrue(positions.contains(10));
	}
	
	@Test
	public void testGetSortedResults() {
		Map<String,MatchingDocument> results = blink.getMatchingDocuments("small ugly cats");
		MatchingDocument[] sortedResults = blink.getSortedResults(results);
		assertTrue(sortedResults[0].getDocID().equals("doc1"));
		for (MatchingDocument doc : sortedResults) {
			System.out.println(doc.getDocID());
			System.out.println(doc.getUrl());
			System.out.println(doc.getFinalScore());
		}
	}

}
