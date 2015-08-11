package com.blink.webUI;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Represents a document that matches a particular word/set of 
 * words in a search query.
 *
 */
public class MatchingDocument {
	private String docID;
	private String url = "";
	private HashMap<String,ArrayList<Integer>> wordPositions;
	private double tfidfScore = 0;
	private double pageRankScore = 0;
	private double finalScore = 0;
	
	public MatchingDocument(String docID) {
		this.docID = docID;
		wordPositions = new HashMap<String, ArrayList<Integer>>();
	}

	public String getDocID() {
		return docID;
	}

	public String getUrl() {
		return url;
	}

	/**
	 * wordAndPositions maps each word in the query that is found in this document
	 * with the positions that word is found in this document.
	 * Eg: Query is "happy cute dog"
	 * No key for happy - it is not in this document.
	 * key: cute, value: [4,5,8]
	 * key: dog, value: [6,10]
	 */
	public HashMap<String, ArrayList<Integer>> getWordPositions() {
		return wordPositions;
	}

	public double getTfidfScore() {
		return tfidfScore;
	}

	public double getPageRankScore() {
		return pageRankScore;
	}
	
	public double getFinalScore() {
		return finalScore;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public void addWordAndPositions(String word, ArrayList<Integer> positions) {
		wordPositions.put(word, positions);
	}

	public void addTfidfScore(double tfidfScore) {
		this.tfidfScore += tfidfScore;
	}

	public void setPageRankScore(double pageRankScore) {
		this.pageRankScore = pageRankScore;
	}
	
	public void setFinalScore(double finalScore) {
		this.finalScore = finalScore;
	}

}
