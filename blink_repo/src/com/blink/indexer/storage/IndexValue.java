package com.blink.indexer.storage;

import java.util.ArrayList;

public class IndexValue {
	long docID;
	ArrayList<Double> locations;
	double normalized_tf;
	
	public long getDocID(){
		return docID;
	}
	
	public ArrayList<Double> getLocations(){
		return locations;
	}
	
	public double getTF(){
		return normalized_tf;
	}
	
	public double getTFIDF(){
		return locations.get(0);
	}
	
	
	
	public void setDocID(long docID){
		this.docID = docID;
	}
	
	public void setLocations(ArrayList<Double> locations){
		//the first value of location list will have tf-idf scores
		this.locations.add(0d);
		this.locations.addAll(locations);
		
	}
	
	public void setTFIDF(double data){
		locations.add(0, data);
	}
	
	public void setNormalizedTermFrequency(double data){
		this.normalized_tf = data;
	}
}
