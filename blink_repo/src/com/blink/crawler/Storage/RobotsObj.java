package com.blink.crawler.Storage;

//package edu.upenn.cis455.crawler.info;

import java.io.BufferedReader;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.StringTokenizer;

//import edu.upenn.cis455.crawler.CrawlerClient;

/**
 * robotstxtinfo stores robots.txt info
 * @author cis455
 *
 */
public class RobotsObj implements Serializable{
	
	//private String domainName;
	private ArrayList<String> disallowedLinks;
	private ArrayList<String> allowedLinks;
	
	private Integer crawlDelay;
	
	/**
	 * constructor initializes robots to default
	 */
	public RobotsObj(ArrayList<String> dis,ArrayList<String> al,Integer cr)
	{
		disallowedLinks = dis;
		allowedLinks = al;
		crawlDelay = cr;
		
	}
	
	/**
	 * initializes info from robots
	 * @param reader to read robots 
	 * @param urlObject url object of robots
	 */
	
	
	
	
	
	/**
	 * returns disallowed links
	 * @return
	 */
	public ArrayList<String> getDisallowedLinks(){
		return disallowedLinks;
	}
	public ArrayList<String> getAllowedLinks(){
		return allowedLinks;
	}
	
	public Integer getCrawlDelay(){
		return crawlDelay;
	}
	
	
	
}
