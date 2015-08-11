package com.blink.crawler.Storage;



import java.io.Serializable;
import java.util.LinkedList;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;


public class Url_Crawl implements Serializable{

	
	private String url;
	private Long last_crawled_time;
	private String content;
	private LinkedList<String> outlinks;
	private String content_type;
	private int hits=0;
	public void set_Links(LinkedList<String> l) {
		outlinks = l;
	}

	public LinkedList<String> get_links() {
		return outlinks;
	}
	public void hit()
	{
		hits=hits+1;
	}

	public void setLastCrawledTime(long time) {
		last_crawled_time = new Long(time);
	}

	public long getLastCrawledTime() {
		return last_crawled_time.longValue();
	}

	public void setURL(String data) {
		url = data;
	}

	public String getURL() {
		return url;
	}
	
	public void setContentType(String data) {
		content_type = data;
	}

	public String getContentType() {
		return content_type;
	}
	
	public void setContent(String data) {
		content = data;
	}

	public String getContent() {
		return content;
	}
} 
