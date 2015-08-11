package com.blink.utils;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Whitelist;
import org.jsoup.select.Elements;

//this class return an HTML tag, it's attributes and it's content
public class HtmlParser {
	String title="";
	String content="";
	String description="";	

	String data;

	//constructor
	public HtmlParser(String data) {
		this.data = data;
		process();
	}

	//public getter methods
	public String getTitle(){
		return title;
	}

	public String getDescription(){
		return description;
	}

	public String getContent(){
		return content;
	}


	private void process(){
		data = Jsoup.clean(data, Whitelist.relaxed());
		Document doc = Jsoup.parse(data, "UTF-8");

		//getting title
		title = doc.title();

		//getting meta description
		Elements meta_tags = doc.getElementsByTag("meta");
		for(Element one_meta : meta_tags){
			if(one_meta.attr("name").equalsIgnoreCase("description")){
				description = one_meta.attr("content");
				break;
			}
		}

		//getting content
		content = doc.text();
	}
}
