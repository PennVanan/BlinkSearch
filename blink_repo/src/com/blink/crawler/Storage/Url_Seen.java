package com.blink.crawler.Storage;

import java.io.Serializable;

public class Url_Seen implements Serializable{

	String url;
	
public Url_Seen(String url)
	{
		this.url=url;
	}
	String getUrl()
	{
		return url;
	}
	void setUrl(String url)
	{
		this.url=url;
	}
}
