package com.blink.crawler.MasterNode;

public class Status {
	
	public String status;
	public String key;
	public int id;
	
	//long store_time;
	
	Status(String status,String class_name,int id)
	{
		/*
		 * storing status from workers as a object
		 * 
		 */
		this.status=status;
		this.key=key;
		this.id=id;
		//this.store_time=store_time;
	}

}