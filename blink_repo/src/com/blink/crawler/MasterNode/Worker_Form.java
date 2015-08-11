package com.blink.crawler.MasterNode;


public class Worker_Form {

	public String classname ;
	public String inputdir ;
	public String outputdir;
	public String mapThreads;
	public String reduceThreads;
	public Worker_Form(String classname, String inputdir, String outputdir, String mapThreads, String reduceThreads) 
	{
		/*
		 * job form data
		 * 
		 */
		this.classname = classname;
		this.inputdir = inputdir;
		this.outputdir = outputdir;
		this.mapThreads = mapThreads;
		this.reduceThreads = reduceThreads;
	}
	public static void main(String args[])
	{
		System.out.println("hey");
	}

}