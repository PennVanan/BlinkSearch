package com.blink.crawler.WorkerNode;

import java.util.ArrayList;

public class CommonParameters {

	public static boolean shutdown=false;
	public static ArrayList<Thread> workers=new ArrayList<Thread>(20);
	public static int max_crawl=100;
	public static int count=0;
	public static int clength=100000000;
}
