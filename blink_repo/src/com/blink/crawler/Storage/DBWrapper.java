package com.blink.crawler.Storage;

import java.io.File;

import java.io.FileNotFoundException;



import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;

/**
 * dbwrapper is wrapper for creating all databases
 * @author cis455
 *
 */
public class DBWrapper 
{
	
	private static String envDirectory = null;
	
	private static Environment myEnv;
	private static EntityStore store;
	//contains urls and its type last crawled outlinks
	//docid is key object is Url_Crawl class obj
	private MyDatabase Url_Crawl;
	//url frontier
	//workerid-key value-Url_Frontier
	private MyDatabase Url_Frontier;
	//robots info
	//key-host  value--RobotsTxtInfo
	private MyDatabase Robots;
	//key-docid value-url
	private MyDatabase seenURLs;
	
	private MyDatabase url_queueDB;
	//private MyDatabase pageDB;
	
	
	public DBWrapper()
	{
		
	}
	
	/**
	 * creates all databases
	 * @param homeDirectory
	 * @throws DatabaseException
	 * @throws FileNotFoundException
	 */
	public void setup(String homeDirectory) throws DatabaseException, FileNotFoundException
	{
		File f = new File(homeDirectory);
		if (!f.exists())
		{
			f.mkdirs();
		}
		
		EnvironmentConfig config = new EnvironmentConfig();
		config.setTransactional(true);
		config.setAllowCreate(true);
		myEnv = new Environment(f, config);    
		
		Url_Crawl = new MyDatabase(myEnv, "webContent");
		this.url_queueDB = new MyDatabase(myEnv, "URLQueue");
		Robots = new MyDatabase(myEnv, "robotsDB");
		seenURLs = new MyDatabase(myEnv, "seenURLs");
		//urlDB = new MyDatabase(myEnv, "urlDB");
		//pageDB = new MyDatabase(myEnv, "pageDB");
	}
	
	/**
	 * get functions are used to retrieve references to databases.
	 * @return
	 */
	
	
	public MyDatabase getcrawledURLs()
	{
		return this.Url_Crawl;
	}
	
	public MyDatabase getrobotsDB()
	{
		return this.Robots;
	}
	
	public MyDatabase getseenURLsDB()
	{
		return this.seenURLs;
	}
	public MyDatabase getURLQueue()
	{
		return this.url_queueDB;
	}
	
	
	/**
	 * closes all databases
	 * @throws DatabaseException
	 */
	public void close() throws DatabaseException
	{
		this.Url_Crawl.close();
		this.Url_Frontier.close();
		this.seenURLs.close();
		this.Robots.close();
		
		myEnv.close();
	}
	
	public final Environment getEnvironment()
	{
		return myEnv;
	}
}