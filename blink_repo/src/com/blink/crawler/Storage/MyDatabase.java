package com.blink.crawler.Storage;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/* TODO: Change path of the database store to point to some path in web.xml file */
/**
 * mydatabase is a wrapper over database 
 * @author cis455
 *
 */
public class MyDatabase {
	
	private String envPath = null;
	private Environment myEnv;
	private Database myDatabase;
	private String dbName;
	
	/**
	 * constructor
	 * @param myEnv
	 * @param dbName
	 */
	public MyDatabase(Environment myEnv, String dbName)
	{
		this.myEnv = myEnv;
		this.dbName = dbName;

		DatabaseConfig dbConfig = new DatabaseConfig();
		
		dbConfig.setAllowCreate(true);
		dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
		myDatabase = myEnv.openDatabase(null, dbName, dbConfig);
	
	}
	
	/**
	 * inserts into databse
	 * @param key
	 * @param data
	 * @throws Exception
	 */
	public void insert(Object key, Object data) throws Exception
	{
		Serializer serializer = new Serializer();
		byte[] keyBytes = serializer.serialize(key);
		byte[] dataBytes = serializer.serialize(data);
		
		final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
        final DatabaseEntry dataEntry = new DatabaseEntry(dataBytes);

        try 
        {
            final Transaction transaction = myEnv.beginTransaction(null, null);
            final OperationStatus result = myDatabase.put(transaction, keyEntry, dataEntry);
            if (result != OperationStatus.SUCCESS) 
            {
            	System.out.println("operation status-failure");
                throw new Exception("Error");
            }
            transaction.commit();
            
        }catch (Exception DE) 
        {
           System.out.println(DE);
        }
	}
	
	/**
	 * delete from the database 
	 * @param key
	 * @throws Exception
	 */
	public void delete(Object key) throws Exception
	{
		Serializer serializer = new Serializer();
		byte[] keyBytes = serializer.serialize(key);
		
		final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
    
        final Transaction transaction = myEnv.beginTransaction(null, null);
        final OperationStatus res = myDatabase.delete(transaction, keyEntry);
        
        if (res != OperationStatus.SUCCESS) 
        {
            throw new Exception("Error retrieving from database");
        }
        
        transaction.commit();
        return ;
	}
	
	/**
	 * gets object from databse
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public Object get(Object key) throws Exception
	{
		Serializer serializer = new Serializer();
		byte[] keyBytes = serializer.serialize(key);
		
		final DatabaseEntry keyEntry = new DatabaseEntry(keyBytes);
		Object result;
	
        final DatabaseEntry dataEntry = new DatabaseEntry();
    
        final Transaction transaction = myEnv.beginTransaction(null, null);
        final OperationStatus res = myDatabase.get(transaction, keyEntry, dataEntry, null);
        
        if (res != OperationStatus.SUCCESS) 
        {
            //throw new Exception("Error retrieving from database");
        	return null;
        }
        else 
        {
            result = serializer.deserialize(dataEntry.getData());
        }
        transaction.commit();
        return result;
	}
	
	/**
	 * removes database 
	 */
	public void removeDatabase()
	{
		final Transaction txn = myEnv.beginTransaction(null, null);
		//close();
       	myEnv.removeDatabase(txn, dbName);
        txn.commit();
	}
	
	/**
	 * closes database
	 */
	
	public void close()
	{
		myDatabase.close();
	}
	
	public Database getDatabase()
	{
		return myDatabase;
	}

}