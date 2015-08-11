package com.blink.crawler.Storage;


import com.blink.crawler.Storage.DBWrapper;
import com.blink.crawler.Storage.MyDatabase;

/**
 * queue is a wrapper for on disk queue
 * @author cis455
 *
 */
public class Queue 
{
	private DBWrapper wrapper;
	public MyDatabase URLQueue;
	private Integer enqueuePtr;
	private Integer dequeuePtr;
	private boolean close;
	private boolean access;
	private boolean enq;
	
	/**
	 * constructor takes db environment
	 * @param wrapper
	 */
	public Queue(DBWrapper wrapper)
	{
		this.wrapper = wrapper;
		URLQueue = wrapper.getURLQueue();
		enqueuePtr = 0;
		dequeuePtr = 0;
		close = false;
		access = true;
		enq = true;
	}
	
	/**
	 * helper
	 */
	public void closeQueue()
	{
		close = true;
	}
	
	/**
	 * enqueue puts object into queue
	 * @param data
	 */
	public synchronized void enqueue(Object data)
	{
		try{
			URLQueue.insert(++enqueuePtr, data);
			notify();
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * dequeue dequeues the object from queue
	 * @return
	 */
	public synchronized String dequeue()
	{
		String url = null;
		try{
			while (dequeuePtr == enqueuePtr || access == false)
			{
				wait();
				if (close == true)
				{
					return "ShutDown";
				}
			}
			access = false;
			dequeuePtr++;
			url = (String) URLQueue.get(dequeuePtr);
			URLQueue.delete(dequeuePtr);
			access = true;
			notify();
		}catch(Exception e)
		{
			System.out.println(e);
		}
		
		return url;
	}
	
	/**
	 * returns the size of the queue
	 * @return
	 */
	public int size()
	{
		return (enqueuePtr - dequeuePtr);
	}
}