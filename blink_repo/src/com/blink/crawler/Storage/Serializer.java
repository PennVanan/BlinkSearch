package com.blink.crawler.Storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * serializer is used to serialize and deserialize objects before storing and retreiving from database
 * @author cis455
 *
 */
public class Serializer 
{
	/**
	 * serialize object
	 * @param obj
	 * @return
	 * @throws Exception
	 */
	public byte[] serialize(Object obj) throws Exception
	{
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
	    ObjectOutputStream out = new ObjectOutputStream(bout);
	    out.writeObject(obj);
	    out.close();
	    return bout.toByteArray();
	}

	/**
	 * deserialize object
	 * @param bytes
	 * @return
	 * @throws Exception
	 */
	public Object deserialize(byte[] bytes) throws Exception
	{
		ByteArrayInputStream bout = new ByteArrayInputStream(bytes);
	    ObjectInputStream out = new ObjectInputStream(bout);
	    return out.readObject();
	}
}