package com.blink.avro;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

public class RankerSerialData {
	// Serialize data to disk	
	DataFileWriter<RankerSchema> dataFileWriter;

	public void openFile(String filename){
		File file = new File(filename);
		DatumWriter<RankerSchema> userDatumWriter = new SpecificDatumWriter<RankerSchema>(RankerSchema.class);
		dataFileWriter = new DataFileWriter<RankerSchema>(userDatumWriter);

		RankerSchema temp = new RankerSchema();
		try {
			dataFileWriter.create(temp.getSchema(), file);
		} catch (IOException e) {
			System.out.println("Error initializing avro file " + e.toString());
		}
	}


	public void addRecord(long docID, String url, List<CharSequence> outlinks){
		RankerSchema ds = new RankerSchema();
		ds.setDocID(docID);
		ds.setUrl(url);
		ds.setOutlinks(outlinks);
		
		try {
			dataFileWriter.append(ds);			
		} catch (IOException e) {
			System.out.println("Error adding to avro file " + e.toString());
		}
	}

	public void closeFile(){
		try {
			dataFileWriter.close();
		} catch (IOException e) {
			System.out.println("Error closing file " + e.toString());
		}
	}
}