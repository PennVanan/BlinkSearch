package com.blink.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

public class IndexSerialData {
	// Serialize data to disk	
	DataFileWriter<DocumentSchema> dataFileWriter;

	public void openFile(String filename){
		File file = new File(filename);
		DatumWriter<DocumentSchema> userDatumWriter = new SpecificDatumWriter<DocumentSchema>(DocumentSchema.class);
		dataFileWriter = new DataFileWriter<DocumentSchema>(userDatumWriter);

		DocumentSchema temp = new DocumentSchema();
		try {
			dataFileWriter.create(temp.getSchema(), file);
		} catch (IOException e) {
			System.out.println("Error initializing avro file " + e.toString());
		}
	}


	public void addRecord(long docID, String content, String charset, String filetype){
		DocumentSchema ds = new DocumentSchema();
		ds.setDocID(docID);
		ds.setContent(content);

		if(charset!=null)
			ds.setCharset(charset);

		if(filetype!=null)
			ds.setFiletype(filetype);

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