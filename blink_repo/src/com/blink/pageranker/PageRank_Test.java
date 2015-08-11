package com.blink.pageranker;

import java.io.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import java.net.*;
/*
 * 
import com.blink.indexer.InvertedIndex;
import com.blink.indexer.Path;
import com.blink.indexer.InvertedIndex.Map;
import com.blink.indexer.InvertedIndex.Reduce;
*/

public class PageRank_Test {

	public void map(String key, String value) {
		String[] values = value.split("#");
		float page_rank = Float.parseFloat(values[0]);
		String[] url_names = values[1].split(",");
		for(String x: url_names) {
			String new_key = x;
			String from_url = key;
			int total_links = url_names.length;
		    String new_value = from_url + "#" + Float.toString(page_rank) + "#" + total_links;
			System.out.println("#");
			System.out.println("Key: " + new_key);
			System.out.println("Value: " + new_value);
			System.out.println("#");
			File f = new File("/root/map_values.txt");
			BufferedWriter output;
			try {
			output = new BufferedWriter(new FileWriter(f, true));
			output.write(new_key + "\t" + new_value + "\n");
			output.close();
			} catch(Exception ex) {
				System.out.println("Exception caught: " + ex.toString());
			} 
		} 
	}
	
	public void reduce(String key, String[] values) {
		   float pageRank = 0.00F;
		   for(String v: values) {
			   System.out.println("Values v");
			   String[] vals = v.split("#");  
			   pageRank = pageRank + (Float.valueOf(vals[1])/Float.valueOf(vals[2]));
		   }
		   pageRank = (pageRank * 0.85F) + 0.15F;
		   System.out.println("PageRank is: " + pageRank);
		
	}
	
	public static void main(String args[]) { 
		
		try {
		String myurl = "http://127.0.0.1:8081/Blink/worker/start_crawl?worker0=127.0.0.1:8081&nwork=1"; 
		URL url = new URL(myurl.substring(0, myurl.indexOf("?"))); 
		System.out.println("URL: " + url);
		} catch(Exception ex)
		{
			System.out.println("Exception caught: " + ex.toString());
		}
		
		
		System.out.println("Hi");
/*
		JobConf conf = new JobConf(InvertedIndex.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
 
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);  */
	 
	}
	 
}
