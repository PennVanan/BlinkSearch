package com.blink.pageranker;

import java.io.IOException;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.blink.avro.DocumentSchema;
import com.blink.avro.RankerSchema;
import com.blink.indexer.NLP.NLP;
import com.blink.pageranker.DynamoStorage; 
import com.blink.indexer.storage.IndexValue;
import com.blink.utils.HtmlParser;

public class PageRank_MR3 extends Configured implements Tool{
	static int number_of_documents;
	static DynamoStorage ds = new DynamoStorage();	

	public static class PR_Mapper3 extends Mapper<Text, Text, Text, List<String>>{
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			 String val_str = value.toString();
			 List<String> tokensList = Arrays.asList(val_str.split(","));
			 List<String> rankList = null;
			 rankList.add(new String("1"));
			 int main_rank = Integer.parseInt(key.toString());
			  int count = 0;
			 for(String v: tokensList) {
				  String indiv_values[] = v.split(",");
				  if(Integer.parseInt(indiv_values[1])==1)
					  count = count + 1; 
			 }
			
			 int rank_of_each_val = main_rank/count;
			 for(String v: tokensList) {
				 String indiv_values[] = v.split(",");
				 if(Integer.parseInt(indiv_values[1])!=0)
				 {
					 context.write(new Text(indiv_values[0].toString()), rankList); 
				 }
				 System.out.println("Emitting " + indiv_values[0] + ": " + rank_of_each_val);			 
			 }
			  context.write(key, tokensList);  
       }
	}
	

	public static class PR_Reducer3 extends Reducer<Text, Text, Text, Text> {
  	@Override
  	public void reduce(Text key, Iterable<Text> iterable_values, Context context) throws IOException, InterruptedException { 
		float res = 0.0F;
		String colon_array = null;
		int count = 0;
		Iterator iter = iterable_values.iterator();
		while(iter.hasNext()) {
	        String s = iter.next().toString();
	        if(s.contains(":")==true) 
	        {
	        	colon_array = s;
	        }
	        
	        else {
			res = res + Float.parseFloat(s);
			count = count + 1;
	        }
		}  
		float page_rank = new Float((1-0.85) + (0.85  * (res/count))); 
		String[] values = colon_array.split(",");
		System.out.println("VALUE: ");
		context.write(new Text(key + ": " + page_rank), new Text(colon_array));   
	    Iterator iter1 = iterable_values.iterator();
	    while(iter1.hasNext()) {
	       String s = iter1.next().toString();
           ds.updatePageRank(s, page_rank);
	    }
  	}	 
   }


	public int run(String[] args) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());
		job.setJarByClass(PageRank_MR3.class);
		job.setJobName("Generating Parent Links"); 
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1])); 
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(PR_Mapper3.class);
		AvroJob.setInputKeySchema(job, DocumentSchema.getClassSchema());
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		job.setReducerClass(PR_Reducer3.class);
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT)); 
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	
	public static void main(String[] args) throws Exception {
		//setting input and output directories
		args[0]="s3n://cis455crawl";
		args[1]=""; 
		AmazonS3 s3 = new AmazonS3Client(new InstanceProfileCredentialsProvider());
		ObjectListing object_listing = s3.listObjects(new ListObjectsRequest().withBucketName(args[0].substring(args[0].lastIndexOf('/')+1)));
		number_of_documents = object_listing.getObjectSummaries().size(); 
		int res = ToolRunner.run(new PageRank_MR3(), args); 
	}
}
