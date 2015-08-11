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

public class PageRank_MR2 extends Configured implements Tool{
	static int number_of_documents;
	static DynamoStorage ds = new DynamoStorage();	

	public static class PR_Mapper2 extends Mapper<Text, Text, Text, Text>{
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			int flag = 0;
			String val = value.toString();
			String[] values = val.split(","); 
		 	List<String> tokensList = Arrays.asList(value.toString().split(",")); 
			if(value.toString().contains("$")){
				flag = 1;
			} 
			for(String s: tokensList) {
				context.write(new Text(s.toString() + ":1"), new Text(value.toString() + ": " + flag));
			}
		}
	}
	

	public static class PR_Reducer2 extends Reducer<Text, Text, Text, Text> {
  	@Override
  	public void reduce(Text key, Iterable<Text> iterable_values, Context context) throws IOException, InterruptedException {
  		String values_list = ""; 
		Iterator<Text> values = iterable_values.iterator();
  		while(values.hasNext()) {
  			values_list = values_list + "," + values.next().getBytes();
  		}
  		context.write(new Text(key), new Text(values_list));
  	}	 
   }


	public int run(String[] args) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());
		job.setJarByClass(PageRank_MR2.class);
		job.setJobName("Generating Parent Links"); 
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1])); 
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(PR_Mapper2.class);
		AvroJob.setInputKeySchema(job, DocumentSchema.getClassSchema());
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		job.setReducerClass(PR_Reducer2.class);
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT)); 
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	//File1 Changed
	public static void main(String[] args) throws Exception {
		//setting input and output directories
		args[0]="s3n://cis455crawl";
		args[1]=""; 
		AmazonS3 s3 = new AmazonS3Client(new InstanceProfileCredentialsProvider());
		ObjectListing object_listing = s3.listObjects(new ListObjectsRequest().withBucketName(args[0].substring(args[0].lastIndexOf('/')+1)));
		number_of_documents = object_listing.getObjectSummaries().size(); 
		int res = ToolRunner.run(new PageRank_MR2(), args); 
	}
}
