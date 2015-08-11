package com.blink.indexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.blink.avro.DocumentSchema;
import com.blink.indexer.NLP.NLP;
import com.blink.indexer.storage.DynamoStorage;
import com.blink.indexer.storage.IndexValue;
import com.blink.utils.HtmlParser;

public class MapReduceIndexing extends Configured implements Tool{
	static int number_of_documents;
	static DynamoStorage ds = new DynamoStorage();	

	public static class IndexMapper extends Mapper<AvroKey<DocumentSchema>, NullWritable, Text, IndexValue>{
		public void map(AvroKey<DocumentSchema> key, NullWritable value, Context context) throws IOException, InterruptedException {

			long docID = key.datum().getDocID();
			CharSequence document = key.datum().getContent(); 					

			//cleaning document and getting content
			HtmlParser parser = new HtmlParser((String)document);
			String content = parser.getContent();
			
			//processing document
			NLP nlp = new NLP();
			HashMap<String, ArrayList<Double>> all_words = nlp.tokenize(content);

			double document_euclidian_norm=0;
			for(String word : all_words.keySet()){
				document_euclidian_norm += Math.pow(all_words.get(word).size(),2);
			}

			document_euclidian_norm = Math.sqrt(document_euclidian_norm);

			for(String word : all_words.keySet()){
				IndexValue iv = new IndexValue();
				iv.setDocID(docID);
				iv.setLocations(all_words.get(word));
				iv.setNormalizedTermFrequency((double)all_words.get(word).size()/document_euclidian_norm);
				context.write(new Text(word), iv);				
			}
		}
	}
	

	public static class IndexReducer extends Reducer<Text, IndexValue, AvroKey<CharSequence>, AvroValue<Integer>> {

		@Override
		public void reduce(Text key, Iterable<IndexValue> iterable_values, Context context) throws IOException, InterruptedException {

			ArrayList<IndexValue> index_values = new ArrayList<IndexValue>();
			
			int docs_containing_word = 0;
			Iterator<IndexValue> values = iterable_values.iterator();
			
			while(values.hasNext()){
				docs_containing_word++;
				index_values.add(values.next());
			}
			
			for(IndexValue iv : index_values){
				iv.setTFIDF(iv.getTF() * Math.log((double)docs_containing_word/(double)number_of_documents));
			}

			ds.addEntry(key.toString(), index_values);
		}
	}


	public int run(String[] args) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());
		job.setJarByClass(MapReduceIndexing.class);
		job.setJobName("Making inverted index");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(IndexMapper.class);
		AvroJob.setInputKeySchema(job, DocumentSchema.getClassSchema());
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		job.setReducerClass(IndexReducer.class);
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
		
		int res = ToolRunner.run(new MapReduceIndexing(), args);
		System.exit(res);
	}
}
