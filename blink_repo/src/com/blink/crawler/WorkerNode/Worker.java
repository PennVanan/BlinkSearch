package com.blink.crawler.WorkerNode;



import java.io.*;
import java.math.BigInteger;
import java.net.Socket;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;

import javax.servlet.*;
import javax.servlet.http.*;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.blink.avro.*;
import com.blink.crawler.Storage.DBWrapper;
import com.blink.crawler.Storage.Queue;
import com.blink.crawler.Storage.Url_Crawl;
import com.sleepycat.je.DatabaseException;


public class Worker extends HttpServlet  {

	public static DBWrapper dbase;

	public static Queue queue;
	public static HashMap<String,String> worker_ports=new HashMap<String,String>();
	String storagedir;
	String port;
	

	public static String master_iport;
	
	public WorkerThread w;
	public String mast;
	public static HashMap<String,Integer> host_crawl=new HashMap<String,Integer>();
	public static HashMap<String,RobotsTxtInfo> robot_info=new HashMap<String,RobotsTxtInfo>();
	
	//url->head/getstatus
	public String status="active";
	public static int workerid;
	public static HashMap<String,Long> delay_map=new HashMap<String,Long>();
	//domain_name->robotstxt object which has disallow info,allow link,crawl-delay
	public void init( ServletConfig sc)
	{
		//workerid=Integer.parseInt(sc.getInitParameter("id"));
		storagedir=sc.getInitParameter("storage");
		//TODO change if reqd
		//File t_f=new File("storagedir");
		master_iport=sc.getInitParameter("master");
		port=sc.getInitParameter("port");
		w=new WorkerThread();
		CommonParameters.workers=new ArrayList<Thread>(20);
		dbase=new DBWrapper();
		try {
			dbase.setup(sc.getInitParameter("storage"));
		} catch (DatabaseException e) {

			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		queue=new Queue(dbase);
		System.out.println("inside init before timer");
		Timer timer=new Timer();
		timer.scheduleAtFixedRate( new TimerTask(){ 
		public void run()
		{
				// System.out.println("hey");
				SagaClient sclient=new SagaClient("http://"+master_iport+"/Blink/master/workerstatus");
				try {
					sclient.doGet("port="+port+"&status="+status);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// System.out.println("howw u dooing");
			}
		}, 0, 10000);



	}
	public synchronized static void addindexToS3(long docID, String content, String charset, String filetype)
	{
		IndexSerialData sc=new IndexSerialData();
		sc.openFile("index"+docID+".avro");
		sc.addRecord(docID, content, charset, filetype);
		sc.closeFile();
		AWSCredentials credentials=null;
		try
		{
			credentials=new ProfileCredentialsProvider("default").getCredentials();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		File f=new File("index"+docID+".avro");
		AmazonS3 s3=new AmazonS3Client(credentials);
		String bucket_name="cis455blinkcrawl";
		s3.putObject(new PutObjectRequest(bucket_name,docID+"",f));
		////////////
		
		//S3Object obj=s3.getObject(new GetObjectRequest(bucket_name,key));
		
		///////
		
	}
	public synchronized static void addRankToS3(long docID,String url,LinkedList<String> hrefs)
	{
		
		LinkedList<CharSequence> lch=new LinkedList<CharSequence>(); 
		lch.addAll(hrefs);
		
		RankerSerialData sc=new RankerSerialData();
		sc.openFile("rank"+docID+".avro");
		sc.addRecord(docID, url, lch);
		sc.closeFile();
		AWSCredentials credentials=null;
		try
		{
			credentials=new ProfileCredentialsProvider("default").getCredentials();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		File f=new File("index"+docID+".avro");
		AmazonS3 s3=new AmazonS3Client(credentials);
		String bucket_name="cis455blinkrank";
		s3.putObject(new PutObjectRequest(bucket_name,docID+"",f));
		////////////
		
		//S3Object obj=s3.getObject(new GetObjectRequest(bucket_name,key));
		
		///////
	
	}
	public synchronized static void putTask(String s)
	{
		queue.enqueue(s);
	}
	public synchronized static void sendToWorker(LinkedList<String> l) throws IOException, NoSuchAlgorithmException
	{
		/////////////////
		MessageDigest md=null;
		for(String s:l)
		{
			String hash_st = null;
			md = MessageDigest.getInstance("SHA-1");
			byte[] b=md.digest(new URL(s).getHost().getBytes());

			for (int h=0; h < b.length; h++) 
			{
				hash_st +=Integer.toString( ( b[h] & 0xff ) + 0x100, 16).substring( 1 );
			}
	
	
			BigInteger big_int=new BigInteger(hash_st,16);
			BigInteger big_int1=new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",16);
			BigInteger bucketsize=big_int1.divide(BigInteger.valueOf(worker_ports.size()));
			BigInteger bucket_number=big_int.divide(bucketsize);
			int worker_number=bucket_number.intValue();
			//send a request to worker
			String ip_port=worker_ports.get(worker_number);
			String post_url="http://"+ip_port+"/from_worker?url="+s;
			URL u_goto=new URL(post_url);
			Socket socket=null;
			try {
				socket = new Socket(u_goto.getHost(), u_goto.getPort());
			} catch (IOException e) {
				
				e.printStackTrace();
			}
			String post_string="";
			PrintWriter out_obj = new PrintWriter(socket.getOutputStream(), true);
			out_obj.write("POST " +post_url+ " HTTP/1.0\r\n");
			out_obj.write("User-Agent: cis455crawler\r\n");

			out_obj.write("Content-Type: application/x-www-form-urlencoded\r\n");
			out_obj.write("Content-Length: "+post_string.getBytes().length+"\r\n");
			out_obj.write("\r\n");
			out_obj.write(post_string);
			out_obj.flush();
			socket.close();

		}
	/////////////////
	
		}

public void doGet(HttpServletRequest request, HttpServletResponse response) 
		throws java.io.IOException
{
	/*
	 * handle get
	 */
	System.out.println("heyyy+++++++++++++++++++");


}

public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException
{
	/*
	 * handle post for runmap,runreduce and pushdata
	 */
	System.out.println("Got RunCrawl Request ");
	PrintWriter out=response.getWriter();
	out.println("<html><p>SAGA</p></html>");
	String request_uri=request.getRequestURI();
	System.out.println("Request URI:" +request_uri);

	if(request_uri.contains("start_crawl"))
	{
		//start the Worker Threads , enqueue and crawl.
		System.out.println("entered start_crawl");
		String url_crawl=request.getParameter("url");
		System.out.println("url in 231 of Worker:"+url_crawl);
		int no_workers=Integer.parseInt((String)request.getParameter("nwork"));
		System.out.println(no_workers);
		for(int i=0;i<no_workers;i++)
		{
			//putting in queue
			System.out.println("setting stuff:"+"worker"+i+request.getParameter("worker"+i));
			worker_ports.put("worker"+i,request.getParameter("worker"+i));
		}
		System.out.println("enqueuing");
		queue.enqueue(url_crawl);
		WorkerThread w=new WorkerThread();
		System.out.println("starting workerthreads");
		for(int i=1;i<=10;i++)
		{
			Thread t=new Thread(w);
			t.setName("Thread:"+(i));
			CommonParameters.workers.add(t);
			t.start();
		}
		System.out.println("workers initialized");

	}
	else if(request_uri.endsWith("/from_worker"))
	{
		//get  url from another worker

		//inside from_worker
		String qstring=request.getQueryString();
		int i=qstring.indexOf("=");
		String url_new=qstring.substring(i+1);
		System.out.println("url_new is"+url_new);
		queue.enqueue(request.getParameter("url"));

	}
}
}



