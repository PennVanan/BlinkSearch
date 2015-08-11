package com.blink.crawler.WorkerNode;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.net.ssl.HttpsURLConnection;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.tidy.Tidy;
import org.xml.sax.InputSource;
//import org.xml.sax.SAXException;


import org.xml.sax.SAXException;




//TODO here check once
//import com.blink.crawler.Worker;
import com.blink.crawler.WorkerNode.RobotsTxtInfo;
import com.blink.crawler.Storage.Url_Crawl;


public class SagaClient {

	protected URL url;
	protected String hname;
	protected int pno;
	protected String ctype = null;
	protected String full_url;
	private String cpath;
	//protected String method;
	protected String char_encoding;
	protected boolean pvalid = true;
	protected String protocol;
	// default constructor
	public SagaClient() {

	}

	public SagaClient(String url1) {
		/*
		 * constructor
		 * 
		 */
		//System.out.println("Inside client constructr");
		try {
			//	this.method=method;
			full_url=url1;
			this.url = new URL(url1);
			this.protocol=url.getProtocol();
			this.hname = url.getHost();
			this.cpath = url.getPath();
			// handling root directory case
			if (cpath == "") 
				cpath = "/";
			this.pno = url.getPort();
			// if port no is not specified
			if(this.pno == -1) 
				this.pno = 80;
		} 
		catch (Exception e) {
			this.pvalid = false;
		}
	}
	public SagaClient(String url_get,String get_query)
	{
		try {
			//	this.method=method;
			full_url=url_get;
			this.url = new URL(url_get);
			this.protocol=url.getProtocol();
			this.hname = url.getHost();
			this.cpath = url.getPath();
			// handling root directory case
			if (cpath == "") 
				cpath = "/";
			this.pno = url.getPort();
			// if port no is not specified
			if(this.pno == -1) 
				this.pno = 80;
		} 
		catch (Exception e) {
			this.pvalid = false;
		}
	}
	public void doGet(String post_string) throws UnknownHostException, IOException
	{
		if(protocol.equals("http")){
			Socket socket = new Socket(this.hname, this.pno);

			PrintWriter out_obj = new PrintWriter(socket.getOutputStream(), true);
			out_obj.write("GET " + this.full_url+"?"+post_string + " HTTP/1.1\r\n");
			System.out.println("GET " + this.full_url+"?"+post_string + " HTTP/1.1\r\n");
			//out_obj.write("User-Agent: cis455crawler\r\n");
			out_obj.write("Host: " + this.hname + ":" + this.pno + "\r\n");
			out_obj.write("Connection: close\r\n\r\n");
			out_obj.flush();
			BufferedReader in_obj = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			StringBuffer content_data = new StringBuffer();
			String curr_line = in_obj.readLine();
			//socket.close();
		}
		
	}

	/**
	 * @param url1
	 * @return
	 * @throws IOException
	 */
	@SuppressWarnings("resource")
	public String getcontent (String url1) throws IOException {
		/*
		 * get content with a GET request.
		 * 
		 */

		if (! this.pvalid)
			return null;
		if(protocol.equals("http")){
			HttpURLConnection http=(HttpURLConnection)this.url.openConnection();
			http.setDoInput(true);
			http.setRequestMethod("GET");
			//http.setRequestProperty("Aceept-Language","english");
			http.setRequestProperty("User-Agent", "cis455crawler");
			http.setDoOutput(true);
			http.connect();
			int response_code=http.getResponseCode();
			BufferedReader inFromClient = new BufferedReader(new InputStreamReader (http.getInputStream()));
			String line=inFromClient.readLine();
			StringBuffer content=new StringBuffer();
			while(line!=null)
			{
				//System.out.println("line is "+line);
				content.append(line);
				line=inFromClient.readLine();
			}
			this.char_encoding=http.getContentType().split("charset=")[1].split(";")[0];
			//System.out.println(content.toString());
			return content.toString();	
		}
		else if(protocol.equals("https"))
		{
			//System.out.println("getcontent https");

			URL u=new URL(full_url);
			//System.out.println("full url is "+u.toString());
			HttpsURLConnection con = (HttpsURLConnection)u.openConnection();
			con.setDoInput(true);
			con.setRequestMethod("GET");
			con.setRequestProperty("User-Agent","cis455crawler");

			con.setDoOutput(true);
			con.connect();
			this.char_encoding=con.getContentType().split("charset=")[1].split(";")[0];
			BufferedReader inFromClient = new BufferedReader(new InputStreamReader (con.getInputStream()));
			String line=inFromClient.readLine();
			StringBuffer content=new StringBuffer();
			while(line!=null)
			{
				//System.out.println("line is "+line);
				content.append(line);
				line=inFromClient.readLine();
			}

			//System.out.println(content.toString());
			return content.toString();
		}
		return null;
	} 
	public LinkedList<Object> doHEAD(long date_ifmod) throws IOException
	{
		/*
		 * perform head request if file has been modified since last crawled found in db
		 * 
		 */
		LinkedList<Object> return_strings=new LinkedList<Object>();
		//System.out.println("inside head request:");
		if (! this.pvalid)
		{
			//System.out.println("broken url");
			return null;
		}
		if(protocol.equals("http"))
		{
			HttpURLConnection con = (HttpURLConnection)url.openConnection();
			con.setRequestMethod("HEAD");
			con.setRequestProperty("User-Agent", "cis455crawler");
			//check
			con.setIfModifiedSince(date_ifmod);
			con.setInstanceFollowRedirects(false);
			con.setDoInput(true);
			con.setDoOutput(true);
			this.char_encoding=con.getContentEncoding();
			if(con.getResponseCode()==200)
			{
				return_strings.add(0,con.getContentType());
				return_strings.add(1,con.getContentLength());
				return_strings.add(2,"200");
				return_strings.add(3,con.getContentType().split("charset=")[1].split(";")[0]);
			}
			else if(con.getResponseCode()==301)
			{
				Worker.putTask(con.getHeaderField("Location"));
				return null;
			}
			else if(con.getResponseCode()==304){
				//System.out.println("ayya 304"+con.getContentType());
				return_strings.add(0,con.getContentType());
				return_strings.add(1,con.getContentLength());
				return_strings.add(2,"304");
				return_strings.add(3,con.getContentType().split("charset=")[1].split(";")[0]);
			}
			else {
				return null;
			}
		}
		else if(protocol.equals("https"))
		{
			//System.out.println("Inside https else");
			HttpsURLConnection con = (HttpsURLConnection)url.openConnection();
			con.setRequestMethod("HEAD");
			con.setRequestProperty("User-Agent", "cis455crawler");
			//check here
			
			con.setIfModifiedSince(date_ifmod);
			con.setInstanceFollowRedirects(false);
			con.setDoInput(true);
			con.setDoOutput(true);
			this.char_encoding=con.getContentType().split("charset=")[1].split(";")[0];
			if(con.getResponseCode()==200)
			{
				return_strings.add(0,con.getContentType());
				return_strings.add(1,con.getContentLength());
				return_strings.add(2,"200");
				return_strings.add(3,con.getContentType().split("charset=")[1].split(";")[0]);
			}
			else if(con.getResponseCode()==301)
			{
				Worker.putTask(con.getHeaderField("Location"));
				return null;
			}
			else if(con.getResponseCode()==304){
				//System.out.println("ayya 304"+con.getContentType());
				return_strings.add(0,con.getContentType());
				return_strings.add(1,con.getContentLength());
				return_strings.add(2,"304");
				return_strings.add(3,con.getContentType().split("charset=")[1].split(";")[0]);
			}
			else {
				return null;
			}
		}
		//System.out.println("return strings are:"+return_strings.get(0)+" "+return_strings.get(1)+" "+return_strings.get(2));
		return return_strings;
	}
	public LinkedList<String> get_hrefs(String present_url,byte[] prev_content,long docid) throws Exception
	{
		/*
		 * get href links from GET or database.
		 * 
		 */
		String f_content="";
		Tidy tidy = new Tidy();
		tidy.setDocType("omit");
		tidy.setTidyMark(false);
		//System.out.println("tidy done");
		LinkedList<String> return_urls=new LinkedList<String>();
		byte[] body_content=prev_content;
		//Url_Crawl url_entity=new Url_Crawl();
		if(prev_content.length==0)
		{
			//not in db.so fetch by GET.
			//change characte encoding here
			f_content=getcontent(present_url);
			body_content=f_content.getBytes();//.getBytes(this.char_encoding);
			//TODO make this in client
			//long docID, String content, String charset, String filetype
			//make it go to s3.
			Worker.addindexToS3(docid,f_content,this.char_encoding,"html");
			
			//TODO url_entity.setContent(body_content);
			
			//put into s3
			
		}
		else
		{
			Url_Crawl u=(Url_Crawl)(Worker.dbase.getcrawledURLs().get(docid));
			return  u.get_links();
		}
		//System.out.println("body content is "+body_content);
		StringWriter writer = new StringWriter(body_content.length);
		Document d= tidy.parseDOM(new StringReader(new String(body_content,this.char_encoding)), writer);
		if(d==null)
			return null;
		NodeList a_nodes=d.getElementsByTagName("a");

		//System.out.println("262");
		//System.out.println("before the for loop"+present_url);
		URL home=new URL(present_url);
		String query=home.getQuery();
		LinkedList<String> for_db=new LinkedList<String>();
		for(int i=0;i<a_nodes.getLength();i++)
		{
			Node n=a_nodes.item(i);
			String each_href = n.getAttributes().getNamedItem("href").getNodeValue();
			URL final_href=new URL(home,each_href);
			for_db.add(final_href.toString());
			if(Worker.robot_info.containsKey(final_href.getHost()))
				Worker.queue.enqueue(final_href.toString());
			else
				return_urls.add(final_href.toString());
		}
		if(prev_content.length==0)
		{
			Url_Crawl url_entity=new Url_Crawl();
			url_entity.setURL(present_url);
			url_entity.setLastCrawledTime(System.currentTimeMillis() );
			url_entity.setContentType("html");
			//TODO checking with berkeley db.
			url_entity.setContent(f_content);
			url_entity.set_Links(return_urls);
			Worker.addRankToS3(docid, present_url, return_urls);
			Worker.dbase.getcrawledURLs().insert(docid,url_entity);
		}
		return return_urls;
		
	}

	/*public Document getDOM_xml(String xml_url,byte[] retrieved_content) throws UnknownHostException, IOException, InterruptedException, SAXException, ParserConfigurationException, TransformerException{
		Document dom_obj = null;
		
		String body_content;
		if(retrieved_content.length!=0)
		{
			DocumentBuilderFactory dbf=DocumentBuilderFactory.newInstance();
			DocumentBuilder dbuild=dbf.newDocumentBuilder();
			//builder.parse(new InputSource(new ByteArrayInputStream(xmlContents.getBytes())));
			
			//dom_obj=dbuild.parse(new InputSource(new ByteArrayInputStream(retrieved_content)));
			dom_obj=dbuild.parse(new InputSource(new StringReader(new String(retrieved_content,"GB2312"))));
			//System.out.println("307");
			return dom_obj;

			
		}
		else{
			body_content=getcontent(xml_url);
			Url_Crawl url_entity=new Url_Crawl();
			url_entity.setUrl(xml_url);
			url_entity.setContent(body_content.getBytes("GB2312"));
			url_entity.setDate( new Date() );
			url_entity.setContentType("xml");
			XPathCrawler.dbase.uda.pIdx.put(url_entity);
//			try 
//			{
//				
//				dom_obj = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new URL(xml_url).openStream());	
//				return dom_obj;
//				
//			} 
//			catch (SAXException e) {
//				e.printStackTrace();
//			} 
//			catch (ParserConfigurationException e) {
//				e.printStackTrace();
//			}
			
		}
		//return dom_obj;
		//return dom_obj;
	}*/
	public RobotsTxtInfo get_robots_content(String robots_path) throws IOException
	{
		/*
		 * get robots.txt
		 * 
		 */
		
		String robot_content = null;
		if (! this.pvalid)
			return null;
		if(protocol.equals("http")){
			
			URL u=new URL(full_url);
			//System.out.println("full url is "+u.toString());
			HttpsURLConnection con = (HttpsURLConnection)u.openConnection();
			con.setDoInput(true);
			con.setRequestMethod("GET");
			con.setRequestProperty("User-Agent","cis455crawler");

			con.setDoOutput(true);
			con.connect();

			BufferedReader inFromClient = new BufferedReader(new InputStreamReader (con.getInputStream()));

			
			String line=inFromClient.readLine();
			StringBuffer content=new StringBuffer();
			while(line!=null)
			{
				content.append(line+"\r\n");
				line=inFromClient.readLine();
			}

			robot_content= content.toString();
		}
		else if(protocol.equals("https"))
		{
			//System.out.println("getcontent https");
			System.out.println("url is:"+full_url);
			URL u=new URL(full_url);
			//System.out.println("full url is "+u.toString());
			HttpsURLConnection con = (HttpsURLConnection)u.openConnection();
			con.setDoInput(true);
			con.setRequestMethod("GET");
			con.setRequestProperty("User-Agent","cis455crawler");

			con.setDoOutput(true);
			con.connect();

			BufferedReader inFromClient = new BufferedReader(new InputStreamReader (con.getInputStream()));

			
			String line=inFromClient.readLine();
			StringBuffer content=new StringBuffer();
			while(line!=null)
			{
				content.append(line+"\r\n");
				line=inFromClient.readLine();
			}

			robot_content= content.toString();
		}
		//System.out.println(robot_content);
		String[] lines=robot_content.split("\n");
		String user_agent;
		ArrayList<String> disallow=new ArrayList<String>();
		ArrayList<String> allow=new ArrayList<String>();
		RobotsTxtInfo rb=new RobotsTxtInfo();
		//System.out.println("no of lines is:"+lines.length);
		for(int i=0;i<lines.length;i++)
		{
			//System.out.println("line is:"+lines[i]);
			if(lines[i].startsWith("User-agent"))
			{
				user_agent=lines[i].split(":")[1].trim();
				i++;
				//System.out.println("i:"+i);
				while(i<lines.length&&!lines[i].startsWith("User-agent")&&lines[i]!=null&&lines[i]!="\n")
				{
					
					//System.out.println("User-agent is "+user_agent);
					if(lines[i].startsWith("Disallow"))
					{
						String site_path=lines[i].split(":")[1].trim();
						if(site_path.startsWith("/~"))
						{
							int first_slash=site_path.indexOf("~")-1;
							int next_slash=site_path.indexOf("/", first_slash);
							String allow_part=site_path=site_path.replaceFirst("~","");
							String disallow_part=allow_part.substring(0, next_slash+1);
							rb.addAllowedLink(user_agent, allow_part);
							rb.addDisallowedLink(user_agent,disallow_part);
							continue;
						}
						//System.out.println(user_agent+" disallows "+lines[i].split(":")[1].trim());
						rb.addDisallowedLink(user_agent,lines[i].split(":")[1].trim());
					}
					else if(lines[i].startsWith("Allow"))
					{
						//System.out.println(user_agent+" allows "+lines[i].split(":")[1].trim());
						rb.addAllowedLink(user_agent,lines[i].split(":")[1].trim());
					}
					else if(lines[i].startsWith("Crawl-delay"))
					{
						rb.addCrawlDelay(user_agent,Integer.parseInt(lines[i].split(":")[1].trim()));
					}
					i++;
				}
				
				if(i>=lines.length)
					break;
				i--;
				
			}
//			try {
//				Worker.dbase.getrobotsDB().insert(this.hname,rb);
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			
		}
		return rb;
	}
	
}