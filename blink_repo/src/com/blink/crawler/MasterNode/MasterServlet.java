package com.blink.crawler.MasterNode;
import java.io.*;
import java.math.BigInteger;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.servlet.*;
import javax.servlet.http.*;
//TODO change here


public class MasterServlet extends HttpServlet {
	
	public ArrayList<String> worker_ipport=new ArrayList<String>();
	String post_reduce_string="";
	public int no_of_workers;
	static final long serialVersionUID = 455555001;
	public static HashMap<String,Status> worker_info=new HashMap<String,Status>();
	public static HashMap<String,Long> last_invoked_worker_status=new HashMap<String,Long>();
	//public static HashMap<String,Worker_Form> worker_form=new HashMap<String,Worker_Form>();
	public static boolean crawl_start=false;
	public static int id=0;
	public void doGet(HttpServletRequest request, HttpServletResponse response) 
			throws java.io.IOException
	{
		/*
		 * get worker status 
		 * display status
		 * take form data about worker
		 */

		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("<html><head><title>Master</title></head>");
		out.println("<body>Hi, I am SAGARIKA SEAS:RAYUDU</body></html>");
		String uri_pattern=request.getServletPath();
		System.out.println("uri_pattern"+uri_pattern);
		if(uri_pattern.endsWith("/workerstatus"))
		{
			/*
			 * receive status from workers and invoke reduce
			 */
			String qstring=request.getQueryString();
			String key=request.getRemoteAddr()+":"+qstring.split("port=")[1].split("&")[0];
			System.out.println(key);
			String status="active";
			System.out.println("master received workerstatus");
			
			//Status s=new Status(status,key,id++);
			
			if(!worker_ipport.contains(key))
			{
				Status s=new Status(status,key,id++);
				this.worker_ipport.add(key);
				worker_info.put(key,s);
			}
			
			//TODO if required
			MasterServlet.last_invoked_worker_status.put(key, System.currentTimeMillis());
			ArrayList<String> active_workers=new ArrayList<String>();
			for(String ip_port:worker_info.keySet())
			{
				if(last_invoked_worker_status.containsKey(ip_port)&&System.currentTimeMillis()-last_invoked_worker_status.get(ip_port)<=30000)
				{
					System.out.println();
					active_workers.add(ip_port);
				}
			}
			int no_active=active_workers.size();
			int count=0;
			//TODO crawl done work
			/*if(MasterServlet.crawl_done==false)
			{
				for(int j=0;j<no_active;j++)
				{
					if(worker_info.get(active_workers.get(0)).status.equals("waiting"))
					{
						count++;
					}
				}
				System.out.println("count is"+count );
				
				System.out.println(worker_info.keySet().size()+":size of active workers");
				if(count==no_active)
				{
					//crawling of 100000 done.
				}
				this.job_active=false;
			}*/

		}
		else if(uri_pattern.endsWith("/status"))
		{
			/*
			 * get a job
			 * 
			 */
			out.println("<table style=\"width:100%\">");
			out.println("<tr><td>"+"IP:Port"+"</td>"+"<td>"+"status"+"</td>"+"<td>"+"Job"+"</td>"+"<td>"+"keysRead"+"</td>"+"<td>"+"keysWritten"+"</td></tr>");
			for(String s:worker_info.keySet())
			{
				if(System.currentTimeMillis()-last_invoked_worker_status.get(s)<=30000)
				{
					out.println("<tr>");
					out.println("<td>"+s+"</td>"+"<td>"+worker_info.get(s).status+"</td>");
					out.println("</tr>");
				}
			}
			out.println("</table>");
			

				crawl_start=true;
				//FORM
				out.println("<form method=\"POST\">");
				
				out.println("Seed Urls:<br>");
				out.println("<input type=\"text\" name=\"seeds\"><br>");
				
				out.println("<input type=\"submit\" value=\"Submit\">");
				out.println("</form>");
				out.println("<form method=\"GET\" action=\"/shutdown\">");
				out.println("<input type=\"submit\" value=\"ShutDown\"></form>");

		}


	}
	public void doPost(HttpServletRequest request, HttpServletResponse response) 
			throws java.io.IOException
	{
		/*
		 * make post to /runmap with form data from doGet
		 */
		System.out.println("master post");

		this.crawl_start=true;
		String seeds = request.getParameter("seeds");
		System.out.println("seeds are:"+seeds);
		String[] seed_urls=seeds.split(";");
		
		//MasterServlet.worker_form.put(classname,new Worker_Form(classname,inputdir,outputdir,mapThreads,reduceThreads));
		HashMap<String,String> active_workers=new HashMap<String,String>();
		int i=0;
		StringBuffer post=new StringBuffer();
		////////////////////////////////////////
		/*for(String ip_port:last_invoked_worker_status.keySet())
		{
			if(System.currentTimeMillis()-last_invoked_worker_status.get(ip_port)<=30000)
			{
				
				active_workers.put("worker"+(i),ip_port);
				post.append("worker"+i+"="+ip_port+"&");
				i+=1;
			}
		}*/
		int i_s=0;
		for(String s:this.worker_ipport)
		{
			post.append("worker"+(i_s)+"="+worker_ipport.get(i_s++)+"&");
		}
		System.out.println("all workers are:"+post.toString());
		System.out.println("worker_ipport:"+worker_ipport);
		/////////////////////////////////////////////
		for(String s:seed_urls)
		{
			System.out.println("hashing s:"+s);
			//calculate hash of the url-->docid  and get call a /start_crawl on post request with post body
			//containing docid in url query string and a list of worker=ip port and url in postbody
			//call a post on /startcrawl
			byte[] s_bytes=new URL(s).getHost().getBytes();
			MessageDigest md = null;
			String hash_st = "";
			try 
			{
				md = MessageDigest.getInstance("SHA-1");
				byte[] b=md.digest(s_bytes);

				for (int h=0; h < b.length; h++) 
				{
					hash_st +=Integer.toString( ( b[h] & 0xff ) + 0x100, 16).substring( 1 );
				}
			}
			catch(NoSuchAlgorithmException e) 
			{
				e.printStackTrace();
			}
			BigInteger big_int=new BigInteger(hash_st,16);
			BigInteger big_int1=new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",16);
			BigInteger bucketsize=big_int1.divide(BigInteger.valueOf(worker_info.size()));
			BigInteger bucket_number=big_int.divide(bucketsize);
			int worker_number=bucket_number.intValue();
			System.out.println("hashing done:going to:"+worker_number);
			System.out.println("worker_ipport is:"+worker_ipport);
			String post_url="http://"+this.worker_ipport.get(worker_number)+"/Blink/worker/start_crawl?"+post.toString()+"nwork="+worker_info.size();
			System.out.println(post_url);
			
			MasterClient mc=new MasterClient();
			mc.doPost(post_url,"url="+s);
		}
	}
}
