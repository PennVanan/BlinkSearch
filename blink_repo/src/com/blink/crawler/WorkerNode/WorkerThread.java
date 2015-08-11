package com.blink.crawler.WorkerNode;
import java.io.IOException;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.Locale;

import com.blink.crawler.Storage.RobotsObj;
import com.blink.crawler.Storage.Url_Seen;
import com.blink.crawler.Storage.Url_Crawl;

public class WorkerThread extends Thread{

	public void run()
	{
		
		try 
		{
			    while(!CommonParameters.shutdown)
				{
					if(CommonParameters.shutdown)
						break;
					String url_dequeued="";
					if(Worker.queue.size()>0)
					{
						url_dequeued = Worker.queue.dequeue();
						System.out.println("url dequeued:"+url_dequeued);
					}
					else
					{
						//System.out.println("queue is empty");
						continue;
					}
					System.out.println(Thread.currentThread().getName()+"started handling request");
					this.process_url(url_dequeued);
					//put in commonParameters from command line arguments
					if(CommonParameters.max_crawl!=-1&&CommonParameters.count>CommonParameters.max_crawl)
					{
						System.out.println("Exiting because of maxcrawl count");
						break;
					}
				}//end while!shutdown
				System.out.println("out of while");
				//end synchronized
			System.out.println("out of run"+Thread.currentThread().getName());
		} //end outermost try
		catch (Exception e) 
		{
			e.printStackTrace();
		} 
	}
	public void process_url(String current) throws Exception
	{
		URL current_url=null;
		try {
			current_url=new URL(current);
		} catch (MalformedURLException e) {
			//System.println("malformed url:"+current);
			if(current.startsWith("http"))
				current_url=new URL("http://"+current);
			
		}
		String current_host=current_url.getHost();
		System.out.println("checking if robotstrfffffffff");
		RobotsObj r=(RobotsObj)Worker.dbase.getrobotsDB().get(current_host);
		if(r==null)
		{
			r=robotsGetInfo(current_url.toString());
			Worker.dbase.getrobotsDB().insert(current_host, r);
			synchronized(Worker.delay_map)
			{
				System.out.println("delay_map updating for robots");
				Worker.delay_map.put(current_host,System.currentTimeMillis());
			}
		}

		boolean b=true;
		
		try 
		{
				if(r==null)
				{
					
				}
				else
				{
					System.out.println("checking robot");
					b=check_robot(r,current);
				}
				
		} 
		catch (MalformedURLException e) 
		{
				//  Auto-generated catch block
				e.printStackTrace();
		}

		
		//decide whether to crawl or not
		if(!b)
		{
			System.out.println("robots ki wajah se not allowed");
			return;
		}
		//check in urlseen db.
		long docid=0;
		String hash_st = null;
		MessageDigest md = MessageDigest.getInstance("SHA-1");
		byte[] b_now=md.digest(current.getBytes());

		for (int h=0; h < b_now.length; h++) 
		{
			hash_st +=Integer.toString( ( b_now[h] & 0xff ) + 0x100, 16).substring( 1 );
		}


		BigInteger big_int=new BigInteger(hash_st,16);
		docid=big_int.longValue();
		System.out.println("docid:"+docid);
		Url_Crawl u_check=(Url_Crawl)Worker.dbase.getseenURLsDB().get(docid);
		if(u_check != null)
		{
			System.out.println("url seen again hit and return");
			u_check.hit();
			Worker.dbase.getcrawledURLs().insert(docid, u_check);
			return;
		}

		LinkedList<Object> req_headers=null;
		SagaClient client = new SagaClient(current);
		//replace with HTTP client own
		
		Url_Crawl u=(Url_Crawl)Worker.dbase.getcrawledURLs().get(big_int.longValue());
		long ifmod=0;
		if(u!=null)
		{
			System.out.println("checking last modified time");
			ifmod=u.getLastCrawledTime();
		}

		try {
			//delay_map.put(current_host,System.currentTimeMillis());
			long delay_host=check_delay(current_host,System.currentTimeMillis());
			//Thread.sleep(delay_host);
			//TODO put it behind queue.
			if(delay_host>0)
			{
				System.out.println("delay not satisfied so put back in queue");
				Worker.queue.enqueue(current);
				return;
			}
			System.out.println("calling head");
			req_headers=client.doHEAD(ifmod);
			
			synchronized(Worker.delay_map)
			{
				System.out.println("calling head after delay");
				Worker.delay_map.put(current_host,System.currentTimeMillis());
			}
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		if(req_headers==null||req_headers.isEmpty())
		{
			System.out.println("adding :"+current+" to urlsen");
			Worker.dbase.getseenURLsDB().insert(docid, new Url_Seen(current));
			return;
		}
		boolean html=false;
		boolean xml=false;
		String content_type=(String)req_headers.get(0);
		int content_length=Integer.parseInt((String)req_headers.get(1));
		int response_code=Integer.parseInt((String)req_headers.get(2));
		String char_encoding=(String)req_headers.get(3);
		
		
		if(response_code/100==3)
		{
			synchronized(Worker.delay_map)
			{
				Worker.delay_map.put(current_host,System.currentTimeMillis());
			}
		}
		if(response_code!=200&&response_code/100!=3)
		{
			System.out.println("weird response so return but ad din last url seen");
			Worker.dbase.getseenURLsDB().insert(docid, new Url_Seen(current));
			return;
		}
		
		//put clength in CommonParameters 
		if(content_length>CommonParameters.clength)
		{
			//System.out.println("131");
			System.out.println("content length :exceeded");
			return;
		}
		if(response_code==304)
		{
			if(content_type.endsWith("html")||content_type.endsWith("htm")||content_type.contains("html"))
			{
				html=true;
			}
			else 
			{
				//handle other datatypes
				
				//System.out.println(u.getContentType());
				xml=true;
			}

		}
		if((response_code!=304)&&(content_type!=null||!content_type.isEmpty()))
		{
			//decide
			//get href
			System.out.println("not 304 and contetnt type not null and not empty");
			if(content_type.endsWith("html")||content_type.endsWith("htm")||content_type.contains("html"))
			{
				System.out.println("html file");
				html=true;
				//System.out.println("155");
			}
			//TODO handle pdf
		}
		boolean b_processed=false;
		if(html)
		{
			
			try 
			{
				if(response_code==304)
				{
					b_processed=handle_html(current,current_host,false,docid);
				}
				else
				{
					b_processed=handle_html(current,current_host,true,docid);
				}
			} catch (IOException e) {
			
				e.printStackTrace();
			}
			if(b_processed)
			{
				CommonParameters.count++;
				
				Worker.dbase.getseenURLsDB().insert(docid, new Url_Seen(current));
				
			}
			
		}
		
	}

	boolean handle_html(String url_html,String current_host,boolean retrieve,long docid) throws Exception
	{
		/*
		 * handle html file.check if in database .other with GET extract hrefs
		 * 
		 */
		System.out.println("handling html");
		SagaClient client=new SagaClient(url_html);
		//get href links
		if(retrieve)
		{
			System.out.println("retrieving from get");
			long d=check_delay(current_host,System.currentTimeMillis());
			if(d>0)
			{
				//Worker.putTask(url_html);
				
				Worker.queue.enqueue(url_html);
				return false;
			}
			LinkedList<String> hrefs=client.get_hrefs(url_html,"".getBytes(),docid);
			Worker.sendToWorker(hrefs);
			synchronized(Worker.delay_map)
			{
				Worker.delay_map.put(current_host,System.currentTimeMillis());
			}
			Url_Crawl u=(Url_Crawl)Worker.dbase.getcrawledURLs().get(docid);
			u.set_Links(hrefs);
			Worker.dbase.getcrawledURLs().insert(docid,u);
			System.out.println("sending hrefs to worker");
			Worker.sendToWorker(hrefs);
			System.out.println(url_html+": Downloading");
			
			//TODO add to master instead of worker
		
		  //synchronized(Worker.requests)
	//		{
				//Worker.requests.addAll(hrefs);
			//}
			return true;
		
		}
		else
		{
			client=new SagaClient(url_html);
			Url_Crawl u=(Url_Crawl)(Worker.dbase.getcrawledURLs().get(docid));
			LinkedList<String> hrefs=client.get_hrefs(url_html,u.getContent().getBytes(),docid);
			//TODO
			//add hrefs to queue on disk
			//url_frontier.addAll(hrefs);
			LinkedList<String> send=new LinkedList<String>();
			System.out.println("sending to worker");
			Worker.sendToWorker(send);
			
			System.out.println(url_html+":Not Modified");
			return true;
		}
		//System.out.println("hrefs are:\n"+hrefs);


	}
	long check_delay(String host_name,long now) throws Exception
	{
		/*
		 * check if crawl delay has been satisfied
		 * 
		 *  return milliseconds to wait
		 */
		//System.out.println("inside delay");
		long hostname_delay=0;
		RobotsObj r=(RobotsObj)Worker.dbase.getrobotsDB().get(host_name);
		//if(Worker.robot_info.containsKey(host_name))
		if(r!=null)
		{
			System.out.println("contains host");
				hostname_delay=r.getCrawlDelay()*1000;
			
		}
		else 
		{
			return 0;
		}
		if(hostname_delay==0)
			return 0;
		if(Worker.delay_map.containsKey(host_name))
		{
			//System.out.println("host contains in delaymap");
			long last_crawled=Worker.delay_map.get(host_name);
			if(last_crawled+hostname_delay>=now)
			{
				System.out.println("delay is"+(now-(last_crawled+hostname_delay)));
				return (last_crawled+hostname_delay)-now;
				
			}
			else 
			{
				System.out.println("returning 0 as there is no delay");
				return 0;
			}
		}
		else
		{
			return 0;
		}
	}
	boolean check_robot(RobotsObj robot,String url_robot_check) throws MalformedURLException
	{
		/*
		 * check if a particular linkcan be crawled or not from parameters in robots.txt
		 * 
		 */
		//TODO robots part check here
		ArrayList<String> disallow=robot.getDisallowedLinks();
		ArrayList<String> allow=robot.getAllowedLinks();
		boolean is=false;
		URL check_url=new URL(url_robot_check);
		String check=check_url.getPath();
		String dis_path="";
		boolean dis=false;
		boolean presence_dis=false;
		if(disallow!=null){
			for(String s:disallow)
			{
				int l=s.length();
				if(s.startsWith("/*"))
				{
					if(s.endsWith("$"))
					{
						String dont_end=s.replaceFirst("/*", "").substring(0, l-3);
						if(check.endsWith(dont_end))
						{
							dis=true;
							dis_path=dont_end;
							break;
						}
					}
					else
					{
						String dont=s.replaceFirst("/*","");
						if(check.contains(dont))
						{
							dis=true;
							dis_path=dont;
							break;
						}
					}
				}
				else if(check.startsWith(s))
				{
					dis=true;
					dis_path=s;
					presence_dis=true;
					break;
				}

			}
		}
		
		boolean presence_al=false;
		boolean al=false;
		String al_path="";
		if(allow!=null){
			for(String s:allow)
			{
				if(check.startsWith(s))
				{
					al=true;
					al_path=s;
					presence_al=true;
					break;
				}

			}
		}
		if(presence_al&&presence_dis)
		{
			if(dis&&al)
			{
				if(dis_path.length()<=al_path.length())
				{
					return true;
				}
				else
				{
					return false;
				}
			}
		}
		else if(presence_al==false && presence_dis==false)
		{
			return true;
		}
		else if(presence_al)
		{
			return true;
		}
		else
		{
			return false;
		}
		return true;
	}

	public RobotsObj robotsGetInfo(String robots_url)
	{
		/*
		 * parse info from robots.txt
		 * 
		 */
		RobotsTxtInfo r=null;
		URL u;
		try 
		{
			u = new URL(robots_url);
		} 
		catch (MalformedURLException e) 
		{
			return null;
		}
		String robots_path=u.getProtocol()+"://"+u.getHost()+"/robots.txt";
		SagaClient sc=new SagaClient(robots_path);
		
		try 
		{
			r=(RobotsTxtInfo)sc.get_robots_content(robots_path);
			Worker.delay_map.put(u.getHost(), System.currentTimeMillis());
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ArrayList<String> disallowed=r.getDisallowedLinks("cis455crawler")==null?r.getDisallowedLinks("*"):r.getDisallowedLinks("cis455crawler");
		ArrayList<String> allowed=r.getAllowedLinks("cis455crawler")==null?r.getAllowedLinks("*"):r.getAllowedLinks("cis455crawler");
		Integer crawl_delay=r.getCrawlDelay("cis455crawler")==0?r.getCrawlDelay("*"):r.getCrawlDelay("cis455crawler");
		RobotsObj robot=new RobotsObj(disallowed,allowed,crawl_delay);
		return robot;
	}
	

}


