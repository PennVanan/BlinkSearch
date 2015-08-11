package com.blink.crawler.MasterNode;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URL;

public class MasterClient {
	public void doPost(String url,String post_string) throws IOException 
	{
		/*
		 * get content with a GET request.
		 * 
		 */
			System.out.println("in masterclient:"+url);
			String only_url=url.substring(0,url.indexOf("?"));
			URL u=new URL(only_url);
			System.out.println("Masterclient:"+url+":"+u.getHost()+":"+u.getPort());
			Socket socket = new Socket(u.getHost(), u.getPort());

			PrintWriter out_obj = new PrintWriter(socket.getOutputStream(), true);
			out_obj.write("POST " + url + " HTTP/1.0\r\n");
			out_obj.write("Content-Type: application/x-www-form-urlencoded\r\n");
			out_obj.write("Content-Length: "+post_string.getBytes().length+"\r\n");
			out_obj.write("\r\n");
			out_obj.write(post_string);
			//out_obj.write("Connection: close\r\n\r\n");
			out_obj.flush();
			//socket.close();
			System.out.println("MasterClient done check Worker");

		
	}
}
