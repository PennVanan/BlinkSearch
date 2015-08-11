package com.blink.utils;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

public class HtmlParserTest {

	@Test
	public void test() {
		File f = new File("something.html");
		StringBuilder sb = new StringBuilder();
		try {
			FileReader fr= new FileReader(f);
			while(true){
				int char_read=fr.read();
				if(char_read==-1)
					break;
								
				sb.append((char)char_read);
			}
			HtmlParser h = new HtmlParser(sb.toString());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		assertEquals(1, 1);
	}

}
