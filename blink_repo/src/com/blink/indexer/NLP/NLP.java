package com.blink.indexer.NLP;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

public class NLP {

	public HashMap<String, ArrayList<Double>> tokenize(String data) {

		HashMap<String, ArrayList<Double>> tbr = new HashMap<String, ArrayList<Double>>();
		ArrayList<Double> locations;

		TokenizerModel model;
		try {
			model = new TokenizerModel(new File("openNLP_models/en-token.bin"));
			Tokenizer tokenizer = new TokenizerME(model);
			String[] all_tokens=tokenizer.tokenize(data);

			double word_offset=1;

			//for each token
			for(String key : all_tokens) {				
				if(!key.matches(",|.|(|)| |#|@|!|%|$|^|-|_|;|:|'|\"|/|<|>|`|~")){	//removing trash
					//some extra optimization
					if(key.endsWith(".") || key.endsWith("\""))
						key = key.substring(0, key.length()-1);
					if(key.startsWith("\""))
						key = key.substring(1, key.length());
					
					//converting to lower case
					key = key.toLowerCase();

					if(tbr.containsKey(key)){
						locations = tbr.get(key);
						locations.add(word_offset);
						tbr.put(key, locations);
					}
					else{
						locations = new ArrayList<Double>();
						locations.add(word_offset);
						tbr.put(key, locations);
					}

					//moving on to next word
					word_offset++;
				}
			}

		} catch (IOException e) {
			System.out.println("Error tokenizing " + e.toString());
		}		

		return tbr;
	}
}
