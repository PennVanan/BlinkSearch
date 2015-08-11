package com.blink.webUI;


import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.imageio.ImageIO;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;



/**
 * Servlet implementation class BlinkSearchEngine
 */
public class BlinkSearchEngine extends HttpServlet {
	private static final long serialVersionUID = 1L;
	public static ArrayList<String> stopWords;
	private DynamoDB dynamoDB;


	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public BlinkSearchEngine() {
		super();
	}

	/**
	 * Initializes stopWord array with stop words stored in a text file and AWS DynamoDB access.
	 */
	public void init() {
		File stopWordFile = new File("/opt/bitnami/apache-tomcat/webapps/Blink/static/stopwords_en.txt");
		try {
			FileReader fr = new FileReader(stopWordFile);
			BufferedReader reader = new BufferedReader(fr);
			stopWords = new ArrayList<String>();
			String word = "";
			while ((word = reader.readLine()) != null) {
				stopWords.add(word);
			}
			reader.close();
		} catch (Exception e) {
			System.out.println("Error generating stopwords");
		}

		//		AWSCredentials credentials = null;
		//        try {
		//            credentials = new ProfileCredentialsProvider("default").getCredentials();
		//        } catch (Exception e) {
		//            throw new AmazonClientException(
		//                    "Cannot load the credentials from the credential profiles file. " +
		//                    "Please make sure that your credentials file is at the correct " +
		//                    "location (/home/cis455/.aws/credentials), and is in valid format.",
		//                    e);
		//        }

		AmazonDynamoDBClient client = new AmazonDynamoDBClient(new InstanceProfileCredentialsProvider());
		dynamoDB = new DynamoDB(client);
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String requestUri = request.getRequestURI();
		PrintWriter out;
		switch(requestUri) {
		case "/Blink/index.html":
			out = response.getWriter();
			response.setContentType("text/html");
			File indexPage = new File("/opt/bitnami/apache-tomcat/webapps/Blink/WEB-INF/index.html");
			if (indexPage.exists()) {
				FileReader fr = new FileReader(indexPage);
				BufferedReader reader = new BufferedReader(fr);
				String line = "";
				while ((line = reader.readLine()) != null) {
					out.println(line);
				}
				reader.close();
			} else {
				out.println("Could not find index.html");
			}
			out.close();
			break;
		case "/Blink/BlinkBG.jpg":
			response.setContentType("image/jpeg");
			File f = new File("/opt/bitnami/apache-tomcat/webapps/Blink/static/BlinkBG.jpg");
			BufferedImage bi = ImageIO.read(f);
			OutputStream os = response.getOutputStream();
			ImageIO.write(bi, "jpg", os);
			os.close();
			break;
		default:
			// Any other url will just redirect client to the main index.html.
			response.sendRedirect(response.encodeRedirectURL("http://ec2-52-6-7-99.compute-1.amazonaws.com/Blink/index.html"));
			break;
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String query = request.getParameter("query");
		String lcQuery = query.toLowerCase();
		String parsedQuery = removeStopWordsInQuery(lcQuery);
		JSONObject yelpResults = getYelpResults(request, parsedQuery);
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();

		// Scenario 1: New user, new query
		HttpSession session = request.getSession();
		if (session.getAttribute("query") == null || 
				(session.getAttribute("query").equals(query) && request.getParameter("next") == null)) {
			System.out.println("Scenario 1 occurred");
			session.setAttribute("query", query);
			session.setAttribute("offset", 0);
			MatchingDocument[] results = null;
			if (lcQuery.startsWith("\"")) { // Is this a valid phrase query?
				String splitQuery[] = lcQuery.split(" ");
				if (splitQuery.length > 1) {
					results = getResultsForPhraseQuery(lcQuery);
				} else {
					String lcQueryWithoutQuotes = lcQuery.replaceAll("\"", "");
					results = getSearchResults(lcQueryWithoutQuotes);
				}
			} else {
				results = getSearchResults(parsedQuery);
			}

			if (results == null) {
				printResultsPage(out, query, 0, null, yelpResults);
			} else {
				if (results.length > 10) {
					cacheResults(query, results); // Cache query results in DynamoDB
				}
				// Create array of results to display
				String [] queryResults;
				if (results.length > 10) {
					queryResults = new String[10];
					for (int i = 0; i < 10; i++) {
						queryResults[i] = results[i].getUrl();
					}
				} else {
					queryResults = new String[results.length];
					for (int i = 0; i < results.length; i++) {
						queryResults[i] = results[i].getUrl();
					}
				}
				printResultsPage(out, query, 0, queryResults, yelpResults);
			}
		} else {
			// Scenario 2: Returning user, requesting prev/next results
			if (session.getAttribute("query").equals(query)) {
				System.out.println("Scenario 2 occurred");
				boolean next = true; // Did user request prev or next results?
				if (request.getParameter("next").equals("false")) {
					next = false;
				}
				int offset = (Integer)session.getAttribute("offset");
				if (next) {
					offset = offset + 10;
					session.setAttribute(query, offset);
				} else {
					offset = offset - 10;
					session.setAttribute(query, offset);
				}
				String[] queryResults = getCachedResults(parsedQuery, offset);
				printResultsPage(out, query, offset, queryResults, yelpResults);
			} else { 
				// Scenario 3: Returning user, new query
				System.out.println("Scenario 3 occurred");
				String oldQuery = (String)session.getAttribute("query");
				deleteCachedResult(oldQuery);
				session.setAttribute("query", query);
				session.setAttribute("offset", 0);

				MatchingDocument[] results = null;
				if (lcQuery.startsWith("\"")) { // Is this a valid phrase query?
					String splitQuery[] = lcQuery.split(" ");
					if (splitQuery.length > 1) {
						results = getResultsForPhraseQuery(lcQuery);
					} else {
						String lcQueryWithoutQuotes = lcQuery.replaceAll("\"", "");
						results = getSearchResults(lcQueryWithoutQuotes);
					}
				} else {
					results = getSearchResults(parsedQuery);
				}

				if (results == null) {
					printResultsPage(out, query, 0, null, yelpResults);
				} else {
					if (results.length > 10) {
						cacheResults(query, results); // Cache query results in DynamoDB
					}
					// Create array of results to display
					String [] queryResults;
					if (results.length > 10) {
						queryResults = new String[10];
						for (int i = 0; i < 10; i++) {
							queryResults[i] = results[i].getUrl();
						}
					} else {
						queryResults = new String[results.length];
						for (int i = 0; i < results.length; i++) {
							queryResults[i] = results[i].getUrl();
						}
					}
					printResultsPage(out, query, 0, queryResults, yelpResults);
				}
			}
		}
	}

	/**
	 * Gets IP location and returns a Document from the XML response.
	 * @param ip The requester's IP address
	 * @return a Document with all location information (city, zip, coordinates, etc.)
	 * @throws Exception
	 */
	protected Document getIPLocation(String ip) throws Exception { 
		URL url = new URL("http://freegeoip.net/xml/" + ip);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("User-Agent", "cis455");

		int responseCode = conn.getResponseCode();

		if (responseCode == 200) {
			BufferedReader in = new BufferedReader(
					new InputStreamReader(conn.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
			String responseString = response.toString();
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();  
			DocumentBuilder builder;  
			try 
			{  
				builder = factory.newDocumentBuilder();  
				Document doc = builder.parse(new InputSource(new StringReader(responseString))); 
				return doc;
			} catch (Exception e) {  
				e.printStackTrace();  
			} 
		}
		System.out.println("Location was unable to be retrieved.");
		return null;
	}

	/**
	 * Returns JSON from freegeoip.net which contains location-based information
	 * (eg. city, zip code, long/lat coordinates, etc.) associated with an IP address.
	 * This method is used to provide location-based search results.
	 * @param ip - The IP address of the requester.
	 * @return JSONObject with location-based information or null if bad connection or
	 * non-200 response is received.
	 * @throws Exception
	 */
	protected JSONObject getIPLocationInJson(String ip) throws Exception { 
		URL url = new URL("http://freegeoip.net/json/" + ip);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("User-Agent", "cis455");

		int responseCode = conn.getResponseCode();

		if (responseCode == 200) {
			BufferedReader in = new BufferedReader(
					new InputStreamReader(conn.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
			String responseString = response.toString();
			JSONObject json = new JSONObject(responseString);
			return json;
		}
		return null;
	}

	/**
	 * Retrieves results from Yelp based on location and query.
	 * @param query The requester's query.
	 * @param zipcode The zip code associated with the requester's IP address.
	 * @return a JSONObject that holds the Yelp Results.
	 */
	protected JSONObject getYelpResults(String query, String zipcode) {
		Yelp yelp = new Yelp();
		String results = yelp.searchForBusinessesByLocation(query, zipcode);
		if (results == null) return null;
		return new JSONObject(results);
	}

	/**
	 * Retrieves the requester's zipcode via IP Address lookup and then gets yelp results
	 * for the query and location.
	 * @param request Needed to retrieve the IP Address location.
	 * @param query The requester's search query
	 * @return a JSONObject with the Yelp results.
	 */
	protected JSONObject getYelpResults(HttpServletRequest request, String query) {
		JSONObject ipLoc = null;
		try {
			ipLoc = getIPLocationInJson(request.getRemoteAddr());
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (ipLoc != null && ipLoc.getString("zip_code") != null) {
			JSONObject yelpResults = getYelpResults(query, ipLoc.getString("zip_code"));
			return yelpResults;
		}
		return null;
	}

	/**
	 * Prints the results of the search query
	 * @param out The writer to the client socket.
	 * @param query The user's query
	 * @param offset 
	 * @param results A string of URLS as results.
	 * @param yelpResults A parseable JSON object of Yelp Results, including business name, rating, image, and snippets.
	 */
	protected void printResultsPage(PrintWriter out, String query, int offset, String[] results, JSONObject yelpResults) {
		out.println("<html><body><div id=\"header\" style=\"background:black;color:white;font-size:20px;padding:25px\">");
		out.println("Blink Results For " + query);
		out.println("<form action=\"/Blink/\" method=\"POST\"><input type=\"text\" name=\"query\" placeholder=\"Search Again\" size=\"45pt\" style=\"height:25px;font-size:16pt\">");
		out.println("<input type=\"submit\" style=\"float:right;font-size:15px\" value=\"Search\"></form>");
		out.println("</div>");
		out.println("<div id=\"section\" style=\"width:700px;height:100%;background-color:#66CCFF;float:left\"><table>");
		//Our search results go here:
		if (results == null) { // There were no results...
			out.println("<h3> Sorry, no results found.</h3>");
		} else {
			for (int i = 0; i < results.length; i++) {
				out.println("<tr><td>" + (i+1) + ".</td><td><a href=\"" + results[i] + "\"><h3>" + results[i] + "</h3></a></td></tr>");
			}
			out.println("</table>");
			if (offset != 0) {
				out.println("<form action=\"/Blink/\" method=\"POST\"><input type=\"hidden\" name=\"query\" value=\"" + query + "\">");
				out.println("<input type=\"hidden\" name=\"next\" value=\"false\">"); //Previous button
				out.println("<input type=\"submit\" value=\"Previous\">");
			}
			if (results.length == 10) {
				out.println("<form action=\"/Blink/\" method=\"POST\"><input type=\"hidden\" name=\"query\" value=\"" + query + "\">");
				out.println("<input type=\"hidden\" name=\"next\" value=\"true\">"); //Next button
				out.println("<input type=\"submit\" value=\"Next\">");
			}
		}
		out.println("</div><div id=\"aside\" style=\"height:100%\"><h2>Search Results From <img src='https://s3-media3.fl.yelpassets.com/assets/2/www/img/2d7ab232224f/developers/yelp_logo_100x50.png'><table>");
		//Yelp results inserted here
		if (yelpResults != null) {
			JSONArray businesses = (JSONArray) yelpResults.get("businesses");
			if (businesses != null) {
				for (int i = 0; i < businesses.length(); i++) {
					JSONObject business = businesses.getJSONObject(i);
					out.println("<tr><td><a href='" + business.get("url") + "'>" + business.get("name") + "</a></td></tr>");
					out.println("<tr><td><img src='" + business.get("rating_img_url_small") + "'> based on " + business.get("review_count") + " reviews</td></tr>");
					out.println("<tr><td><img src='" + business.get("image_url") + "'></td><tr>");
					out.println("<tr><td>" + business.get("snippet_text") + "</td></tr><tr><br/></tr>");
				}
			}
		}
		out.println("</table></div></body></html>");
	}

	/**
	 * Removes stopword in a query. If all words in the query are stopwords,
	 * the original query is returned.
	 * @param query The user's search query.
	 * @return The query without stopwords.
	 */
	protected String removeStopWordsInQuery(String query) {
		String query2 = query.toLowerCase();
		String[] splitQueryIntoWords = query2.split(" "); // split on spaces
		String updatedQuery = "";
		for (String queryWord : splitQueryIntoWords) {
			if (!stopWords.contains(queryWord)) {
				updatedQuery += queryWord + " ";
			}
		}
		// Handles edge case where all words are stopwords
		if (updatedQuery.isEmpty()) {
			return query;
		}
		return updatedQuery.trim();
	}

	/**
	 * Helper method to call all methods needed to get our search results.
	 */
	protected MatchingDocument[] getSearchResults(String parsedQuery) {
		Map<String,MatchingDocument> matches = getMatchingDocuments(parsedQuery);
		if (matches == null) {
			return null;
		}
		MatchingDocument[] results = getSortedResults(matches);
		return results;
	}

	/**
	 * For each word in the search query, return the documents which contain those words
	 * and their information.
	 * @param parsedQuery The user query without stop words
	 * @return A map from docID to a MatchingDocument object that contains key information
	 * such as tfidf, word position, pagerank, and url.
	 */
	protected Map<String, MatchingDocument> getMatchingDocuments(String parsedQuery) {
		String [] queryWords = parsedQuery.split(" ");

		// Get item from inverted index table for each word in query. Map it to each query word.
		Map<String,Item> queryWordsToItems = new HashMap<String,Item>();
		for (String word : queryWords) {
			Item item = getItemFromIndexer(word); //makes a call to DynamoDB
			if (item != null) {
				queryWordsToItems.put(word, item);
			}
		}
		// If no results, queryWordsToItems will be empty. Return null.
		if (queryWordsToItems.isEmpty()) return null;

		// For each word/item pair in queryWordsToItems, create a MatchingDocument object to store
		// word matched, word position, tfidf, pagerank, and url. The latter 2 come from
		// crawlerTable.
		Map<String,MatchingDocument> matches = new HashMap<String,MatchingDocument>();
		Iterator<String> queryWordIter = queryWordsToItems.keySet().iterator();
		while (queryWordIter.hasNext()) {
			String queryWord = queryWordIter.next();
			Item item = queryWordsToItems.get(queryWord);
			Map<String,List<BigDecimal>> posting_list = item.getMap("posting_list");
			Iterator<String> postingIter = posting_list.keySet().iterator();
			while (postingIter.hasNext()) {
				String docID = postingIter.next();
				// Get word positions and tfidf score from indexer table.
				List<BigDecimal> positions = posting_list.get(docID);
				double tfidfScore = positions.get(0).doubleValue();
				positions.remove(0); // removes TFIDF score from positions list.
				ArrayList<Integer> positionsInInt = convertBigDecimalToInteger(positions);

				// If docID is a key in matches map, add new values, else create a new MatchingDocument
				if (matches.containsKey(docID)) {
					MatchingDocument updateMd = matches.get(docID);
					updateMd.addWordAndPositions(queryWord, positionsInInt);
					updateMd.addTfidfScore(tfidfScore);
				} else {
					// Get url and pagerank from crawler table.
					Item crawledDoc = getItemFromCrawlerTable(docID);
					String url = crawledDoc.getString("url");
					Double pagerank = crawledDoc.getNumber("pagerank").doubleValue();

					MatchingDocument newMd = new MatchingDocument(docID);
					newMd.addWordAndPositions(queryWord, positionsInInt);
					newMd.addTfidfScore(tfidfScore);
					newMd.setUrl(url);
					newMd.setPageRankScore(pagerank);
					matches.put(docID, newMd);
				}
			}
		}
		return matches;
	}

	/**
	 * Sorts all matching documents by calculating their final score and putting them in an array
	 * by descending order of relevance.
	 * @param matches All documents that matched one or more query words.
	 * @return An array of matchingdocuments in order of relevance.
	 */
	protected MatchingDocument[] getSortedResults(Map<String,MatchingDocument> matches) {
		// Calculate final scores for all MatchingDocument in matches
		HashMap<Double, MatchingDocument> results = new HashMap<Double,MatchingDocument>();
		for (String docID : matches.keySet()) {
			MatchingDocument md = matches.get(docID);
			double tfidf = md.getTfidfScore();
			double pagerank = md.getPageRankScore();
			double finalScore = (0.25 * tfidf) + (0.75 * pagerank);
			md.setFinalScore(finalScore);
			results.put(finalScore, md);
		}

		//Sort by key (in descending order by finalScore)
		Map<Double,MatchingDocument> sortedResults = new TreeMap<Double,MatchingDocument>(Collections.reverseOrder());
		sortedResults.putAll(results);

		//Put matchingdocs in array in order of relevance
		MatchingDocument[] finalResults = new MatchingDocument[sortedResults.keySet().size()];
		Iterator<Double> iter = sortedResults.keySet().iterator();
		for (int i = 0; i < finalResults.length; i++) {
			double finalScoreKey = iter.next();
			finalResults[i] = sortedResults.get(finalScoreKey);
		}
		return finalResults;
	}



	/**
	 * Converts a list of BigDecimal to a list of Integer.
	 * @param positions the list of BigDecimal to convert.
	 * @return An arraylist of integers.
	 */
	protected ArrayList<Integer> convertBigDecimalToInteger(List<BigDecimal> positions) {
		ArrayList<Integer> positionsInInt = new ArrayList<Integer>();
		for (BigDecimal position : positions) {
			positionsInInt.add(position.intValueExact());
		}
		return positionsInInt;
	}

	/**
	 * Saves users' searches in a DynamoDB table - used as a cache with
	 * the key being the query and the value being a list of URLs.
	 */
	protected void cacheResults(String query, MatchingDocument[] results) {
		String tableName = "SearchCache";
		Table table = dynamoDB.getTable(tableName);
		ArrayList<String> urls = new ArrayList<String>();
		for (int i = 0; i < results.length; i++) {
			urls.add(results[i].getUrl());
		}

		Item item = new Item()                
		.withPrimaryKey("query", query)                	     
		.withList("urls", urls);
		table.putItem(item);
	}

	/**
	 * Retrieves a list of URLS that were cached for a specific query.
	 * Only retrieves the next or previous 10 from the offset.
	 */
	protected String[] getCachedResults(String query, int offset) {
		ArrayList<String> cachedResults = new ArrayList<String>();

		Table table = dynamoDB.getTable("SearchCache");
		Item item = table.getItem("query", query);
		List<Object> urls = item.getList("urls");
		int resultSize = urls.size();

		if (offset >= resultSize) {
			return null; // We've reached the end of results.
		} else if (offset < resultSize - 10) { // There are at least 10 more results to display
			for (int i = offset; i < offset + 10; i++) {
				cachedResults.add((String)urls.get(i));
			}
		} else { //there are less than 10 remaining results to display
			for (int i = offset; i < resultSize; i++) {
				cachedResults.add((String)urls.get(i));
			}
		}
		String[] results = (String[]) cachedResults.toArray();
		return results;
	}

	/**
	 * Deletes a cached result when a returning user requests a new search query result.
	 * @param query The key of the cached result to delete.
	 */
	protected void deleteCachedResult(String query) {
		Table table = dynamoDB.getTable("SearchCache");
		try {
			DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
			.withPrimaryKey("query", query);
			DeleteItemOutcome outcome = table.deleteItem(deleteItemSpec);
			if (outcome.getItem() != null) {
				return;
			}
		} catch (Exception e) {
			System.out.println("Error deleting item in SearchCache");
			System.out.println(e.getMessage());
		}
	}

	/**
	 * This method exclusively handles support for phrase queries.
	 */
	protected MatchingDocument[] getResultsForPhraseQuery(String query) {
		// Remove quotes from query and split query into individual words
		String queryWithoutQuotes = query.replaceAll("\"", "");
		String[] splitQuery = queryWithoutQuotes.split(" ");

		Map<String, MatchingDocument> possibleMatches = new HashMap<String, MatchingDocument>();

		// This block of code gets the initial documents with the first word in the phrase.
		String queryWord = splitQuery[0];
		Item docs = getItemFromIndexer(queryWord);
		if (docs == null) {
			System.out.println("Line 614: No documents matched first word in phrase query");
			return null; // If no docs contain that word, query cannot be matched.
		}
		Map<String,List<BigDecimal>> posting_list = docs.getMap("posting_list");
		Iterator<String> postingIter = posting_list.keySet().iterator();
		while (postingIter.hasNext()) { // While there are more documents that contain this word...
			String docID = postingIter.next();
			MatchingDocument md = createMatchingDocumentFromPostingList(posting_list, docID, queryWord);
			possibleMatches.put(docID, md);
		}

		// This block of code adds the position of the rest of the words in the phrase
		// to the MatchingDocument objects that also had the first word in the phrase.
		for (int i = 1; i < splitQuery.length; i++) {
			String queryWord2 = splitQuery[i];
			Item docs2 = getItemFromIndexer(queryWord2);
			if (docs2 == null) return null;
			Map<String,List<BigDecimal>> posting_list2 = docs2.getMap("posting_list");
			Iterator<String> postingIter2 = posting_list2.keySet().iterator();
			while (postingIter2.hasNext()) {
				String docID = postingIter2.next();
				if (possibleMatches.containsKey(docID)) {
					MatchingDocument md = possibleMatches.get(docID);
					// update this matchingdocument's word/position map.
					List<BigDecimal> positions = posting_list2.get(docID);
					positions.remove(0);
					ArrayList<Integer> positionsInInt = convertBigDecimalToInteger(positions);
					md.addWordAndPositions(queryWord2, positionsInInt);
				}
			}
		}
		
		System.out.println("possibleMatches has " + possibleMatches.keySet().size() + " documents");

		// This block of code gets all the documents with all the words in the query.
		ArrayList<MatchingDocument> docsWithAllWords = new ArrayList<MatchingDocument>();
		for (String docId : possibleMatches.keySet()) {
			MatchingDocument md = possibleMatches.get(docId);
			if (md.getWordPositions().size() == splitQuery.length) {
				docsWithAllWords.add(md);
			}
		}

		if (docsWithAllWords.isEmpty()) {
			System.out.println("Line 657: docsWithAllWords was empty");
			return null;
		}
		Map<String,MatchingDocument> docsWithWordsInOrder = getDocumentsWithQueryWordsInOrder(docsWithAllWords, splitQuery);
		if (docsWithWordsInOrder == null) {
			System.out.println("Line 662: docsWithWordsInOrder was empty");
			return null; // means no documents had the words in the correct order.
		}

		if (docsWithWordsInOrder.keySet().size() > 1) {
			MatchingDocument[] sortedResults = getSortedResults(docsWithWordsInOrder);
			return sortedResults;
		}
		MatchingDocument[] sortedResults = new MatchingDocument[1];
		for (String docID : docsWithWordsInOrder.keySet()) {	
			sortedResults[0] = docsWithWordsInOrder.get(docID);
		}
		return sortedResults;
	}

	/**
	 * For each document that contained all words in the phrase query, determine if the position
	 * of those words are in the same order as the query. If so, add to result map.
	 * @param docsWithAllWords
	 * @return
	 */
	protected Map<String,MatchingDocument> getDocumentsWithQueryWordsInOrder
	(ArrayList<MatchingDocument> docsWithAllWords, String[] splitQuery) {

		Map<String, MatchingDocument> results = new HashMap<String,MatchingDocument>();
		for (MatchingDocument doc : docsWithAllWords) {
			for (int i = 1; i < splitQuery.length; i++) {
				String queryWord = splitQuery[i];
				ArrayList<Integer> wordPositions = doc.getWordPositions().get(queryWord);
				for (int j = 0; j < wordPositions.size(); j++) {
					wordPositions.set(j, wordPositions.get(j) - i);
				}
			}
			
			// Do an intersection of all wordPosition arraylists for each query word. If not emptyset,
			// then all query terms appear in correct order.
			Map<String,ArrayList<Integer>> positionMap = doc.getWordPositions();
			for (int i = 1; i < splitQuery.length; i++) {
				positionMap.get(splitQuery[0]).retainAll(positionMap.get(splitQuery[i]));
			}
			if (!positionMap.get(splitQuery[0]).isEmpty()) {
				results.put(doc.getDocID(), doc);
			}
		}

		if (results.isEmpty()) return null;
		return results;
	}

	/**
	 * Creates a MatchingDocument with all instance variables filled in using the results
	 * received from the inverted index for a specific word in the search query.
	 */
	protected MatchingDocument createMatchingDocumentFromPostingList(Map<String,List<BigDecimal>> posting_list, String docID, String queryWord) {
		// Get word positions and tfidf score from indexer table.
		List<BigDecimal> positions = posting_list.get(docID);
		double tfidfScore = positions.get(0).doubleValue();
		positions.remove(0); // removes TFIDF score from positions list.
		ArrayList<Integer> positionsInInt = convertBigDecimalToInteger(positions);
		// Get page info from crawler table.
		Item crawledDoc = getItemFromCrawlerTable(docID);
		String url = crawledDoc.getString("url");
		Double pagerank = crawledDoc.getNumber("pagerank").doubleValue();

		MatchingDocument newMd = new MatchingDocument(docID);
		newMd.addWordAndPositions(queryWord, positionsInInt);
		newMd.addTfidfScore(tfidfScore);
		newMd.setUrl(url);
		newMd.setPageRankScore(pagerank);

		return newMd;
	}


	/////// Below was testing to add / get items from DynamoDB. Get methods are only needed for this project.
	protected void addItemsToIndexer() {	
		String tableName = "IndexerTable";
		Table table = dynamoDB.getTable(tableName);
		Map<String, List<Integer>> posting_list = new HashMap<String, List<Integer>>();
		List<Integer> positionList1 = new ArrayList<Integer>();
		positionList1.add(2);
		posting_list.put("doc3", positionList1);

		Item item = new Item()                
		.withPrimaryKey("word", "adorable")                	     
		.withMap("posting_list", posting_list)
		.withNumber("tfidf", 0.66);
		table.putItem(item);
	}

	protected Item getItemFromIndexer(String wordKey) {
		Table table = dynamoDB.getTable("IndexerTable");
		Item item = table.getItem("word", wordKey);
		return item;
	}

	protected void addItemsToCrawlerTable() {
		String tableName = "CrawlerTable";
		Table table = dynamoDB.getTable(tableName);
		Item item = new Item()                
		.withPrimaryKey("docID", "doc3")                	     
		.withString("url", "http://www.buzzfeed.com/kaelintully/sign-up-and-send-me-pics-of-your-pets-it-will-be-a-magical-f#.upawr3yzEe")
		.withNumber("pagerank", 0.60);
		table.putItem(item);
	}

	protected Item getItemFromCrawlerTable(String docID) {
		Table table = dynamoDB.getTable("CrawlerTable");
		Item item = table.getItem("docID", docID);
		return item;
	}

	/**
	 * Used for testing purposes only.
	 */
	protected void setDynamoDB() {
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. " +
							"Please make sure that your credentials file is at the correct " +
							"location (/home/cis455/.aws/credentials), and is in valid format.",
							e);
		}

		AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials);
		dynamoDB = new DynamoDB(client);
	}

}
