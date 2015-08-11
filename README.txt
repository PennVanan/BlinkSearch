CIS 455 README for Blink Search Engine


Project Team Members
==================
Lori Becker SEAS ID: belori
Aayush Gupta SEAS ID: aayushg
Alagiavanan Saravanan SEAS ID: alasar
Sagarika Rayudu SEAS ID: rayudu




Description of Features Implemented
============================
Crawler: The crawler follows a multithreaded mercator design running on various nodes in a map reduce style framework.There is a single master and mutiple workers which send their status to the master every 10 seconds.Once the seed urls are taken from a web form, the master hashes each seed urls and send them to the corresponding worker along with a list of all workers and their ip:port values. Each worker now knows about every other worker. Worker has a queue on disk to save its state incase crawl is interrupted. All html documents are crawled, href links are extracted and stored in berkeley DB while the contents of the html file are stored in S3. Each time hrefs are extracted their docids are hashed and each link is sent to corresponding worker and they are also stored in S3 for the Page rank.
Storage in S3:bucket-1:content  bucket-2:outlinks from a page.
Storage in Berkeley DB: local content needed for the crawler(robots, last crawled etc).


Indexer


When the Crawler is finished crawling, the Indexer is invoked. The Crawler saves data required by Indexer in an S3 bucket which can be directly accessed. The files are in .avro format so that data can be efficiently serialized and retrieved in the Map-Reduce job responsible for making inverted index. Indexer first deserializes data from .avro file, gets the content and sends this for preprocessing. 


During preprocessing, the content is stripped of all the unnecessary content (depending on the file type) and the raw text is retained. The content remaining is now tokenized using openNLP tokenizer and their model trained over their own data sets. Note that stemming and lemmatization has been avoided in favor of precise “Phrase Search queries”.


For each document processed, the mapper outputs every unique word in the document, along with the docID and a list of locations of the word inside that document (The list is called posting list). It also passes to the reducer the normalized term frequency scores. The reducer in turn combines all these posting lists for every unique word, calculates TF-IDF scores and creates the inverted index which is saved in DynamoDB so that faster queries can be achieved.


PageRank


The PageRank module is invoked alongside the indexer module after the crawler is completed. It is run as a set of 3 EMR jobs. The first job identifies the sinks to single them out because as sinks have no outgoing links they are not involved in the pagerank calculation process. The second job initializes the rank of all nodes except sinks to 1 and presents them in the format that could be used to calculate pagerank. Specifically, it maps the children to the respective parent nodes. The final EMR job calculates the page rank as per the formula and is executed iteratively until convergence. Roughly a  set of 25-30 iterations is executed to get consistent pagerank values for all links. 


Search Engine/WebUI


When a search query is submitted to the search engine, the search engine first determines whether this session is new. If the session is new, the query and an offset are saved as attributes for this session, results are retrieved from the indexer by selecting the union of documents containing all words in the query, and sorted by descending rank using a combination of a weighted TF/IDF and weighted PageRank score. If there are more than 10 results, the results are then cached in DynamoDB to provide faster results when requesting the next 10 matches or previous 10 matches. In addition, the client’s location is determined from their IP address and that is used to get business results from the Yelp API that are relevant to the client’s search query. If the session is not new and the client has requested more results from the same search query, the next 10 or previous 10 results are pulled from the cache and displayed. If the search query is different, the session attributes are updated and the request is treated as if the session was new.


Extra Credit
=========
1. Search Engine/Web UI contains location based search results from Yelp.
2. Search Engine/Web UI can handle phrase based queries.
3. Crawler stores both HTML and PDF filetypes.
4. Indexer includes word metadata.
5. Indexer can handle location specific results.




How To Install and Run Our Project
===========================
Crawler: To run the crawler, the master and workers are setup on EC2 on multiple nodes and a list of seed urls are given through a web form which are passed on to the workers. 


Indexer: To run the indexer, an EMR cluster is set up and the job (as a .jar file) is placed in an S3 bucket. The job can be started from the console in AWS.


PageRank: To run the PageRank, set up EMR cluster in the same way as for indexer and place the jar files in S3 buckets. Starting the jobs in AWS console in sequential order yields the final result.


Search Engine/WebUI: I have included the .war file that implements the search engine (Blink.war). If run on an EC2 instance with Tomcat installed, simply run the .war file. Note that the search won’t go through because it is dependent upon having access to multiple DynamoDB tables, which are obviously non available to you at this time. However, you should be able to view the main page by going to http://ec2.something.aws.amazon.com/Blink/index.html.

List of Source Files
======================
com/blink/crawler/MasterNode/MasterServlet.java
com/blink/crawler/MasterNode/Status.java
com/blink/crawler/MasterNode/package-info.java
com/blink/crawler/MasterNode/Worker_Form.java
com/blink/crawler/MasterNode/MasterClient.java
com/blink/crawler/WorkerNode/CommonParameters.java
com/blink/crawler/WorkerNode/RobotsTxtInfo.java
com/blink/crawler/WorkerNode/WorkerThread.java
com/blink/crawler/WorkerNode/Worker.java
com/blink/crawler/WorkerNode/package-info.java
com/blink/crawler/WorkerNode/SagaClient.java
com/blink/crawler/Storage/DBWrapper.java
com/blink/crawler/Storage/RobotsObj.java
com/blink/crawler/Storage/Url_Crawl.java
com/blink/crawler/Storage/Serializer.java
com/blink/crawler/Storage/Queue.java
com/blink/crawler/Storage/Url_Seen.java
com/blink/crawler/Storage/MyDatabase.java
com/blink/avro/RankerSchema.java
com/blink/avro/RankerSerialData.java
com/blink/avro/DocumentSchema.java
com/blink/avro/IndexSerialData.java
com/blink/indexer/MapReduceIndexing.java
com/blink/indexer/NLP/NLPTest.java
com/blink/indexer/NLP/NLP.java
com/blink/indexer/storage/DynamoStorage.java
com/blink/indexer/storage/IndexValue.java
com/blink/pageranker/DynamoStorage.java
com/blink/pageranker/PageRank_MR2.java
com/blink/pageranker/PageRank_MR1.java
com/blink/pageranker/PageRank_Test.java
com/blink/pageranker/PageRank_MR3.java
com/blink/utils/IndexTrie.java
com/blink/utils/HtmlParserTest.java
com/blink/utils/HtmlParser.java
com/blink/webUI/Yelp.java
com/blink/webUI/TwoStepOAuth.java
com/blink/webUI/BlinkSearchEngine.java
com/blink/webUI/MatchingDocument.java
com/blink/webUI/BlinkSearchEngineTest.java
