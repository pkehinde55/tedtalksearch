package edu.upenn.cis.nets212.hw1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

import edu.upenn.cis.nets212.config.Config;
import edu.upenn.cis.nets212.storage.DynamoConnector;
import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.stemmer.Stemmer;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class QueryForWord {
	/**
	 * A logger is useful for writing different types of messages
	 * that can help with debugging and monitoring activity.  You create
	 * it and give it the associated class as a parameter -- so in the
	 * config file one can adjust what messages are sent for this class. 
	 */
	static Logger logger = LogManager.getLogger(QueryForWord.class);

	/**
	 * Connection to DynamoDB
	 */
	DynamoDB db;
	
	/**
	 * Inverted index
	 */
	Table iindex;
	
	Stemmer stemmer;

	/**
	 * Default loader path
	 */
	public QueryForWord() {
		stemmer = new PorterStemmer();
	}
	
	/**
	 * Initialize the database connection
	 * 
	 * @throws IOException
	 */
	public void initialize() throws IOException {
		logger.info("Connecting to DynamoDB...");
		db = DynamoConnector.getConnection(Config.DYNAMODB_URL);
		logger.debug("Connected!");
		iindex = db.getTable("inverted");
	}
	
	/**
	 * Checks if input string is a word made of only
	 * alphabetic letters
	 * @author - pkehinde
	 * */
	public boolean isWord(String sequence) {
		int index = 0;
		while(index != sequence.length()) {
			if(!Character.isLetter(sequence.charAt(index))) {
				return false;
			}
			index++;
		}
		return true;
	}
	
	public Set<Set<String>> query(final String[] words) throws IOException, DynamoDbException, InterruptedException {
		// TODO implement query() in QueryForWord
		
		ArrayList<String> tokens = new ArrayList<String>();
		for(int i = 0; i < words.length;i++) {
			if(!(!isWord(words[i]) || words[i] == "a"
					|| words[i] == "all"|| words[i] == "any" 
					|| words[i] == "but" || words[i] == "the")
					
			) {
				tokens.add((String) stemmer.stem(words[i].toLowerCase()));
			}
		}
		Set<Set<String>> results = new HashSet<Set<String>>();
		
		for(int i = 0; i < tokens.size(); i++) {
			ItemCollection<QueryOutcome> matches = iindex.query("keyword", tokens.get(i)); 
			Iterator <Item> itr = matches.iterator();
			Set<String> links = new HashSet<String>();
			while(itr.hasNext()) {
				links.add(itr.next().getString("url"));
			}
			results.add(links);
		
		}

		return results;
	}

	/**
	 * Graceful shutdown of the DynamoDB connection
	 */
	public void shutdown() {
		logger.info("Shutting down");
		DynamoConnector.shutdown();
	}

	public static void main(final String[] args) {
		final QueryForWord qw = new QueryForWord();

		try {
			qw.initialize();

			final Set<Set<String>> results = qw.query(args);
			for (Set<String> s : results) {
				System.out.println("=== Set");
				for (String url : s)
				  System.out.println(" * " + url);
			}
  		System.out.println(results.toString());
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final DynamoDbException e) {
			e.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			qw.shutdown();
		}
	}

}
