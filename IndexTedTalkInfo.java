package edu.upenn.cis.nets212.hw1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import edu.upenn.cis.nets212.hw1.files.TedTalkParser.TalkDescriptionHandler;
import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.tokenize.SimpleTokenizer;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

/**
 * Callback handler for talk descriptions.  Parses, breaks words up, and
 * puts them into DynamoDB.
 * 
 * @author zives
 *
 */
public class IndexTedTalkInfo implements TalkDescriptionHandler {
	static Logger logger = LogManager.getLogger(TalkDescriptionHandler.class);

  final static String tableName = "inverted";
	int row = 0;
	
	SimpleTokenizer model;
	Stemmer stemmer;
	DynamoDB db;
	Table iindex;
	
	public IndexTedTalkInfo(final DynamoDB db) throws DynamoDbException, InterruptedException {
		model = SimpleTokenizer.INSTANCE;
		stemmer = new PorterStemmer();
		this.db = db;

		initializeTables();
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
	/**
	 * Returns a tokenized version of the input String in the 
	 * form of a String array. The tokens are non-stop words and are stemmed
	 * to their root version
	 * */
	public ArrayList<String> breaker(String columnString){
		String [] tokenHolder = model.tokenize(columnString);
		ArrayList<String> tokens = new ArrayList<String>();
		for(int i = 0; i < tokenHolder.length;i++) {
			if(!(!isWord(tokenHolder[i]) || tokenHolder[i] == "a"
					|| tokenHolder[i] == "all"||tokenHolder[i] == "any" 
					|| tokenHolder[i] == "but" || tokenHolder[i] == "the")
					
			) {
				tokens.add((String) stemmer.stem(tokenHolder[i].toLowerCase()));
			}
		}
		return tokens;
	}
	/**
	 * Adds item showing that the word was used in this particular 
	 * Ted Talk
	 * @param String keyword - word that is related to Ted Talk
	 * @param id - id of Ted Talk
	 * @param link - url of Ted Talk
	 */
	public Item produceItem(String keyword, int id, String link) {
		Item item = new Item()
		.withPrimaryKey("keyword",keyword, "inxid", id).withString("url", link);
		return item;
		
	}

	/**
	 * Called every time a line is read from the input file. Breaks into keywords
	 * and indexes them.
	 * 
	 * @param csvRow      Row from the CSV file
	 * @param columnNames Parallel array with the names of the table's columns
	 */
	@Override
	public void accept(final String[] csvRow, final String[] columnNames) {
		// TODO implement accept() in IndexTexTalkInfo.java
		int talkId = Integer.parseInt(csvRow[0]);
		String url = csvRow[16];
		Map <String, ArrayList<String>> colData = new HashMap<String, ArrayList<String>>();
		for(int i = 1; i < csvRow.length;i++) {
			if(i < 6 || i == 14 || i == 15 || i == 17 || i == 18) {
				colData.put(columnNames[i], breaker(csvRow[i]));
			}
		}
		Set <Item> setItems = new HashSet<Item>();
		Iterator <Map.Entry<String, ArrayList<String>>> itr = colData.entrySet().iterator();
		while(itr.hasNext()) {
			Map.Entry<String, ArrayList<String>> curr = (Map.Entry<String, ArrayList<String>>)itr.next();
			ArrayList <String> words = curr.getValue();
			for(int i = 0; i < words.size(); i++) {
				if(csvRow[0] != null && url != null) {
				setItems.add(produceItem(words.get(i), talkId, url));
				}
				if(setItems.size() == 25 || i == words.size() - 1) {
					TableWriteItems data = new TableWriteItems(tableName).withItemsToPut(setItems);
					BatchWriteItemOutcome outcome = db.batchWriteItem(data);
					Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
					if (!outcome.getUnprocessedItems().isEmpty()) {
	                 
	                    outcome = db.batchWriteItemUnprocessed(unprocessedItems);
	                }
					setItems.clear();
				}
				
			}
			/**TableWriteItems data = new TableWriteItems(tableName).withItemsToPut(setItems);
			BatchWriteItemOutcome outcome = db.batchWriteItem(data);
			Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
			if (!outcome.getUnprocessedItems().isEmpty()) {
             
                outcome = db.batchWriteItemUnprocessed(unprocessedItems);
            }**/
			
		}
	}

	private void initializeTables() throws DynamoDbException, InterruptedException {
		try {
			iindex = db.createTable(tableName, Arrays.asList(new KeySchemaElement("keyword", KeyType.HASH), // Partition
																												// key
					new KeySchemaElement("inxid", KeyType.RANGE)), // Sort key
					Arrays.asList(new AttributeDefinition("keyword", ScalarAttributeType.S),
							new AttributeDefinition("inxid", ScalarAttributeType.N)),
					new ProvisionedThroughput(100L, 100L));

			iindex.waitForActive();
		} catch (final ResourceInUseException exists) {
			iindex = db.getTable(tableName);
		}

	}

	/**
	 * Given the CSV row and the column names, return the column with a specified
	 * name
	 * 
	 * @param csvRow
	 * @param columnNames
	 * @param columnName
	 * @return
	 */
	public static String lookup(final String[] csvRow, final String[] columnNames, final String columnName) {
		final int inx = Arrays.asList(columnNames).indexOf(columnName);
		
		if (inx < 0)
			throw new RuntimeException("Out of bounds");
		
		return csvRow[inx];
	}
}
