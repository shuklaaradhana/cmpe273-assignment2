package edu.sjsu.cmpe.procurement.jobs;

import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;
import edu.sjsu.cmpe.procurement.ProcurementService;
import edu.sjsu.cmpe.procurement.domain.Book;
import edu.sjsu.cmpe.procurement.domain.BookCollection;

/**
 * This job will run at every 5 second.
 */
@Every("5min")

public class ProcurementSchedulerJob extends Job {
	private final Logger log = LoggerFactory.getLogger(getClass());

	private static StompJmsConnectionFactory factory;
	private static Connection connection;

	@Override
	public void doJob() {
		long waitUntil = 5000;
		MessageConsumer consumer = null;
		ArrayList<Integer> arrayBookIsbn = new ArrayList<Integer>();
		String queue = "/queue/38340.book.orders";
		String destination = queue;
		
//		//added new for testing
//		BookCollection bookCollection = new BookCollection();
//		Book book1 = new Book(1,"Java Concurrency in Practice","computer");
//		Book book2 = new Book(101,"inGenius","selfimprovement");
//		Book book3 = new Book(200,"Harry Potter","comics");
//		Book book4= new Book(500,"Managemnet Guru","management");
//		bookCollection.addBookToCollection(book1);
//		bookCollection.addBookToCollection(book2);
//		bookCollection.addBookToCollection(book3);
//		bookCollection.addBookToCollection(book4);
		//end of //added new for testing
		
		
		try {
			factory = new StompJmsConnectionFactory();
			factory.setBrokerURI(ProcurementService.apolloHostURLTCP);
			connection = factory.createConnection(ProcurementService.user,
					ProcurementService.password);
			connection.start();
			Session session = connection.createSession(false,
					Session.AUTO_ACKNOWLEDGE);
			Destination dest = new StompJmsDestination(destination);
			consumer = session.createConsumer(dest);
		} catch (JMSException e) {

		}

		System.out.println("Waiting for messages from " + queue + "...");
		
		//Added new for testing
//		try{
//		postToTopic(bookCollection);
//		}
//		catch(JMSException e){
//			
//		}
		
		//End of Added new for testing

		while (true) {
			try {
				Message msg = consumer.receive(waitUntil);
				String body = null;
				String[] str_array;
				String stringb;

				if (msg instanceof TextMessage) {
					body = ((TextMessage) msg).getText();
					if(!("SHUTDOWN".equals(body)) && body!=null ){
						str_array = body.split(":");
						stringb = str_array[1];
						int bookIsbn = Integer.parseInt(stringb);
						arrayBookIsbn.add(bookIsbn);
					}
					System.out.println("Received message = " + body);
				} else if (msg == null) {
					System.out
							.println("No new messages. Existing due to timeout - "
									+ waitUntil / 1000 + " sec");
					break;
				} else {
					System.out.println("Unexpected message type: "
							+ msg.getClass());
				}
			} catch (JMSException e) {

			}
		} // end while loop
			// close the connection
		try {
			connection.close();
		} catch (JMSException e) {

		}
		System.out.println("Done");

		if (!arrayBookIsbn.isEmpty()) {
			for (int i = 0; i < arrayBookIsbn.size(); i++) {
				System.out.println(arrayBookIsbn.get(i) + "  ");
			}
			postToPublisher(arrayBookIsbn);
			getFromPublisher();
		}
	}// end functionxs

	public static void postToPublisher(ArrayList<Integer> arrayBookIsbn) {

		String bookIsbn = null;
		try {
			Client client = Client.create();
			WebResource webResource = client
					.resource(ProcurementService.apolloHostURLHTTPPost);

			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < arrayBookIsbn.size(); i++) {
				sb.append("\"");
				sb.append(arrayBookIsbn.get(i));
				if (i == (arrayBookIsbn.size() - 1))
					sb.append("\"");
				else
					sb.append("\",");
			}

			System.out.println(sb.toString());
			String input = "{\"id\":\"38340\",\"order_book_isbns\":[" + sb
					+ "]}";

			ClientResponse response = webResource.type("application/json")
					.post(ClientResponse.class, input);

			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ response.getStatus());
			}

			System.out.println("Output from Server .... \n");
			String output = response.getEntity(String.class);
			System.out.println(output);

		} catch (Exception e) {

			e.printStackTrace();

		}

	}

	public static void getFromPublisher() {
		try {

			Client client = Client.create();

			WebResource webResource = client
					.resource(ProcurementService.apolloHostURLHTTPGet);

			ClientResponse response = webResource.accept("application/json")
					.get(ClientResponse.class);

			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ response.getStatus());
			}

			String output = response.getEntity(String.class);

			System.out.println("Output from Server .... \n");
			System.out.println(output);

			ObjectMapper mapper = new ObjectMapper();
			BookCollection bookCollection = mapper.readValue(output,
					BookCollection.class);
			for (int i = 0; i < bookCollection.getBookCollection().size(); i++) {
				System.out.println(bookCollection.getBookCollection().get(i)
						+ "   ");				
			}
			
			postToTopic(bookCollection);

		} catch (Exception e) {

			e.printStackTrace();

		}
		
		
		
	}
//		
		public static void postToTopic(BookCollection bookCollection) throws JMSException{
			
			String destination = null;
			Destination dest;
			TextMessage msg;
			String data;
			factory = new StompJmsConnectionFactory();
			factory.setBrokerURI(ProcurementService.apolloHostURLTCP);
			connection = factory.createConnection(ProcurementService.user,
					ProcurementService.password);
			connection.start();
			Session session = connection.createSession(false,
					Session.AUTO_ACKNOWLEDGE);		
			
			
			for(int i=0;i<bookCollection.getBookCollection().size();i++){
				Book book = bookCollection.getBookCollection().get(i);
			
			data = book.getIsbn() +  ": \"" + book.getTitle() + "\" : \"" +
			book.getCategory() + "\" : \"" + book.getCoverimage() + "\"";
			
			if("comics".equals(book.getCategory())){
				destination = ProcurementService.topicName + "comics";	
			}
			else if("computer".equals(book.getCategory())){
				destination = ProcurementService.topicName + "computer";
			}			
			else if("management".equals(book.getCategory())){
				destination = ProcurementService.topicName + "management";				
			}
			else if("selfimprovement".equals(book.getCategory())){
				destination = ProcurementService.topicName + "selfimprovement";				
			}
			else{
				System.out.println("Invalid category");
			}
				
			
			dest = new StompJmsDestination(destination);
			MessageProducer producer = session.createProducer(dest);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
				
			msg = session.createTextMessage(data);
			msg.setLongProperty("id", System.currentTimeMillis());
			System.out.println("Sending message to topic");
			producer.send(msg);
			
			}
			
			
			
			
//			Message Format:
			
		//	/topic/38340.book.
			
//			/topic/38340.book.comics
//			/topic/38340.book.computer
//			/topic/38340.book.management
//			/topic/38340.book.selfimprovement
//			{isbn}:{title}:{category}:{coverimage}    # category - all lowercase.
//			Example:
//			123:”Restful Web Services”:”computer”:”http://goo.gl/ZGmzoJ”  
		
		
			
			

			/**
			 * Notify all Listeners to shut down. if you don't signal them, they
			 * will be running forever.
			 */
		
			//connection.close();
		    }	   
		}// end class


