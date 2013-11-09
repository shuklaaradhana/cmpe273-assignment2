package edu.sjsu.cmpe.library;

import java.io.IOException;
import java.lang.Object;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Assert;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.sjsu.cmpe.library.domain.Book;
import edu.sjsu.cmpe.library.domain.Book.Status;
import edu.sjsu.cmpe.library.repository.BookRepository;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;

public class LibraryTopicListener {

	static String destination = null;
	private static BookRepositoryInterface bookRepository;
	

	public LibraryTopicListener(BookRepositoryInterface bookRepository){
			this.bookRepository=bookRepository;
		backgroundThreadExecution();
	}	

	public static void backgroundThreadExecution(){
	

	int numThreads = 1;
	ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    Runnable backgroundTask = new Runnable() {

	    @Override
	    public void run() {
	    	String[] str_array;
	    	String stringb;
		System.out.println("Hello World");
		
		if("library-a".equals(LibraryService.instanceName)){
			 destination =LibraryService.topicName ;
		}
		else if("library-b".equals(LibraryService.instanceName)){
			destination =LibraryService.topicName;
		}		

		try{
		Session session = LibraryService.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = new StompJmsDestination(destination);

		MessageConsumer consumer = session.createConsumer(dest);
		System.currentTimeMillis();
		System.out.println("Waiting for messages...");
		long isbn;
		String title;
		String category;
		URL coverimage;
		String protocol;
		String myurl;
		String urlStr;
		
		//{isbn}:{title}:{category}:{coverimage} 
		
			while(true) {
			    Message msg = consumer.receive();
			    if( msg instanceof  TextMessage ) {
				String body = ((TextMessage) msg).getText();
				
				System.out.println("Received message = " + body);
				
				str_array = body.split(":");
				stringb = str_array[0]; 
				isbn = Integer.parseInt(stringb);
		title = str_array[1];
	category = str_array[2];
	 protocol = str_array[3];
	 myurl = str_array[4];
	 urlStr = protocol + ":" +  myurl;
	 urlStr = urlStr.replaceAll("\"", "");
	URL url = new URL(urlStr);
//	URI uri = new URI(urlStr);
//	url = uri.toURL();
	Book book=new Book(isbn,title,category,url);
				
//				ObjectMapper mapper = new ObjectMapper();	    	    
//		    		book = mapper.readValue(body, Book.class);      
			
				updateBookStatus(isbn,book);
		 
			    } else if (msg instanceof StompJmsMessage) {
				StompJmsMessage smsg = ((StompJmsMessage) msg);
				String body = smsg.getFrame().contentAsString();
				
				System.out.println("Received message = " + body);

			    } else {
				System.out.println("Unexpected message type: "+msg.getClass());
			    }
	    }
		}
		catch(JMSException e){
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
    };

	System.out.println("About to submit the background task");
	executor.execute(backgroundTask);
	System.out.println("Submitted the background task");

	executor.shutdown();
	System.out.println("Finished the background task");
	executor.shutdown();
	System.out.println("Finished the background task");
} 
	
	public static void updateBookStatus(long isbn,Book book){
		 List<Book> bookCollection = bookRepository.getAllBooks();
		
		 int flag=0;
		 for(int i=0;i<bookCollection.size();i++){
		
				int value = new Long(isbn).compareTo(bookCollection.get(i).getIsbn());
				if(value == 0){
			
				 bookCollection.get(i).setStatus(Status.available);
				 flag=1;
				 System.out.println ("Book: " + bookCollection.get(i).getIsbn() + "Status: " +  bookCollection.get(i).getStatus());
				 break;			 
		 }
		 
		 }
		 
		 if(flag == 0){
			 bookRepository.saveBook(book);
			 System.out.println("New book added: " + book.getIsbn() + "Status:" + book.getStatus() );
		 }
		
	}
}

	

