package edu.sjsu.cmpe.library;

import javax.jms.Connection;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.views.ViewBundle;

import edu.sjsu.cmpe.library.api.resources.BookResource;
import edu.sjsu.cmpe.library.api.resources.RootResource;
import edu.sjsu.cmpe.library.config.LibraryServiceConfiguration;
import edu.sjsu.cmpe.library.repository.BookRepository;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;
import edu.sjsu.cmpe.library.ui.resources.HomeResource;


public class LibraryService extends Service<LibraryServiceConfiguration> {

    private final Logger log = LoggerFactory.getLogger(getClass());
    public static String instanceName;
    public static String topicName;
    public static Connection connection;
    public static BookRepositoryInterface bookRepository;
    public static void main(String[] args) throws Exception {
	new LibraryService().run(args);	
	new LibraryTopicListener();
    }

    @Override
    public void initialize(Bootstrap<LibraryServiceConfiguration> bootstrap) {
	bootstrap.setName("library-service");
	bootstrap.addBundle(new ViewBundle());
    }

    @Override
    public void run(LibraryServiceConfiguration configuration,
	    Environment environment) throws Exception {
	// This is how you pull the configurations from library_x_config.yml
	String queueName = configuration.getStompQueueName();
	topicName = configuration.getStompTopicName();
	 instanceName=configuration.getInstanceName();
	String user = configuration.getApolloUser();
	String password = configuration.getApolloPassword();
	String host = configuration.getApolloHost();
	int port = Integer.parseInt(configuration.getApolloPort());
	log.debug("Queue name is {}. Topic name is {}", queueName,
		topicName);
	// TODO: Apollo STOMP Broker URL and login

	
	StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
	factory.setBrokerURI("tcp://" + host + ":" + port );
	
	connection = factory.createConnection(user,password);
	connection.start();
	
	/** Root API */
	environment.addResource(RootResource.class);
	/** Books APIs */
	BookRepositoryInterface bookRepository = new BookRepository();
	environment.addResource(new BookResource(bookRepository,queueName,topicName,instanceName,user,password,host,port,connection));

	/** UI Resources */
	environment.addResource(new HomeResource(bookRepository));
    }
}
