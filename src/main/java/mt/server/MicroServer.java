package mt.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import mt.Order;
import mt.comm.ServerComm;
import mt.comm.ServerSideMessage;
import mt.comm.impl.ServerCommImpl;
import mt.exception.ServerException;
import mt.filter.AnalyticsFilter;

/**
 * MicroTraderServer implementation. This class should be responsible
 * to do the business logic of stock transactions between buyers and sellers.
 * 
 * @author Group 78
 *
 */
public class MicroServer implements MicroTraderServer {
	
	public static void main(String[] args) {
		ServerComm serverComm = new AnalyticsFilter(new ServerCommImpl());
		MicroTraderServer server = new MicroServer();
		server.start(serverComm);
	}

	public static final Logger LOGGER = Logger.getLogger(MicroServer.class.getName());

	/**
	 * Server communication
	 */
	private ServerComm serverComm;

	/**
	 * A map to sore clients and clients orders
	 */
	private Map<String, Set<Order>> orderMap;

	/**
	 * Orders that we must track in order to notify clients
	 */
	private Set<Order> updatedOrders;

	/**
	 * Order Server ID
	 */
	private static int id = 1;
	
	/** The value is {@value #EMPTY} */
	public static final int EMPTY = 0;

	/**
	 * Constructor
	 */
	public MicroServer() {
		LOGGER.log(Level.INFO, "Creating the server...");
		orderMap = new HashMap<String, Set<Order>>();
		updatedOrders = new HashSet<>();
	}

	@Override
	public void start(ServerComm serverComm) {
		serverComm.start();
		
		LOGGER.log(Level.INFO, "Starting Server...");

		this.serverComm = serverComm;

		ServerSideMessage msg = null;
		while ((msg = serverComm.getNextMessage()) != null) {
			ServerSideMessage.Type type = msg.getType();
			
			if(type == null){
				serverComm.sendError(null, "Type was not recognized");
				continue;
			}

			switch (type) {
				case CONNECTED:
					try{
						processUserConnected(msg);
					}catch (ServerException e) {
						serverComm.sendError(msg.getSenderNickname(), e.getMessage());
					}
					break;
				case DISCONNECTED:
					processUserDisconnected(msg);
					break;
				case NEW_ORDER:
					try {
						verifyUserConnected(msg);
						if(msg.getOrder().getNumberOfUnits() >= 10){
							if(msg.getOrder().getServerOrderID() == EMPTY){
								msg.getOrder().setServerOrderID(id++);
							}
							notifyAllClients(msg.getOrder());
							processNewOrder(msg);
						}
						else{
							displayWarning("Number of units must be greater than 9");
						}
					} catch (ServerException e) {
						serverComm.sendError(msg.getSenderNickname(), e.getMessage());
					}
					break;
				default:
					break;
				}
		}
		LOGGER.log(Level.INFO, "Shutting Down Server...");
	}


	/**
	 * Verify if user is already connected
	 * 
	 * @param msg
	 * 			the message sent by the client
	 * @throws ServerException
	 * 			exception thrown by the server indicating that the user is not connected
	 */
	private void verifyUserConnected(ServerSideMessage msg) throws ServerException {
		for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
			if(entry.getKey().equals(msg.getSenderNickname())){
				return;
			}
		}
		throw new ServerException("The user " + msg.getSenderNickname() + " is not connected.");
		
	}

	/**
	 * Process the user connection
	 * 
	 * @param msg
	 * 			  the message sent by the client
	 * 
	 * @throws ServerException
	 * 			exception thrown by the server indicating that the user is already connected
	 */
	private void processUserConnected(ServerSideMessage msg) throws ServerException {
		LOGGER.log(Level.INFO, "Connecting client " + msg.getSenderNickname() + "...");
		
		// verify if user is already connected
		for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
			if(entry.getKey().equals(msg.getSenderNickname())){
				throw new ServerException("The user " + msg.getSenderNickname() + " is already connected.");
			}
		}
		
		// register the new user
		orderMap.put(msg.getSenderNickname(), new HashSet<Order>());
		
		notifyClientsOfCurrentActiveOrders(msg);
	}
	
	/**
	 * Send current active orders sorted by server ID ASC
	 * @param msg
	 */
	private void notifyClientsOfCurrentActiveOrders(ServerSideMessage msg) {
		List<Order> ordersToSend = new ArrayList<>();
		// update the new registered user of all active orders
		for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
			Set<Order> orders = entry.getValue();
			for (Order order : orders) {
				ordersToSend.add(order);
			}
		}
		
		// sort the orders to send to clients by server id
		Collections.sort(ordersToSend, new Comparator<Order>() {
			@Override
			public int compare(Order o1, Order o2) {
				return o1.getServerOrderID() < o2.getServerOrderID() ? -1 : 1;
			}
		});
		
		for(Order order : ordersToSend){
			serverComm.sendOrder(msg.getSenderNickname(), order);
		}
	}

	/**
	 * Process the user disconnection
	 * 
	 * @param msg
	 * 			  the message sent by the client
	 */
	private void processUserDisconnected(ServerSideMessage msg) {
		LOGGER.log(Level.INFO, "Disconnecting client " + msg.getSenderNickname()+ "...");
		
		//remove the client orders
		orderMap.remove(msg.getSenderNickname());
		
		// notify all clients of current unfulfilled orders
		for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
			Set<Order> orders = entry.getValue();
			for (Order order : orders) {
				serverComm.sendOrder(msg.getSenderNickname(), order);
			}
		}
	}

	/**
	 * Process the new received order
	 * 
	 * @param msg
	 *            the message sent by the client
	 */
	private void processNewOrder(ServerSideMessage msg) throws ServerException {
		LOGGER.log(Level.INFO, "Processing new order...");

		Order o = msg.getOrder();
		
		// save the order on map
		saveOrder(o);

		// if is buy order
		if (o.isBuyOrder()) {
			processBuy(msg.getOrder());
		}
		
		// if is sell order
		if (o.isSellOrder()) {
			processSell(msg.getOrder());
		}

		// notify clients of changed order
		notifyClientsOfChangedOrders();

		// remove all fulfilled orders
		removeFulfilledOrders();

		// reset the set of changed orders
		updatedOrders = new HashSet<>();
		}
		
	
	/**
	 * Store the order on map
	 * 
	 * @param o
	 * 			the order to be stored on map
	 */
	private void saveOrder(Order o) {
		LOGGER.log(Level.INFO, "Storing the new order...");
		
		//save order on map
		Set<Order> orders = orderMap.get(o.getNickname());
		orders.add(o);
		
		try {
			processOrdersXML(o);
					
		} catch (ParserConfigurationException | SAXException | IOException | TransformerFactoryConfigurationError
				| TransformerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Process the sell order
	 * 
	 * @param sellOrder
	 * 		Order sent by the client with a number of units of a stock and the price per unit he wants to sell
	 */
	private void processSell(Order sellOrder){
		LOGGER.log(Level.INFO, "Processing sell order...");
		
		for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
			for (Order o : entry.getValue()) {
				if (o.isBuyOrder() && o.getStock().equals(sellOrder.getStock()) && o.getPricePerUnit() >= sellOrder.getPricePerUnit()) {
					doTransaction (o, sellOrder);
				}
			}
		}
		
	}
	
	/**
	 * Process the buy order
	 * 
	 * @param buyOrder
	 *          Order sent by the client with a number of units of a stock and the price per unit he wants to buy
	 */
	private void processBuy(Order buyOrder) {
		LOGGER.log(Level.INFO, "Processing buy order...");

		for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
			for (Order o : entry.getValue()) {
				if (o.isSellOrder() && buyOrder.getStock().equals(o.getStock()) && o.getPricePerUnit() <= buyOrder.getPricePerUnit()) {
					doTransaction(buyOrder, o);
				}
			}
		}

	}

	/**
	 * Process the transaction between buyer and seller
	 * 
	 * @param buyOrder 		Order sent by the client with a number of units of a stock and the price per unit he wants to buy 
	 * @param sellerOrder	Order sent by the client with a number of units of a stock and the price per unit he wants to sell
	 */
	private void doTransaction(Order buyOrder, Order sellerOrder) {
		LOGGER.log(Level.INFO, "Processing transaction between seller and buyer...");

		if (buyOrder.getNumberOfUnits() >= sellerOrder.getNumberOfUnits()) {
			buyOrder.setNumberOfUnits(buyOrder.getNumberOfUnits()
					- sellerOrder.getNumberOfUnits());
			sellerOrder.setNumberOfUnits(EMPTY);
		} else {
			sellerOrder.setNumberOfUnits(sellerOrder.getNumberOfUnits()
					- buyOrder.getNumberOfUnits());
			buyOrder.setNumberOfUnits(EMPTY);
		}
		
		updatedOrders.add(buyOrder);
		updatedOrders.add(sellerOrder);
	}
	
	/**
	 * Notifies clients about a changed order
	 * 
	 * @throws ServerException
	 * 			exception thrown in the method notifyAllClients, in case there's no order
	 */
	private void notifyClientsOfChangedOrders() throws ServerException {
		LOGGER.log(Level.INFO, "Notifying client about the changed order...");
		for (Order order : updatedOrders){
			notifyAllClients(order);
		}
	}
	
	/**
	 * Notifies all clients about a new order
	 * 
	 * @param order refers to a client buy order or a sell order
	 * @throws ServerException
	 * 				exception thrown by the server indicating that there is no order
	 */			
	private void notifyAllClients(Order order) throws ServerException {
		LOGGER.log(Level.INFO, "Notifying clients about the new order...");
		if(order == null){
			throw new ServerException("There was no order in the message");
		}
		for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
			serverComm.sendOrder(entry.getKey(), order); 
		}
	}
	
	/**
	 * Remove fulfilled orders
	 */
	private void removeFulfilledOrders() {
		LOGGER.log(Level.INFO, "Removing fulfilled orders...");
		
		// remove fulfilled orders
		for (Entry<String, Set<Order>> entry : orderMap.entrySet()) {
			Iterator<Order> it = entry.getValue().iterator();
			while (it.hasNext()) {
				Order o = it.next();
				if (o.getNumberOfUnits() == EMPTY) {
					it.remove();
				}
			}
		}
	}

	/**
	 * Displays a warning in the console and on a frame with the given String
	 * 
	 * @param warning String containing the warning to be displayed
	 */
	private void displayWarning(String warning){
		System.out.println(warning);
		JFrame frame=new JFrame("Orders");
		JOptionPane.showMessageDialog(frame, warning, "Warning",
                JOptionPane.WARNING_MESSAGE);
	}
	

	/**
	 * Checks if the XML file exists, creates 2 elements (Order and Customer) and writes the received order
	 * 
	 * @param o		Received order
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 * @throws TransformerFactoryConfigurationError
	 * @throws TransformerException
	 */
	private void processOrdersXML(Order o) throws ParserConfigurationException, SAXException, IOException, TransformerFactoryConfigurationError, TransformerException{
		File inputFile = new File("MiniTrader_AS.xml");
		checkFileExists(inputFile);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(inputFile);
		Element newElement=createNewElementOrder(o,doc);
		createNewElementCustomer(doc, newElement, o);
		writeXML(doc);
	}
	
	/**
	 * Checks the existence of the XML file and if it doesn't exist or is empty, it writes the XML tag in the XML file
	 * 
	 * @param f		XML File to be checked its existence
	 * @throws FileNotFoundException
	 */
	private void checkFileExists(File f) throws FileNotFoundException{
		PrintWriter pw;
		if(!f.exists() || f.length() == 0){
			pw = new PrintWriter(f);
			pw.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>" + "\n" + "<XML>" + "\n" + "</XML>");
			pw.close();
		}
	}
	
	
	
	/**
	 * Creates the Order element and sets the type, stock, units and price attributes
	 * 
	 * @param o		Received order
	 * @param doc	Document that is going to be used to write the element Order
	 * @return		Order element
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	private Element createNewElementOrder( Order o, Document doc) throws ParserConfigurationException, SAXException, IOException{
		String type;
        Element newElement = doc.createElement("Order");
        newElement.setAttribute("Id",Integer.toString(o.getServerOrderID()));
		if(o.isBuyOrder()){
			type="Buy";
		}
		else{
			type="Sell";
		}
        newElement.setAttribute("Type",type);
        newElement.setAttribute("Stock", o.getStock());
        newElement.setAttribute("Units", Integer.toString(o.getNumberOfUnits()));
        newElement.setAttribute("Price",Double.toString(o.getPricePerUnit()));
		return newElement;
	}
	
	/**
	 * Creates the Customer element, adds the element to the Order element and adds the Order element to the root element
	 * 
	 * @param doc			Document that is going to be used to write the element Customer		
	 * @param newElement	Order element
	 * @param o				Received order
	 */
	private void createNewElementCustomer(Document doc, Element newElement, Order o){
		Element e = doc.createElement("Customer");
        e.appendChild(doc.createTextNode(o.getNickname()));
        newElement.appendChild(e);
        // Add new node to XML document root element
        Node n = doc.getDocumentElement();
        n.appendChild(newElement);
	}
	
	/**
	 * Writes the received order and customer into the XML file
	 * 
	 * @param doc	Document to be written
	 * @throws TransformerFactoryConfigurationError
	 * @throws FileNotFoundException
	 * @throws TransformerException
	 */
	private void writeXML(Document doc) throws TransformerFactoryConfigurationError, FileNotFoundException, TransformerException{
		System.out.println("Save XML document.");
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        StreamResult result = new StreamResult(new FileOutputStream("MiniTrader_AS.xml"));
        DOMSource source = new DOMSource(doc);
        transformer.transform(source, result);
	}
}
