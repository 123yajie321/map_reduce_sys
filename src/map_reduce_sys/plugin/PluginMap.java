package map_reduce_sys.plugin;

import java.util.concurrent.LinkedBlockingQueue;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import map_reduce_sys.connector.ConnectorSendTuple;
import map_reduce_sys.interfaces.BiFunction;
import map_reduce_sys.interfaces.Function;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleImplementationI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.interfaces.createCalculServiceI;
import map_reduce_sys.ports.ManagementInboundPortForPlugin;
import map_reduce_sys.ports.ReceiveTupleWithPluginInboundPort;
import map_reduce_sys.ports.SendTupleOutboundPort;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.OrderedTuple;
import map_reduce_sys.structure.Tuple;
/**
 * The class <code>PluginMap</code> implements the calculate component side  plug-in
 * for the <code>ManagementI</code> component interface and it implements also the the client side and server side
 * for the <code>SendTupleServiceI</code> component interface
 * @author Yajie LIU, Zimeng ZHANG
 */

public class PluginMap extends AbstractPlugin implements ManagementI{
	private static final long serialVersionUID=1L;
	/**Inbound port for management, Offer the service runTaskMap*/
	protected ManagementInboundPortForPlugin managementMapPluginInboundPort;
	/**uri of inbound port for management*/
	protected String ManagementInPortUri; 
	/**The number of threads in the thread pool used to perform the calculation  */
	protected int nbThread;
	/**Function used to do Map calculate*/
	protected Function<Tuple, Tuple> fonction_map;
	/**A buffer to store the result tuples to be sent*/
	protected LinkedBlockingQueue<Tuple> bufferSend;
	/**Thread pools used for the execution of function fonction_map*/
	protected int indexCalculExector;
	/**Thread pools used to send tuple*/
	protected int indexSendExector;
	/**outbound port to send tuple to Reduce component*/
	protected SendTupleOutboundPort sendTupleobp;
	/**Inbound port to receive tuple from Resourc ecomponent  */
	protected ReceiveTupleWithPluginInboundPort ReceiveTupleInboundPort;
	/**uri of inbound port to receive tuple */
	protected String receiveTupleInPortUri;
	/** The uri of inboundPort of the sendTuple destination*/
	protected String sendReduceTupleInboundPortUri;

	

	
	public PluginMap(String uri,int nb,Function<Tuple, Tuple> fonction_map,String inboundPortReceiveTupleuri,String inboundPortSendReduceUri) {
		super();
		this.ManagementInPortUri=uri;
		this.receiveTupleInPortUri=inboundPortReceiveTupleuri;
		this.sendReduceTupleInboundPortUri=inboundPortSendReduceUri;
		this.nbThread=nb;
		this.fonction_map=fonction_map;
		bufferSend=new LinkedBlockingQueue<Tuple>();
	}
	
	
	
	@Override
	public void	installOn(ComponentI owner) throws Exception{
		super.installOn(owner);

		this.addRequiredInterface(SendTupleServiceI.class);
		this.sendTupleobp=new SendTupleOutboundPort(this.getOwner());
		this.sendTupleobp.publishPort();
		
	}
	
	@Override
	public void initialise() throws Exception{
		
		super.initialise();
		this.addOfferedInterface(ManagementI.class);
		this.managementMapPluginInboundPort = new ManagementInboundPortForPlugin(ManagementInPortUri,this.getPluginURI(),this.getOwner());
		this.managementMapPluginInboundPort.publishPort();
		
		
		this.addOfferedInterface(SendTupleServiceI.class);
		this.ReceiveTupleInboundPort=new ReceiveTupleWithPluginInboundPort(receiveTupleInPortUri,this.getPluginURI(),this.getOwner() );
		this.ReceiveTupleInboundPort.publishPort();
		
		indexCalculExector=createNewExecutorService("MapCalculexector_uri", nbThread,false);
		indexSendExector=createNewExecutorService("MapSendexector_uri", nbThread,false);
		
}
	
	@Override
	public void			finalise() throws Exception
	{
		this.getOwner().doPortDisconnection(this.sendTupleobp.getPortURI());
	}
	
	@Override
	public void uninstall() throws Exception {
		this.managementMapPluginInboundPort.unpublishPort();
		this.managementMapPluginInboundPort.destroyPort();
		this.removeOfferedInterface(ManagementI.class);
		
		this.ReceiveTupleInboundPort.unpublishPort();
		this.ReceiveTupleInboundPort.destroyPort();
		this.removeOfferedInterface(SendTupleServiceI.class);
		
		this.sendTupleobp.unpublishPort();
		this.sendTupleobp.destroyPort();
		this.removeRequiredInterface(SendTupleServiceI.class);
		
		
	}

	/**
	 * Launch the map task to execute foncion_map
	 * @param function Function(Tuple, Tuple),the function to be executed
	 * @param t Tuple of data size,Contains two int's, 
	 * one storing the minimum value Function<Tuple, Tuple>of the generated Tuple's id and one storing the maximum value
	 * @throws Exception exception
	 */
	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
	     
       
        int tupleIdMax=(int) t.getIndiceData(0);
		int tupleIdMin=(int) t.getIndiceData(1);
        
		this.fonction_map=function;
	
		for(int i=tupleIdMin;i<tupleIdMax;i++) {
			OrderedTuple result =(OrderedTuple) bufferSend.take();
			
			this.getOwner().runTask(indexSendExector, map -> {try {
				((SendTupleImplementationI)map).send_Tuple(this.sendTupleobp, result);
			} catch (Exception e) {
				e.printStackTrace();
			}});
			
		
		}
		
		System.out.println("Component map finshed"); 
		return true;
		
		
	}

	/**
	 * receive tuple from component resource and submit the map calculate task 
	 * @param t Tuple containing the data to be computed
	 * @throws Exception exception
	 */
	public void tupleSender(Tuple t) throws Exception {
		
		this.getOwner().runTask(indexCalculExector, map->{	try {
			((createCalculServiceI)map).createMapCalculTask(bufferSend,fonction_map,t);
		} catch (Exception e) {

			e.printStackTrace();
		}});
		

	}

	/**
	 * Launch the connection between Map component and Reduce component to send Tuple
	 * @throws Exception exception
	 */

	@Override
	public void DoPluginPortConnection() throws Exception {
		this.getOwner().doPortConnection(this.sendTupleobp.getPortURI(),sendReduceTupleInboundPortUri, ConnectorSendTuple.class.getCanonicalName());
		
	}

	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t,Nature nature) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	

	@Override
	public boolean runTaskResource(Function<Integer, Tuple> function, Tuple t) throws Exception {
	
		return false;
	}

	
	
}