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
import map_reduce_sys.ports.SendTupleOutboundPort;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;

/**
 * The class <code>PluginResource</code> implements the calculate component side  plug-in
 * for the <code>ManagementI</code> component interface and it implements also the the client side plug-in
 * for the <code>SendTupleServiceI</code> component interface
 * @author Yajie LIU, Zimeng ZHANG
 */

public class PluginResource extends AbstractPlugin implements ManagementI{
	private static final long serialVersionUID=1L;
	/**Inbound port for management, Offer the service runTaskResource*/
	protected ManagementInboundPortForPlugin managementPluginInboundPort;
	/**uri of inbound port for management*/
	protected String ManagementInPortUri; 
	/**The number of threads in the thread pool used to perform the calculation  */
	protected int nbThread;
	/**Function used to generate data*/
	protected Function<Integer, Tuple> data_generator;
	/**A buffer to store the generated tuples to be sent*/
	protected LinkedBlockingQueue<Tuple> bufferSend;
	/**Thread pools used for the execution of function data_generator*/
	protected int indexCalculExector;
	/**Thread pools used to send tuple*/
	protected int indexSendExector;
	/**outbound port to send tuple to MAP component*/
	protected SendTupleOutboundPort sendTupleobp;
	/** The uri of inboundPort of the sendTuple destination*/
	protected String sendTupleInPortUri;

	

	
	public PluginResource(String uri,int nb,Function<Integer, Tuple> data_generator,String sendTupleInPortUri) {
		super();
		this.ManagementInPortUri=uri;
		this.sendTupleInPortUri=sendTupleInPortUri;
		this.nbThread=nb;
		this.data_generator=data_generator;
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
		this.managementPluginInboundPort = new ManagementInboundPortForPlugin(ManagementInPortUri,this.getPluginURI(),this.getOwner());
		this.managementPluginInboundPort.publishPort();
		
		indexCalculExector=createNewExecutorService("RessourceCalculexector_uri", nbThread,false);
		indexSendExector=createNewExecutorService("RessourceSendexector_uri", nbThread,false);
		
	}
	

	@Override
	public void			finalise() throws Exception
	{
		this.getOwner().doPortDisconnection(this.sendTupleobp.getPortURI());
	}
	
	@Override
	public void uninstall() throws Exception {
		this.managementPluginInboundPort.unpublishPort();
		this.managementPluginInboundPort.destroyPort();
		this.removeOfferedInterface(ManagementI.class);
		
		this.sendTupleobp.unpublishPort();
		this.sendTupleobp.destroyPort();
		this.removeRequiredInterface(SendTupleServiceI.class);
		
		
	}
	
	/**
	 * Launch the resource task to generate data
	 * @param function Function(Integer, Tuple),the function to be executed
	 * @param t Tuple of data size,Contains two int's, 
	 * one storing the minimum value of the generated Tuple's id and one storing the maximum value
	 * @throws Exception exception
	 */

	@Override
	public boolean runTaskResource(Function<Integer, Tuple> function, Tuple t) throws Exception {
	  
		int tupleIdMax=(int) t.getIndiceData(0);
		int tupleIdMin=(int) t.getIndiceData(1);
		int currentTupleId=tupleIdMin;
		this.data_generator=function;
		for(int i=tupleIdMin;i<tupleIdMax;i++) {
		
			int tupleId=currentTupleId;
			this.getOwner().runTask(indexCalculExector, res->{	try {
				((createCalculServiceI)res).createResourceCalculTask(bufferSend,data_generator,tupleId);
			} catch (Exception e) {
				e.printStackTrace();
			}});
			currentTupleId++;
			
			Tuple result =bufferSend.take();
			this.getOwner().runTask(indexSendExector, res -> {try {
				((SendTupleImplementationI)res).send_Tuple(this.sendTupleobp, result);
			} catch (Exception e) {
				e.printStackTrace();
			}});
			
			
		}
		
		System.out.println("Component resource finished" );
		
		
		return true;
	
	}
	
	/**
	 * Launch the connection between Resource component and Map component to send Tuple
	 * @throws Exception exception
	 */

	@Override
	public void DoPluginPortConnection() throws Exception {
		this.getOwner().doPortConnection(this.sendTupleobp.getPortURI(),sendTupleInPortUri, ConnectorSendTuple.class.getCanonicalName());
		
	}

	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
		return false;
	}



	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t,Nature nature) throws Exception {
		return false;
	}



	






	

	
	
}