package map_reduce_sys.plugin;

import java.util.concurrent.LinkedBlockingQueue;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import map_reduce_sys.ManagementInboundPortForPlugin;
import map_reduce_sys.ReceiveTupleWithPluginInboundPort;
import map_reduce_sys.SendTupleOutboundPort;
import map_reduce_sys.connector.ConnectorSendTuple;
import map_reduce_sys.interfaces.BiFunction;
import map_reduce_sys.interfaces.Function;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleImplementationI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.interfaces.createCalculServiceI;
import map_reduce_sys.map.ManagementMapInboundPortForPlugin;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.OrderedTuple;
import map_reduce_sys.structure.Tuple;


public class PluginMap extends AbstractPlugin implements ManagementI,SendTupleServiceI{
	private static final long serialVersionUID=1L;
	
	//Offere the servive runTaskMap
	protected ManagementInboundPortForPlugin managementMapPluginInboundPort;
	protected String ManagementInPortUri; 
	protected int nbThread;
	protected  int dataSize;
	protected Function<Tuple, Tuple> fonction_map;
	protected LinkedBlockingQueue<Tuple> bufferSend;
	protected int indexCalculExector;
	protected int indexSendExector;
	//send tuple to component reduce
	protected SendTupleOutboundPort sendTupleobp;
	
	//recevive the tuple from component resource
	protected ReceiveTupleWithPluginInboundPort ReceiveTupleInboundPort;
	protected String receiveTupleInPortUri;
	
	//uri of the inboundPort of Component reduce
	// used to do the connection
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
		//this.addRequiredInterface(ReflectionCI.class);
		//ReflectionOutboundPort  rop= new ReflectionOutboundPort(this.getOwner());
		super.initialise();
		
		//connecte with the component reduce to send Tuple
		
		this.addOfferedInterface(ManagementI.class);
		this.managementMapPluginInboundPort = new ManagementInboundPortForPlugin(ManagementInPortUri,this.getPluginURI(),this.getOwner());
		this.managementMapPluginInboundPort.publishPort();
		System.out.println("map man inbound port created "+ManagementInPortUri);
		
		this.addOfferedInterface(SendTupleServiceI.class);
		this.ReceiveTupleInboundPort=new ReceiveTupleWithPluginInboundPort(receiveTupleInPortUri,this.getPluginURI(),this.getOwner() );
		this.ReceiveTupleInboundPort.publishPort();
		System.out.println("map receive inbound port created "+receiveTupleInPortUri);
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



	@Override
	public boolean runTaskResource(Function<Integer, Tuple> function, Tuple t) throws Exception {
	
		return false;
	}



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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}});
			
		
		}
		
		System.out.println("Component map finshed"); 
		return true;
		
		
	}



	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t,Nature nature) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}


	//receive tuple from component resource and submit the task to component map
	@Override
	public void tupleSender(Tuple t) throws Exception {
		
		this.getOwner().runTask(indexCalculExector, map->{	try {
			((createCalculServiceI)map).createMapCalculTask(bufferSend,fonction_map,t);
		} catch (Exception e) {

			e.printStackTrace();
		}});
		

	}



	@Override
	public void DoPluginPortConnection() throws Exception {
		System.out.println("Map send   inport"+sendReduceTupleInboundPortUri);
		this.getOwner().doPortConnection(this.sendTupleobp.getPortURI(),sendReduceTupleInboundPortUri, ConnectorSendTuple.class.getCanonicalName());
		
	}



	

	
	
}