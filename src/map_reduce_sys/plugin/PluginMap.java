package map_reduce_sys.plugin;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent.ExecutorServiceFactory;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import map_reduce_sys.ComponentCalcul;
import map_reduce_sys.OrderedTuple;
import map_reduce_sys.SendTupleInboundPort;
import map_reduce_sys.SendTupleOutboundPort;
import map_reduce_sys.Tuple;
import map_reduce_sys.connector.ConnectorSendTuple;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.map.ManagementMapInboundPortPlugin;
import map_reduce_sys.ressource.ComponentRessource;
import map_reduce_sys.ressource.ManagementResourceInboundPortPlugin;
import map_reduce_sys.ressource.RessourceSendMapOutboundPort;


public class PluginMap extends AbstractPlugin implements ManagementI,SendTupleServiceI{
	private static final long serialVersionUID=1L;
	
	//Offere the servive runTaskMap
	protected ManagementMapInboundPortPlugin managementMapPluginInboundPort;
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
	protected SendTupleInboundPort sendTupleInboundPort;
	protected String sendTupleInPortUri;
	
	//uri of the inboundPort of Component reduce
	// used to do the connection
	protected String sendReduceTupleInboundPortUri;

	

	
	public PluginMap(String uri,int nb,Function<Tuple, Tuple> fonction_map,String inboundPortReceiveTupleuri,String inboundPortSendReduceUri) {
		super();
		this.ManagementInPortUri=uri;
		this.sendTupleInPortUri=inboundPortReceiveTupleuri;
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
		this.managementMapPluginInboundPort = new ManagementMapInboundPortPlugin(ManagementInPortUri,this.getPluginURI(),this.getOwner());
		this.managementMapPluginInboundPort.publishPort();
		System.out.println("map inbound port created "+ManagementInPortUri);
		
		this.addOfferedInterface(SendTupleServiceI.class);
		this.sendTupleInboundPort=new SendTupleInboundPort(sendTupleInPortUri,this.getPluginURI(),this.getOwner() );
		this.sendTupleInboundPort.publishPort();
		
		this.getOwner().doPortConnection(this.sendTupleobp.getPortURI(),sendReduceTupleInboundPortUri, ConnectorSendTuple.class.getCanonicalName());
		
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
		
		this.sendTupleInboundPort.unpublishPort();
		this.sendTupleInboundPort.destroyPort();
		this.removeOfferedInterface(SendTupleServiceI.class);
		
		this.sendTupleobp.unpublishPort();
		this.sendTupleobp.destroyPort();
		this.removeRequiredInterface(SendTupleServiceI.class);
		
		
	}



	@Override
	public boolean runTaskResource(Function<Void, Tuple> function, Tuple t) throws Exception {
	
		return false;
	}



	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
	
        this.dataSize=(int) t.getIndiceData(0);
		this.fonction_map=function;
	
		for(int i=0;i<dataSize;i++) {
			OrderedTuple result =(OrderedTuple) bufferSend.take();
			
			this.getOwner().runTask(indexSendExector, map -> {try {
				((ComponentCalcul)map).send_Tuple(this.sendTupleobp, result);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}});
			
		
		}
		
		System.out.println("Component map finshed"); 
		return true;
		
		
	}



	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}


	//receive tuple from component resource and submit the task to component map
	@Override
	public boolean tupleSender(Tuple t) throws Exception {
		// TODO Auto-generated method stub
		this.getOwner().runTask(indexCalculExector, map->{	((ComponentCalcul)map).createMapCalculTask(bufferSend,fonction_map,t);});
		
		return true;
	}



	

	
	
}