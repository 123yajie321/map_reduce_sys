package map_reduce_sys.plugin;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
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
import map_reduce_sys.reduce.ManagementReduceInboundPortPlugin;
import map_reduce_sys.ressource.ComponentRessource;
import map_reduce_sys.ressource.ManagementResourceInboundPortPlugin;
import map_reduce_sys.ressource.RessourceSendMapOutboundPort;


public class PluginReduce extends AbstractPlugin implements ManagementI,SendTupleServiceI{
	private static final long serialVersionUID=1L;
	
	//Offere the servive runTaskReduce
	protected ManagementReduceInboundPortPlugin managementReducePluginInboundPort;
	protected String ManagementInPortUri; 
	protected int nbThread;
	protected  int dataSize;
	protected BiFunction<Tuple,Tuple, Tuple> fonction_reduce;
	protected PriorityBlockingQueue<Tuple>  bufferReceive;
	protected int indexCalculExector;
	//protected int indexSendExector;
	//send tuple to component reduce
	//protected SendTupleOutboundPort sendTupleobp;
	
	//recevive the tuple from component map
	protected SendTupleInboundPort sendTupleInboundPort;
	//uri of the inboundPort to receive tuple 
	protected String sendTupleInPortUri;
	
	
	//protected String sendReduceTupleInboundPortUri;

	

	
	public PluginReduce(String uri,int nb,BiFunction<Tuple,Tuple, Tuple> fonction_reduce,String inboundPortReceiveTupleuri) {
		super();
		this.ManagementInPortUri=uri;
		this.sendTupleInPortUri=inboundPortReceiveTupleuri;
		this.nbThread=nb;
		this.fonction_reduce=fonction_reduce;
		bufferReceive=new PriorityBlockingQueue<Tuple>();
	}
	
	
	
	@Override
	public void	installOn(ComponentI owner) throws Exception{
		super.installOn(owner);

		/*this.addRequiredInterface(SendTupleServiceI.class);
		this.sendTupleobp=new SendTupleOutboundPort(this.getOwner());
		this.sendTupleobp.publishPort();*/
		
		
		
	}
	
	@Override
	public void initialise() throws Exception{
		//this.addRequiredInterface(ReflectionCI.class);
		//ReflectionOutboundPort  rop= new ReflectionOutboundPort(this.getOwner());
		super.initialise();
		
		//connecte with the component reduce to send Tuple
		
		this.addOfferedInterface(ManagementI.class);
		this.managementReducePluginInboundPort = new ManagementReduceInboundPortPlugin(ManagementInPortUri,this.getPluginURI(),this.getOwner());
		this.managementReducePluginInboundPort.publishPort();
		
		this.addOfferedInterface(SendTupleServiceI.class);
		this.sendTupleInboundPort=new SendTupleInboundPort(sendTupleInPortUri,this.getPluginURI(),this.getOwner() );
		this.sendTupleInboundPort.publishPort();
		
		//this.getOwner().doPortConnection(this.sendTupleobp.getPortURI(),sendReduceTupleInboundPortUri, ConnectorSendTuple.class.getCanonicalName());
		
		indexCalculExector=createNewExecutorService("ReduceCalculexector_uri", nbThread,false);
		//indexSendExector=createNewExecutorService("MapSendexector_uri", nbThread,false);
		
}
	
	@Override
	public void			finalise() throws Exception
	{
		//this.getOwner().doPortDisconnection(this.sendTupleobp.getPortURI());
	}
	
	@Override
	public void uninstall() throws Exception {
		this.managementReducePluginInboundPort.unpublishPort();
		this.managementReducePluginInboundPort.destroyPort();
		this.removeOfferedInterface(ManagementI.class);
		
		this.sendTupleInboundPort.unpublishPort();
		this.sendTupleInboundPort.destroyPort();
		this.removeOfferedInterface(SendTupleServiceI.class);
		
		//this.sendTupleobp.unpublishPort();
		//this.sendTupleobp.destroyPort();
		//this.removeRequiredInterface(SendTupleServiceI.class);
		
		
	}



	@Override
	public boolean runTaskResource(Function<Void, Tuple> function, Tuple t) throws Exception {
	
		return false;
	}



	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
		return false;
		
	}



	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t) throws Exception {

        this.dataSize=(int) t.getIndiceData(0);
		this.fonction_reduce=function;
		for(int i=0;i<dataSize-1;i++) {
			OrderedTuple t1=(OrderedTuple) bufferReceive.take();
			OrderedTuple t2=(OrderedTuple) bufferReceive.take();
			
			this.getOwner().runTask(indexCalculExector,reduce -> {try {
				((ComponentCalcul)reduce).createReduceCalculTask(bufferReceive, function, t1, t2);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}});
			
			
			
		}
			Tuple finalResult = bufferReceive.take();
			int result = (int) finalResult.getIndiceData(0);
			
			System.out.println("final result is :  " + result);
			System.out.println("Component Reduce finished" );
		return true;
	}


	//receive tuple from component map and put the tuple to bufferReceive
	@Override
	public boolean tupleSender(Tuple t) throws Exception {
		bufferReceive.put(t);
		System.out.println("Component reduce receive  :" + t.getIndiceData(0));
		return true;
		
		
	}



	

	
	
}