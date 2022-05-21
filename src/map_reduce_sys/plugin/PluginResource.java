package map_reduce_sys.plugin;

import java.util.concurrent.LinkedBlockingQueue;

import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import map_reduce_sys.ManagementInboundPortForPlugin;
import map_reduce_sys.SendTupleOutboundPort;
import map_reduce_sys.connector.ConnectorSendTuple;
import map_reduce_sys.interfaces.BiFunction;
import map_reduce_sys.interfaces.Function;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleImplementationI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.interfaces.createCalculServiceI;
import map_reduce_sys.ressource.ManagementResourceInboundPortForPlugin;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;


public class PluginResource extends AbstractPlugin implements ManagementI{
	private static final long serialVersionUID=1L;
	
	protected ManagementInboundPortForPlugin managementPluginInboundPort;
	protected String ManagementInPortUri; 
	protected int nbThread;
	//protected  int resourceSize;
	protected Function<Integer, Tuple> data_generator;
	protected LinkedBlockingQueue<Tuple> bufferSend;
	protected int indexCalculExector;
	protected int indexSendExector;
	protected SendTupleOutboundPort sendTupleobp;
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
	public void uninstall() throws Exception {
		this.managementPluginInboundPort.unpublishPort();
		this.managementPluginInboundPort.destroyPort();
		this.removeOfferedInterface(ManagementI.class);
	}



	@Override
	public boolean runTaskResource(Function<Integer, Tuple> function, Tuple t) throws Exception {
	  
		int tupleIdMax=(int) t.getIndiceData(0);
		int tupleIdMin=(int) t.getIndiceData(1);
		int currentTupleId=tupleIdMin;
		this.data_generator=function;
		for(int i=tupleIdMin;i<tupleIdMax;i++) {
		
			//calculExecutor.submit(taskCalcul);
			//this.getOwner().handleRequest(indexCalculExector, res -> {	((ComponentRessource)res).createCalculTask(bufferSend,data_generator);
			//return null;});
			int tupleId=currentTupleId;
			this.getOwner().runTask(indexCalculExector, res->{	try {
				((createCalculServiceI)res).createResourceCalculTask(bufferSend,data_generator,tupleId);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}});
			currentTupleId++;
			Tuple result =bufferSend.take();
			
		
			this.getOwner().runTask(indexSendExector, res -> {try {
				((SendTupleImplementationI)res).send_Tuple(this.sendTupleobp, result);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}});
			
			
		}
		//Thread.sleep(100);
	
		//calculExecutor.shutdown();
		//SendExecutor.shutdown();
		System.out.println("Component resource finished" );
		
		
		
		
		return true;
	
	
	
	
	
	}


	@Override
	public void			finalise() throws Exception
	{
		this.getOwner().doPortDisconnection(this.sendTupleobp.getPortURI());
	}
	
	

	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
		return false;
	}



	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t,Nature nature) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}



	@Override
	public void DoPluginPortConnection() throws Exception {
		System.out.println("Res sendTuple Map Inuri:"+sendTupleInPortUri);
		this.getOwner().doPortConnection(this.sendTupleobp.getPortURI(),sendTupleInPortUri, ConnectorSendTuple.class.getCanonicalName());
		
	}






	

	
	
}