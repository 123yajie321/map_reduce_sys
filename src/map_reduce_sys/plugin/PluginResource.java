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
import map_reduce_sys.SendTupleOutboundPort;
import map_reduce_sys.Tuple;
import map_reduce_sys.connector.ConnectorSendTuple;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.ressource.ComponentRessource;
import map_reduce_sys.ressource.ManagementResourceInboundPortPlugin;
import map_reduce_sys.ressource.RessourceSendMapOutboundPort;


public class PluginResource extends AbstractPlugin implements ManagementI{
	private static final long serialVersionUID=1L;
	
	protected ManagementResourceInboundPortPlugin managementPluginInboundPort;
	protected String ManagementInPortUri; 
	protected int nbThread;
	protected  int resourceSize;
	protected Function<Void, Tuple> data_generator;
	protected LinkedBlockingQueue<Tuple> bufferSend;
	protected int indexCalculExector;
	protected int indexSendExector;
	protected SendTupleOutboundPort sendTupleobp;
	protected String sendTupleInPortUri;

	

	
	public PluginResource(String uri,int nb,Function<Void, Tuple> data_generator,String sendTupleInPortUri) {
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
		System.out.println("pluginREs install");
		this.addRequiredInterface(SendTupleServiceI.class);
		this.sendTupleobp=new SendTupleOutboundPort(this.getOwner());
		this.sendTupleobp.publishPort();
		
	}
	
	@Override
	public void initialise() throws Exception{
		//this.addRequiredInterface(ReflectionCI.class);
		//ReflectionOutboundPort  rop= new ReflectionOutboundPort(this.getOwner());
		super.initialise();
		
		this.getOwner().doPortConnection(this.sendTupleobp.getPortURI(),sendTupleInPortUri, ConnectorSendTuple.class.getCanonicalName());
		this.addOfferedInterface(ManagementI.class);
		this.managementPluginInboundPort = new ManagementResourceInboundPortPlugin(ManagementInPortUri,this.getPluginURI(),this.getOwner());
		this.managementPluginInboundPort.publishPort();
		
		
		
		indexCalculExector=createNewExecutorService("Calculexector_uri", nbThread,false);
		indexSendExector=createNewExecutorService("Sendexector_uri", nbThread,false);
		
}
	@Override
	public void uninstall() throws Exception {
		this.managementPluginInboundPort.unpublishPort();
		this.managementPluginInboundPort.destroyPort();
		this.removeRequiredInterface(ManagementI.class);
	}



	@Override
	public boolean runTaskResource(Function<Void, Tuple> function, Tuple t) throws Exception {
		//return ((ManagementI)this.getOwner()).runTaskResource(function, t);
	
		this.resourceSize=(int) t.getIndiceData(0);
		this.data_generator=function;
		for(int i=0;i<resourceSize;i++) {
		
			//calculExecutor.submit(taskCalcul);
			//this.getOwner().handleRequest(indexCalculExector, res -> {	((ComponentRessource)res).createCalculTask(bufferSend,data_generator);
			//return null;});
			this.getOwner().runTask(indexCalculExector, res->{	((ComponentCalcul)res).createResourceCalculTask(bufferSend,data_generator);});
			Tuple result =bufferSend.take();
			
			
			this.getOwner().runTask(indexSendExector, res -> {try {
				((ComponentRessource)res).send_Tuple(this.sendTupleobp, result);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}});
			
			
		}
		//Thread.sleep(100);
	
		/*Tuple fin= new Tuple(1); 
		fin.setIndiceTuple(0, true); 
		
		Runnable task=()->{
			try {
				send_Tuple(fin);
				System.out.println("Component Ressource Send ressource :"+fin.getIndiceData( 0)); 
			} catch (Exception e) {
				e.printStackTrace();
				
			}
		};
		SendExecutor.submit(task);*/
		
	
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
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}



	

	
	
}