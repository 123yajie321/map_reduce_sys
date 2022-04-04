package map_reduce_sys.plugin;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent.ExecutorServiceFactory;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.ressource.ManagementResourceInboundPortPlugin;


public class PluginManagementResourceIn1 extends AbstractPlugin implements ManagementI{
	private static final long serialVersionUID=1L;
	
	protected ManagementResourceInboundPortPlugin managementPluginInboundPort;
	protected String PortUri; 
	protected int nbThread;
	
	public PluginManagementResourceIn1(String uri,int nb) {
		super();
		this.PortUri=uri;
		this.nbThread=nb;
		
	}
	
	
	
	@Override
	public void	installOn(ComponentI owner) throws Exception{
		super.installOn(owner);
	}
	
	@Override
	public void initialise() throws Exception{
		//this.addRequiredInterface(ReflectionCI.class);
		//ReflectionOutboundPort  rop= new ReflectionOutboundPort(this.getOwner());
		super.initialise();
		this.addOfferedInterface(ManagementResourceInboundPortPlugin.class);
		this.managementPluginInboundPort = new ManagementResourceInboundPortPlugin(PortUri,this.getPluginURI(),this.getOwner());
		this.managementPluginInboundPort.publishPort();
		
		int index=createNewExecutorService("Calculexector_uri", nbThread,false);
		int index2=createNewExecutorService("Sendexector_uri", nbThread,false);
		
}
	@Override
	public void uninstall() throws Exception {
		this.managementPluginInboundPort.unpublishPort();
		this.managementPluginInboundPort.destroyPort();
		this.removeRequiredInterface(ManagementI.class);
	}



	@Override
	public boolean runTaskResource(Function<Void, Tuple> function, Tuple t) throws Exception {
		return ((ManagementI)this.getOwner()).runTaskResource(function, t);
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