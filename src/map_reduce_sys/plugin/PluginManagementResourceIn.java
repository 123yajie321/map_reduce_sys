package map_reduce_sys.plugin;

import java.util.concurrent.LinkedBlockingQueue;

import fr.sorbonne_u.components.AbstractComponent.ExecutorServiceFactory;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import map_reduce_sys.Tuple;
import map_reduce_sys.ressource.ManagementResourceInboundPortPlugin;


public class PluginManagementResourceIn extends AbstractPlugin implements EventEmissionCI{
	private static final long serialVersionUID=1L;
	
	protected ManagementResourceInboundPortPlugin managementPluginInboundPort;
	protected String PortUri; 
	protected int nbThread;
	
	public PluginManagementResourceIn(String uri,int nb) {
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
		
		createNewExecutorService("plugin_resource-uri", nbThread);
	
}
	@Override
	public void uninstall() throws Exception {
		this.receivePluginInboundPort.unpublishPort();
		this.receivePluginInboundPort.destroyPort();
		this.removeRequiredInterface(CepEventRecieveInboundPort.class);
	}
	
	@Override
	public void sendEvent(String emitterURI, EventI event) throws Exception {
		System.out.println("begin Send PLugin in");
		/*
		this.getOwner().handleRequest(
				cep -> {	((CEPBus)cep).
									recieveEvent(emitterURI, event);;
						return null;
					 });
					 */
		((EventEmissionCI)this.getOwner()).sendEvent(emitterURI, event);;
		System.out.println("fin Send PLugin in");
	}

	@Override
	public void sendEvents(String emitterURI, EventI[] events) {
		

	}
	
}