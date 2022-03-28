package map_reduce_sys.ressource;

import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.plugin.PluginManagementResourceIn;


public class ManagementResourceInboundPortPlugin extends AbstractInboundPort implements EventEmissionCI {

	public ManagementResourceInboundPortPlugin(String uri,String pluginURI,ComponentI owner)
			throws Exception {
		super(uri,ManagementI.class, owner,pluginURI,null);
	}
	
	public ManagementResourceInboundPortPlugin(String pluginURI,ComponentI owner)
			throws Exception {
		super(ManagementI.class, owner,pluginURI,null);
	}
	
	
	
	
	public boolean runTaskResource(Function<Void, Tuple> function, Tuple t) throws Exception {
		System.out.println("begin Gestion resource port");
		this.getOwner().handleRequest(
				new AbstractComponent.AbstractService<Void>(this.getPluginURI()) {
					@Override
					public Void call() throws Exception{
						((PluginManagementResourceIn)this.getServiceProviderReference()).
						 runTaskResource(function, (int) t.getIndiceData(0));
						
						return null;
					}
				});
		System.out.println("fin Gestion resource port");
}
	

	

}
