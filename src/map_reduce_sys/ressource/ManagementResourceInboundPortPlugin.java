package map_reduce_sys.ressource;

import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.plugin.PluginManagementResourceIn1;
import map_reduce_sys.plugin.PluginResource;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;


public class ManagementResourceInboundPortPlugin extends AbstractInboundPort implements ManagementI {


	private static final long serialVersionUID = 1L;




	public ManagementResourceInboundPortPlugin(String uri,String pluginURI,ComponentI owner)
			throws Exception {
		super(uri,ManagementI.class, owner,pluginURI,null);
	}
	
	public ManagementResourceInboundPortPlugin(String pluginURI,ComponentI owner)
			throws Exception {
		super(ManagementI.class, owner,pluginURI,null);
	}
	
	
	
	
	public boolean runTaskResource(Function<Integer, Tuple> function, Tuple t) throws Exception {
		System.out.println("begin Gestion resource port");
		return this.getOwner().handleRequest(
				new AbstractComponent.AbstractService<Boolean>(this.getPluginURI()) {
					@Override
					public Boolean call() throws Exception{
						return ((PluginResource)this.getServiceProviderReference()).
						 runTaskResource(function, t);
						
						
					}
				});
		//System.out.println("fin Gestion resource port");
}

	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t,Nature nature) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}
	

	

}
