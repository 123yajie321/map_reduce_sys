package map_reduce_sys.reduce;

import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.plugin.PluginManagementResourceIn1;
import map_reduce_sys.plugin.PluginMap;
import map_reduce_sys.plugin.PluginReduce;


public class ManagementReduceInboundPortPlugin extends AbstractInboundPort implements ManagementI {


	private static final long serialVersionUID = 1L;




	public ManagementReduceInboundPortPlugin(String uri,String pluginURI,ComponentI owner)
			throws Exception {
		super(uri,ManagementI.class, owner,pluginURI,null);
	}
	
	public ManagementReduceInboundPortPlugin(String pluginURI,ComponentI owner)
			throws Exception {
		super(ManagementI.class, owner,pluginURI,null);
	}
	
	
	
	
	public boolean runTaskResource(Function<Void, Tuple> function, Tuple t) throws Exception {
		return false;
		//System.out.println("fin Gestion resource port");
}

	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
		return false;
	}

	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t) throws Exception {
	
			System.out.println("begin Gestion reduce port");
		return this.getOwner().handleRequest(
				new AbstractComponent.AbstractService<Boolean>(this.getPluginURI()) {
					@Override
					public Boolean call() throws Exception{
						return ((PluginReduce)this.getServiceProviderReference()).
						 runTaskReduce(function, t);	
					}
				});
	}
	

	

}
