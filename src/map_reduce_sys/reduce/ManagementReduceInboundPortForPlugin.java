package map_reduce_sys.reduce;


import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.interfaces.BiFunction;
import map_reduce_sys.interfaces.Function;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.plugin.PluginMap;
import map_reduce_sys.plugin.PluginReduce;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;


public class ManagementReduceInboundPortForPlugin extends AbstractInboundPort implements ManagementI {


	private static final long serialVersionUID = 1L;




	public ManagementReduceInboundPortForPlugin(String uri,String pluginURI,ComponentI owner)
			throws Exception {
		super(uri,ManagementI.class, owner,pluginURI,null);
	}
	
	public ManagementReduceInboundPortForPlugin(String pluginURI,ComponentI owner)
			throws Exception {
		super(ManagementI.class, owner,pluginURI,null);
	}
	
	
	
	
	public boolean runTaskResource(Function<Integer, Tuple> function, Tuple t) throws Exception {
		return false;
		//System.out.println("fin Gestion resource port");
}

	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
		return false;
	}

	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t,Nature nature) throws Exception {
	
			System.out.println("begin Gestion reduce port");
		return this.getOwner().handleRequest(
				new AbstractComponent.AbstractService<Boolean>(this.getPluginURI()) {
					@Override
					public Boolean call() throws Exception{
						return ((PluginReduce)this.getServiceProviderReference()).
						 runTaskReduce(function, t,nature);	
					}
				});
	}
	
	@Override
	public void DoPluginPortConnection() throws Exception {
		this.getOwner().handleRequest(
				new AbstractComponent.AbstractService<Void>(this.getPluginURI()) {
					@Override
					public Void call() throws Exception{
						 ((PluginReduce)this.getServiceProviderReference()).
						 DoPluginPortConnection();
						return null;
						
					}
				});
		
	}
	

	

}
