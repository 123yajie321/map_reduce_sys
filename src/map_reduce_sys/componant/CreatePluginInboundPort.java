package map_reduce_sys.componant;

import java.util.function.BiFunction;
import java.util.function.Function;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.gestion.ComponentGestion;
import map_reduce_sys.interfaces.createPluginI;
import map_reduce_sys.structure.Tuple;

public class CreatePluginInboundPort extends AbstractInboundPort implements createPluginI {

	private static final long serialVersionUID=1L;

	public CreatePluginInboundPort(ComponentI owner) throws Exception {
		super(createPluginI.class, owner);
		assert owner instanceof ComponentGestion;

	}

	
	
	public CreatePluginInboundPort(String uri,ComponentI owner) throws Exception {
		super(uri, createPluginI.class,owner);
	}



	@Override
	public void createPluginResource(String managementResourceInboundPort, int nb,
			Function<Integer, Tuple> data_generator, String mapReceiveTupleinboundPorturi, int pluginId)
			throws Exception {
	
		
		this.getOwner().runTask( c-> {
				try {
					((ComponentCalcul)c).createPluginResource(managementResourceInboundPort, nb, data_generator, mapReceiveTupleinboundPorturi, pluginId);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		});		
		
		
	}



	@Override
	public void createPluginMap(String managementMapInboundPort, int nb, Function<Tuple, Tuple> fonction_map,
			String mapReceiveTupleinboundPorturi, String ReduceReceiveTupleinboundPortUri, int pluginId)
			throws Exception {
		
		this.getOwner().runTask( c-> {
				try {
					((ComponentCalcul)c).createPluginMap(managementMapInboundPort, nb, fonction_map, mapReceiveTupleinboundPorturi, ReduceReceiveTupleinboundPortUri, pluginId);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		});		
		
	}



	@Override
	public void createPluginReduce(String managementReduceInboundPort, int nb,
			BiFunction<Tuple, Tuple, Tuple> fonction_reduce, String ReduceReceiveTupleinboundPorturi,
			String sendResultinboundPortUri, int pluginId) throws Exception {
		
		this.getOwner().runTask( c-> {
				try {
					((ComponentCalcul)c).createPluginReduce(managementReduceInboundPort, nb, fonction_reduce, ReduceReceiveTupleinboundPorturi, sendResultinboundPortUri, pluginId);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		});		
		
	}






	
	

	
}
