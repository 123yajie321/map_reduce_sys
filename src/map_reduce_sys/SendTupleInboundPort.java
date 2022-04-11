package map_reduce_sys;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.plugin.PluginMap;
import map_reduce_sys.plugin.PluginReduce;
import map_reduce_sys.structure.Tuple;

public class SendTupleInboundPort extends AbstractInboundPort implements SendTupleServiceI {

	

	public SendTupleInboundPort(String uri,String pluginURI,ComponentI owner)
			throws Exception {
		super(uri,ManagementI.class, owner,pluginURI,null);
	}
	
	public SendTupleInboundPort(String pluginURI,ComponentI owner)
			throws Exception {
		super(ManagementI.class, owner,pluginURI,null);
	}

	@Override
	public boolean tupleSender(Tuple t) throws Exception {
		return this.getOwner().handleRequest(
				new AbstractComponent.AbstractService<Boolean>(this.getPluginURI()) {
					@Override
					public Boolean call() throws Exception{
						
						if (this.getServiceProviderReference() instanceof PluginMap)
						{
							return ((PluginMap)this.getServiceProviderReference()).tupleSender(t);
							
						}
						else {
							return ((PluginReduce)this.getServiceProviderReference()).tupleSender(t);
						
						}
						
						
					}
				});
	}

}
