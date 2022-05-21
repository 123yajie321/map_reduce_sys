package map_reduce_sys.ports;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.plugin.PluginMap;
import map_reduce_sys.plugin.PluginReduce;
import map_reduce_sys.structure.Tuple;

public class ReceiveTupleWithPluginInboundPort extends AbstractInboundPort implements SendTupleServiceI {

	private static final long serialVersionUID = 1L;

	public ReceiveTupleWithPluginInboundPort(String uri,String pluginURI,ComponentI owner)
			throws Exception {
		super(uri,SendTupleServiceI.class, owner,pluginURI,null);
	}
	
	public ReceiveTupleWithPluginInboundPort(String pluginURI,ComponentI owner)
			throws Exception {
		super(SendTupleServiceI.class, owner,pluginURI,null);
	}

	@Override
	public void tupleSender(Tuple t) throws Exception {
		 this.getOwner().runTask(
				new AbstractComponent.AbstractTask(this.getPluginURI()) {				
					@Override
					public void run() {
						
						if (this.getTaskProviderReference() instanceof PluginMap)
						{
							    try {
									((PluginMap)this.getTaskProviderReference()).tupleSender(t);
								} catch (Exception e) {
								
									e.printStackTrace();
								}
							
						}
						else  {
							 try {
								((PluginReduce)this.getTaskProviderReference()).tupleSender(t);
							} catch (Exception e) {
							
								e.printStackTrace();
							}
						
						}
						
					}
				});
	}

}
