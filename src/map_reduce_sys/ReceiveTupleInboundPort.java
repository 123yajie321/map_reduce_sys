package map_reduce_sys;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import map_reduce_sys.gestion.ComponentGestion;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.plugin.PluginMap;
import map_reduce_sys.plugin.PluginReduce;
import map_reduce_sys.structure.Tuple;

public class ReceiveTupleInboundPort extends AbstractInboundPort implements SendTupleServiceI {

	

	public ReceiveTupleInboundPort(String uri,ComponentI owner)
			throws Exception {
		super(uri,SendTupleServiceI.class, owner);
	}
	
	public ReceiveTupleInboundPort(ComponentI owner)
			throws Exception {
		super(SendTupleServiceI.class, owner);
	}

	@Override
	public boolean tupleSender(Tuple t) throws Exception {
		return this.getOwner().handleRequest(m->((ComponentGestion)m).recieve_Tuple(t));
	}

}
