package map_reduce_sys;

import fr.sorbonne_u.components.connectors.AbstractConnector;

public class ConnectorMapGestion extends AbstractConnector implements SendTupleServiceI{



	@Override
	public Tuple tupleSender() throws Exception {
		return ((RecieveTupleServiceI)this.offering).tupleReciever(t)();
	}

}
