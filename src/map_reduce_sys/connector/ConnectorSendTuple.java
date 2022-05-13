package map_reduce_sys.connector;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import map_reduce_sys.interfaces.RecieveTupleServiceI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.structure.Tuple;

public class ConnectorSendTuple extends AbstractConnector implements SendTupleServiceI {

	@Override
	public void tupleSender(Tuple t) throws Exception {
		 ((SendTupleServiceI)this.offering).tupleSender(t);
	}

}
