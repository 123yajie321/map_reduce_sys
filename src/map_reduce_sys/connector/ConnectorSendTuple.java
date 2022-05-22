package map_reduce_sys.connector;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import map_reduce_sys.interfaces.SendTupleServiceCI;
import map_reduce_sys.structure.Tuple;

/**
 * The class <code>ConnectorSendTuple</code> implements a connector for the
 * <code>SendTupleServiceCI</code> component interface.
 * @author	Yajie LIU, Zimeng ZHANG
 */


public class ConnectorSendTuple extends AbstractConnector implements SendTupleServiceCI {

	@Override
	public void tupleSender(Tuple t) throws Exception {
		 ((SendTupleServiceCI)this.offering).tupleSender(t);
	}

}
