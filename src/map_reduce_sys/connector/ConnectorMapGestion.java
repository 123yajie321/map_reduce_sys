package map_reduce_sys.connector;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.RecieveTupleServiceI;
import map_reduce_sys.interfaces.SendTupleServiceI;

public class ConnectorMapGestion extends AbstractConnector implements SendTupleServiceI,RecieveTupleServiceI{



	@Override
	public void tupleSender(Tuple t) throws Exception {
		((RecieveTupleServiceI)this.offering).tupleReciever(t);
	}
	
	public void tupleReciever(Tuple t) throws Exception {
		((SendTupleServiceI)this.offering).tupleSender(t);
	}
	
	

}
