package map_reduce_sys.connector;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.RecieveTupleServiceI;
import map_reduce_sys.interfaces.SendTupleServiceI;

public class ConnectorMapGestion extends AbstractConnector implements SendTupleServiceI,RecieveTupleServiceI{



	@Override
	public boolean tupleSender(Tuple t) throws Exception {
		return ((RecieveTupleServiceI)this.offering).tupleReciever(t);
	}
	
	public boolean tupleReciever(Tuple t) throws Exception {
		return ((SendTupleServiceI)this.offering).tupleSender(t);
		
	}
	
	

}
