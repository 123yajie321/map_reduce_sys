package map_reduce_sys.connector;

import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.ManagementI;
import map_reduce_sys.interfaces.RecieveTupleServiceI;
import map_reduce_sys.interfaces.SendTupleServiceI;

public class ConnectorResourceGestion extends AbstractConnector implements ManagementI{


	@Override
	public boolean runTaskResource(Function<Void, Tuple> function, Tuple t) throws Exception {
		 return ((ManagementI)this.offering).runTaskResource(function,t);
	}

	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}



	
	

}
