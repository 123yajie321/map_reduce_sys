package map_reduce_sys.connector;


import fr.sorbonne_u.components.connectors.AbstractConnector;
import map_reduce_sys.interfaces.BiFunction;
import map_reduce_sys.interfaces.Function;
import map_reduce_sys.interfaces.ManagementCI;
import map_reduce_sys.interfaces.SendTupleServiceCI;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;

/**
 * The class <code>ConnectorGestion</code> implements a connector for the
 * <code>ManagementCI</code> component interface.
 * @author	Yajie LIU, Zimeng ZHANG
 */

public class ConnectorGestion extends AbstractConnector implements ManagementCI{

	

	@Override
	public boolean runTaskResource(Function<Integer, Tuple> function, Tuple t) throws Exception {
		 return ((ManagementCI)this.offering).runTaskResource(function,t);
	}

	@Override
	public boolean runTaskMap(Function<Tuple, Tuple> function, Tuple t) throws Exception {
		return ((ManagementCI)this.offering).runTaskMap(function,t);
	
	}

	@Override
	public boolean runTaskReduce(BiFunction<Tuple, Tuple, Tuple> function, Tuple t,Nature nature) throws Exception {
		return ((ManagementCI)this.offering).runTaskReduce(function, t,nature);
	}

	@Override
	public void DoPluginPortConnection() throws Exception {
		((ManagementCI)this.offering).DoPluginPortConnection();
		
	}


}
