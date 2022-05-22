package map_reduce_sys.interfaces;

import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import map_reduce_sys.structure.Tuple;
/**
 * The interface <code>SendTupleServiceCI</code> declares the signatures of the
 * implementation methods for transferring tuple services.
 *
 * @author Yajie LIU, Zimeng ZHANG
 */

public interface SendTupleServiceCI extends RequiredCI,OfferedCI {
	
	public void tupleSender(Tuple t)throws Exception;
	

}
