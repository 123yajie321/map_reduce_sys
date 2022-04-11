package map_reduce_sys.interfaces;

import java.util.concurrent.BlockingQueue;
import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import map_reduce_sys.structure.Tuple;

public interface createCalculServiceI  {
	public void createResourceCalculTask(BlockingQueue<Tuple>bufferSend,Function<Void, Tuple> function) throws Exception;
	public void createMapCalculTask(BlockingQueue<Tuple>bufferSend,Function<Tuple, Tuple> fonction_map,Tuple t)throws Exception;
	public void createReduceCalculTask(BlockingQueue<Tuple>bufferReceive,BiFunction<Tuple,Tuple, Tuple> fonction_reduce,Tuple t1,Tuple t2)throws Exception;
}
