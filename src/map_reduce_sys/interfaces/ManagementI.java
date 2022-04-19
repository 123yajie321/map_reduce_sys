package map_reduce_sys.interfaces;

import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.interfaces.RequiredCI;
import map_reduce_sys.job.SimpleJob;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.Tuple;

public interface ManagementI  extends OfferedCI,RequiredCI{
	/*
	public <T,R> boolean SetFunction(Function<T, R> f);
	public <T,T2,R> boolean SetBiFunction(BiFunction<T,T2,R> f);
	public void SubmitJob(SimpleJob job);*/
	
	public boolean runTaskResource(Function<Void, Tuple>function,Tuple t)throws Exception;
	public boolean runTaskMap(Function<Tuple, Tuple>function,Tuple t)throws Exception;
	public boolean runTaskReduce(BiFunction<Tuple,Tuple, Tuple>function,Tuple t,Nature nature) throws Exception;
	
}
