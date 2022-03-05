package map_reduce_sys.interfaces;

import java.util.function.BiFunction;
import java.util.function.Function;

import map_reduce_sys.SimpleJob;

public interface ManagementI {
	
	public <T,R> boolean SetFunction(Function<T, R> f);
	public <T,T2,R> boolean SetBiFunction(BiFunction<T,T2,R> f);
	public void SubmitJob(SimpleJob job);
	
}
