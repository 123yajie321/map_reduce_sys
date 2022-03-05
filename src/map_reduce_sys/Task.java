package map_reduce_sys;


import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

public class Task implements Runnable {
	
	Function<Tuple, Tuple> f;
	Tuple t;
	ThreadPoolExecutor executor;

	public Task(Function<Tuple, Tuple> fonction_map, Tuple t,ThreadPoolExecutor executor) {
		this.f=fonction_map;
		this.t=t;
		this.executor=executor;
	}


	@Override
	public void run() {
		f.apply(t);
		
		
	}

}
