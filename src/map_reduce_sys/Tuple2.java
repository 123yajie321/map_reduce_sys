package map_reduce_sys;

public class Tuple2<A,B> extends Tuple<A> {

	
	private B second;
	
	public Tuple2(A a,B b) {
		super(a);
		this.second=b;
		
	}

	private B getSecond() {
		
		return second;
	}
	

}
