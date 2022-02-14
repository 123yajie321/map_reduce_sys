package map_reduce_sys.gestion;

import java.util.ArrayList;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.*;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import map_reduce_sys.CalculServiceOutboundPort;
import map_reduce_sys.Nature;
import map_reduce_sys.SimpleJob;
import map_reduce_sys.Tuple;
import map_reduce_sys.interfaces.RecieveTupleServiceI;
import map_reduce_sys.interfaces.SendTupleServiceI;

@RequiredInterfaces(required ={SendTupleServiceI.class})


public class ComponentGestion extends AbstractComponent {

	public static final String GMOP_URI = "gmop-uri";
	protected GestionMapOutboundPort gmop;
	protected ConcurrentLinkedQueue<Tuple> bufferRessource;
	protected ConcurrentLinkedQueue<Tuple> bufferResult;
	protected ConcurrentLinkedQueue<Tuple> bufferResultMap;
	protected SimpleJob job;


	
	protected ComponentGestion() throws Exception {
		super(1, 0);
	    this.gmop=new GestionMapOutboundPort(GMOP_URI,this);
	    this.gmop.publishPort();
	}
	
	@Override
	public synchronized void execute() throws Exception {
		super.execute();
		/*
		Function<Tuple, Tuple> f_map = a -> {
			Double resDouble = (Double)a.getIndiceData(0)*10.0;
			Tuple tuple = new Tuple(1);
			tuple.setIndiceTuple(0, resDouble);
			return tuple;
		};
		
		BiFunction<Tuple, Tuple, Tuple> g_reduce = (a,b) -> {
			Double resDouble = (Double)a.getIndiceData(0)+(Double)b.getIndiceData(0);
			Tuple tuple = new Tuple(1);
			tuple.setIndiceTuple(0, resDouble);
			return tuple;
		};
		
		Function<Void, Tuple> s_generator = (Void v) -> {
			Object[] randomNum=new Object[10];
			for(int i=0;i<randomNum.length;i++){
				randomNum[i]=(int)(Math.random()*20);
			}	
			Tuple tuple = new Tuple(1);
			tuple.setIndiceTuple(0, randomNum);
			return tuple;
		};
		 
		SimpleJob job = new SimpleJob(f_map,g_reduce,s_generator,Nature.COMMUTATIVE_ASSOCIATIVE);
		setJob(job);
		*/
		

		ArrayList<Tuple>source=new ArrayList<Tuple>();
		
		for(int i=0;i<6;i++) {
			Tuple tuple=new Tuple(1);
			Double randomNum=(double)(Math.random()*20);
			tuple.setIndiceTuple(0, randomNum);
			source.add(tuple);
		}
		Tuple tuple=new Tuple(1);
		tuple.setIndiceTuple(0, tuple);
		this.gmop.tupleSender(tuple);
		
		
	}
	
	public SimpleJob getSimpleJob() {
		return this.job;
	}
	
	public void setJob(SimpleJob job) {
		this.job = job;
	}
	
	public boolean recieveTuple(Tuple t) {
		this.bufferResultMap.add(t);
		return true;
	}
	
	@Override
	public synchronized void finalise() throws Exception {		
		this.doPortDisconnection(GMOP_URI);
		super.finalise();
	}
	
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		super.shutdown();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	/*
	
	public <R> Tuple map(Function < Tuple,Tuple>f,Tuple tuple1) {
		

	   ArrayList<Object> tuple=tuple1.getTuple();
	   ArrayList<Object> result=(ArrayList<Object>) tuple.stream().map( (Function<? super Object, ? extends R>) f).collect(Collectors.toList());	   
	   Tuple res=new Tuple(result);
		return res;
		
	}
	
	
	public <R> Tuple reduce(Callable<?> g,Object acc, Tuple tuple1) {
		
		   ArrayList<Object> tuple=tuple1.getTuple();
		   Object result=(ArrayList<Object>) ((Stream<Object>) tuple.stream().reduce(acc,(BinaryOperator<Object>) g)).collect(Collectors.toList());	   
		   ArrayList<Object> tmp= new ArrayList<Object>();
		   tmp.add(result);
		   Tuple res=new Tuple(tmp);
		   return res;		
		}
	
	
  public ArrayList<Tuple> generate_data(Callable<?> g) throws Exception {
		  ArrayList<Tuple>result=(ArrayList<Tuple>) g.call();
		  return result;
	  }
	*/
	


}
