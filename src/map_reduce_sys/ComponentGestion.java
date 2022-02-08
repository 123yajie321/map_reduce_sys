package map_reduce_sys;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.*;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
@RequiredInterfaces(required = {CalculServiceI.class})

public class ComponentGestion extends AbstractComponent {

	public static final String CGOP_URI = "cgop-uri";
	protected CalculServiceOutboundPort cgop;
	
	protected ComponentGestion() throws Exception {
		super(1, 0);
	    this.cgop=new CalculServiceOutboundPort(CGOP_URI,this);
	    this.cgop.publishPort();
	}
	
	@Override
	public synchronized void execute() throws Exception {
		super.execute();
		SimpleJob job;
		
	}
	
	@Override
	public synchronized void finalise() throws Exception {		
		this.doPortDisconnection(CGOP_URI);
		super.finalise();
	}
	
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		super.shutdown();
	}
	
	
	
	
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
	
	


}
