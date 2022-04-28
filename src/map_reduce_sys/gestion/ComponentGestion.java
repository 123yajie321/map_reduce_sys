package map_reduce_sys.gestion;

import java.util.ArrayList;


import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;


import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;

import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.reflection.connectors.ReflectionConnector;
import fr.sorbonne_u.components.reflection.ports.ReflectionOutboundPort;
import map_reduce_sys.CVM;
import map_reduce_sys.CalculServiceOutboundPort;

import map_reduce_sys.connector.ConnectorMapGestion;
import map_reduce_sys.connector.ConnectorReduceGestion;
import map_reduce_sys.connector.ConnectorResourceGestion;
import map_reduce_sys.interfaces.RecieveTupleServiceI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.job.SimpleJob;
import map_reduce_sys.map.ComponentMap;
import map_reduce_sys.plugin.PluginManagementMapOut;
import map_reduce_sys.plugin.PluginManagementReduceOut;
import map_reduce_sys.plugin.PluginManagementResourceIn1;
import map_reduce_sys.plugin.PluginResource;
import map_reduce_sys.plugin.PluginManagementResourceOut;
import map_reduce_sys.plugin.PluginMap;
import map_reduce_sys.plugin.PluginReduce;
import map_reduce_sys.reduce.ComponentReduce;
import map_reduce_sys.ressource.ComponentRessource;
import map_reduce_sys.structure.Nature;
import map_reduce_sys.structure.OrderedTuple;
import map_reduce_sys.structure.Tuple;

@RequiredInterfaces(required ={SendTupleServiceI.class})


public class ComponentGestion extends AbstractComponent {

	public static final String GMOP_URI = "gmop-uri";
	
	protected GestionMapOutboundPort gmop;
	protected GestionReduceOutboundPort grdop;
	protected GestionResourceOutboundPort grsop;
	protected ThreadPoolExecutor runTaskExecutor;
	/*
	 * protected LinkedBlockingQueue<Tuple> bufferRessource; protected
	 * LinkedBlockingQueue<Tuple> bufferResult; protected LinkedBlockingQueue<Tuple>
	 * bufferResultMap;
	 */
	
	
	
	protected SimpleJob job;


	
	protected ComponentGestion() throws Exception {
		super(5, 0);
	   /* this.gmop=new GestionMapOutboundPort(GMOP_URI,this);
	    this.gmop.publishPort();
	    this.grdop=new GestionReduceOutboundPort(this);
	    this.grsop=new GestionResourceOutboundPort(this);
	    this.grdop.publishPort();
	    this.grsop.publishPort();*/
		int N=3;
		runTaskExecutor = new ThreadPoolExecutor(N, N, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(20));
		runTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		this.getTracer().setTitle("Gestion ");
		this.getTracer().setRelativePosition(2, 0);
		this.toggleTracing();
	}
	
	@Override
	public synchronized void start() throws ComponentStartException {

		super.start();
		/*try {
			doPortConnection(this.grsop.getPortURI(),ComponentRessource.GRSIP_URI , ConnectorResourceGestion.class.getCanonicalName());
			doPortConnection(this.gmop.getPortURI(),ComponentMap.MGIP_URI , ConnectorMapGestion.class.getCanonicalName());
			doPortConnection(this.grdop.getPortURI(),ComponentReduce.GRDIP_URI , ConnectorReduceGestion.class.getCanonicalName());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}
	@Override
	public synchronized void execute() throws Exception {
		super.execute();
		

		Function<Void, Tuple> data_generator = (v)->{
			
			//	int number=(int) (Math.random()*50);
			int number=5;
			Tuple tuple=new OrderedTuple(1);
			tuple.setIndiceTuple(0, number);
			return tuple;	
			
		};
		
		Function<Void, Tuple> matrix_generator = (v)->{
			
			int nb_line=2;
		    int matrix[][]=new int[nb_line][nb_line];
			
		    for(int i=0;i<nb_line;i++) {
		    	 for(int j=0;j<nb_line;j++) {
		    		matrix[i][j]= (int) (Math.random()*10);
		    	 }
		    	
		    }
		    
		    Tuple tuple=new OrderedTuple(1);
			tuple.setIndiceTuple(0, matrix);
			return tuple;	
			
		};
				
		

		Function<Tuple, Tuple> f_map = a -> { 
			OrderedTuple t=(OrderedTuple)a;
			Integer res =(Integer)a.getIndiceData(0)*10;
			Tuple tuple = new OrderedTuple(1,t.getId());
			tuple.setIndiceTuple(0, res); 
			return tuple;
		};
		
		Function<Tuple, Tuple> f_map_matrix = a -> { 
			OrderedTuple t=(OrderedTuple)a;
			Integer res =(Integer)a.getIndiceData(0)*10;
			Tuple tuple = new OrderedTuple(1,t.getId());
			tuple.setIndiceTuple(0, res); 
			return tuple;
		};
		
		
		
		
		
		BiFunction<Tuple, Tuple, Tuple> g_reduce = (a,b) -> {
			OrderedTuple t=(OrderedTuple)b;

			int resInteger =(int)a.getIndiceData(0)+(int)b.getIndiceData(0); 
			Tuple tuple = new OrderedTuple(1,t.getId());
			tuple.setIndiceTuple(0, resInteger); 
			return tuple; 
				  
		};
		
		

		BiFunction<Tuple, Tuple, Tuple> g_reduce_iteratif = (a,b) -> {
			OrderedTuple t=(OrderedTuple)b;

			int resInteger =(int)a.getIndiceData(0)-(int)b.getIndiceData(0); 
			Tuple tuple = new OrderedTuple(1,t.getId());
			tuple.setIndiceTuple(0, resInteger); 
			return tuple; 
				  
		};
		Tuple sizeTuple=new Tuple(1);
		sizeTuple.setIndiceTuple(0, 100);
		
		
		
		
		SimpleJob jobAssocCommuJob=new SimpleJob(f_map, g_reduce, data_generator, Nature.COMMUTATIVE_ASSOCIATIVE, sizeTuple);
		SimpleJob jobIteratif=new SimpleJob(f_map, g_reduce_iteratif, data_generator, Nature.ITERATIVE, sizeTuple);
		
		
		
		/*this.grsop.runTaskResource(data_generator, sizeTuple);
		this.gmop.runTaskMap(f_map,sizeTuple);
		this.grdop.runTaskReduce(g_reduce,sizeTuple);*/
		
		
		
		
		
		//vesion avec plugin
		
		
		String managementResInboundPort =AbstractPort.generatePortURI();
		String managementMapInboundPort=AbstractPort.generatePortURI();
		String managementReduceInboundPort=AbstractPort.generatePortURI();
		
		String ResourceSendInboundPort=AbstractPort.generatePortURI();
		String mapSendInboundPort=AbstractPort.generatePortURI();
		
		this.traceMessage(
				"begin resouceReflectOutboundPort" + ".\n");
		ReflectionOutboundPort ResourcereflectionOutboundPort=new ReflectionOutboundPort(this);
		ResourcereflectionOutboundPort.publishPort();
		this.doPortConnection(ResourcereflectionOutboundPort.getPortURI(), CVM.URI_PORT_REFLEXION1, ReflectionConnector.class.getCanonicalName());
		this.traceMessage(
				"resource Connection reussi " + ".\n");

		ReflectionOutboundPort MapreflectionOutboundPort=new ReflectionOutboundPort(this);
		MapreflectionOutboundPort.publishPort();
		this.doPortConnection(MapreflectionOutboundPort.getPortURI(), CVM.URI_PORT_REFLEXION2, ReflectionConnector.class.getCanonicalName());
		

		ReflectionOutboundPort ReducereflectionOutboundPort=new ReflectionOutboundPort(this);
		ReducereflectionOutboundPort.publishPort();
		this.doPortConnection(ReducereflectionOutboundPort.getPortURI(), CVM.URI_PORT_REFLEXION3, ReflectionConnector.class.getCanonicalName());
		
		
		
		this.traceMessage(
				"begin Resource plugin" + ".\n");
		PluginResource pluginResourceIn=new PluginResource(managementResInboundPort, 2,data_generator,ResourceSendInboundPort);
		pluginResourceIn.setPluginURI("PluginResourceIn");
		this.traceMessage(
				"end Resouce plugin " + ".\n");
		
		PluginManagementResourceOut pluginResOut=new PluginManagementResourceOut();
		pluginResOut.setInboundPortUri(managementResInboundPort);
		pluginResOut.setPluginURI("PluginResourceOut");
		
		PluginMap pluginMapIn=new PluginMap(managementMapInboundPort, 2,f_map,ResourceSendInboundPort,mapSendInboundPort);
		pluginMapIn.setPluginURI("pluginMapIn");
		
		PluginManagementMapOut pluginMapOut=new PluginManagementMapOut();
		pluginMapOut.setInboundPortUri(managementMapInboundPort);
		pluginMapOut.setPluginURI("PluginMapOut");
		
		
		PluginReduce pluginReduceIn=new PluginReduce(managementReduceInboundPort, 2,g_reduce,mapSendInboundPort);
		pluginReduceIn.setPluginURI("PluginReduceIn");
		
		
		PluginManagementReduceOut pluginReduceOut=new PluginManagementReduceOut();
		pluginReduceOut.setInboundPortUri(managementReduceInboundPort);
		pluginReduceOut.setPluginURI("PluginReduceOut");
		System.out.println(" install begin");

		ReducereflectionOutboundPort.installPlugin(pluginReduceIn);
		System.out.println(" install reduce");
		MapreflectionOutboundPort.installPlugin(pluginMapIn);
		System.out.println(" install map");
		ResourcereflectionOutboundPort.installPlugin(pluginResourceIn);
		System.out.println(" install in");
		this.installPlugin(pluginResOut);
		this.installPlugin(pluginMapOut);
		this.installPlugin(pluginReduceOut);
		System.out.println(" install out");
		
		
		Runnable taskRessource = () -> {
			 try {
				pluginResOut.getResourceServicePort().runTaskResource(data_generator, sizeTuple);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		};
		
		Runnable taskMap = () -> {
			 try {
				 pluginMapOut.getResMapServicePort().runTaskMap(f_map, sizeTuple);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		};
		
		
		Runnable taskReduce = () -> {
			 try {
				 pluginReduceOut.getReduceServicePort().runTaskReduce(g_reduce_iteratif, sizeTuple,Nature.ITERATIVE);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		};
		
		
		runTaskExecutor.submit(taskRessource);
		runTaskExecutor.submit(taskMap);
		runTaskExecutor.submit(taskReduce);
		/*
		 pluginResOut.getResourceServicePort().runTaskResource(data_generator, sizeTuple);
		 pluginMapOut.getResMapServicePort().runTaskMap(f_map, sizeTuple);
		 pluginReduceOut.getReduceServicePort().runTaskReduce(g_reduce, sizeTuple);
		 */
		 
		 
		 
		 ResourcereflectionOutboundPort.doDisconnection();
		 MapreflectionOutboundPort.doDisconnection();
		 ReducereflectionOutboundPort.doDisconnection();
		 
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
		
		/*
		 * ArrayList<Tuple>source=new ArrayList<Tuple>();
		 * 
		 * for(int i=0;i<6;i++) { Tuple tuple=new Tuple(1); Double
		 * randomNum=(double)(Math.random()*20); tuple.setIndiceTuple(0, randomNum);
		 * source.add(tuple); } Tuple tuple=new Tuple(1); tuple.setIndiceTuple(0,
		 * tuple); this.gmop.tupleSender(tuple);
		 */
		
		
	}
	
	public SimpleJob getSimpleJob() {
		return this.job;
	}
	
	public void setJob(SimpleJob job) {
		this.job = job;
	}
	
	/*
	 * public boolean recieveTuple(Tuple t) throws InterruptedException {
	 * this.bufferResultMap.put(t); return true; }
	 */
	@Override
	public synchronized void finalise() throws Exception {		
		/*
		 * this.doPortDisconnection(GMOP_URI);
		 * this.doPortDisconnection(this.grsop.getPortURI());
		 * this.doPortDisconnection(this.grdop.getPortURI());
		 */
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
