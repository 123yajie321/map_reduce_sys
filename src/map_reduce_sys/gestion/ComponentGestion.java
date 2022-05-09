package map_reduce_sys.gestion;

import java.util.ArrayList;
import java.util.Random;
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
import map_reduce_sys.ReceiveTupleInboundPort;
import map_reduce_sys.ReceiveTupleWithPluginInboundPort;
import map_reduce_sys.connector.ConnectorCreatePlugin;
import map_reduce_sys.interfaces.RecieveTupleServiceI;
import map_reduce_sys.interfaces.SendTupleServiceI;
import map_reduce_sys.job.SimpleJob;
import map_reduce_sys.map.ComponentMap;
import map_reduce_sys.plugin.PluginManagementOut;
import map_reduce_sys.plugin.PluginResource;
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

	static int pluginid=0;
	protected ThreadPoolExecutor runTaskExecutor;
	
	/*
	 * protected LinkedBlockingQueue<Tuple> bufferRessource; protected
	 * LinkedBlockingQueue<Tuple> bufferResult; protected LinkedBlockingQueue<Tuple>
	 * bufferResultMap;
	 */
	protected SimpleJob job;
	long startTime ;
	long endTime;

	
	protected ComponentGestion() throws Exception {
		super(5, 0);
	   /* this.gmop=new GestionMapOutboundPort(GMOP_URI,this);
	    this.gmop.publishPort();
	    this.grdop=new GestionReduceOutboundPort(this);
	    this.grsop=new GestionResourceOutboundPort(this);
	    this.grdop.publishPort();
	    this.grsop.publishPort();*/
		int N=5;
		
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
		

		Function<Integer, Tuple> data_generator = (id)->{
		
			//	int number=(int) (Math.random()*50);
			int number=5;
			Tuple tuple=new OrderedTuple(1,id);
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
		Tuple sizeTuple=new Tuple(2);
		sizeTuple.setIndiceTuple(0, 15000);
		sizeTuple.setIndiceTuple(1, 0);
		
		Tuple sizeTuple2=new Tuple(2);
		sizeTuple2.setIndiceTuple(0, 30000);
		sizeTuple2.setIndiceTuple(1, 15000);
		Tuple sizeTuple3=new Tuple(2);
		
		sizeTuple3.setIndiceTuple(0, 30000);
		sizeTuple3.setIndiceTuple(1, 0);
		
		
		
		//SimpleJob jobAssocCommuJob=new SimpleJob(f_map, g_reduce, data_generator, Nature.COMMUTATIVE_ASSOCIATIVE, sizeTuple);
		//SimpleJob jobIteratif=new SimpleJob(f_map, g_reduce_iteratif, data_generator, Nature.ITERATIVE, sizeTuple);
		
		
		
		/*this.grsop.runTaskResource(data_generator, sizeTuple);
		this.gmop.runTaskMap(f_map,sizeTuple);
		this.grdop.runTaskReduce(g_reduce,sizeTuple);*/
		
		
		
		
		
		//vesion avec plugin
		
		//receive the  final result from component reduce 
		 startTime=System.currentTimeMillis();
		System.out.println(startTime);
		/*
		createPluginOutboundPort cpopReduce=new createPluginOutboundPort(this);
		createPluginOutboundPort cpopMap=new createPluginOutboundPort(this);
		createPluginOutboundPort cpopResource=new createPluginOutboundPort(this);
		createPluginOutboundPort cpopMap2=new createPluginOutboundPort(this);
		createPluginOutboundPort cpopResource2=new createPluginOutboundPort(this);
		cpopReduce.publishPort();
		cpopMap.publishPort();
		cpopResource.publishPort();
		cpopMap2.publishPort();
		cpopResource2.publishPort();
		
		doPortConnection(cpopResource.getPortURI(),CVM.URI_PORT_CREATEPLUGIN1, ConnectorCreatePlugin.class.getCanonicalName());
		doPortConnection(cpopMap.getPortURI(), CVM.URI_PORT_CREATEPLUGIN2, ConnectorCreatePlugin.class.getCanonicalName());
		doPortConnection(cpopReduce.getPortURI(), CVM.URI_PORT_CREATEPLUGIN3, ConnectorCreatePlugin.class.getCanonicalName());
		doPortConnection(cpopResource2.getPortURI(),CVM.URI_PORT_CREATEPLUGIN4, ConnectorCreatePlugin.class.getCanonicalName());
		doPortConnection(cpopMap2.getPortURI(), CVM.URI_PORT_CREATEPLUGIN5, ConnectorCreatePlugin.class.getCanonicalName());
		 */
		
		ArrayList<createPluginOutboundPort>CreatePluginconnectionsOP=createConnectionForinstallPlugins(CVM.URI_PORT_CREATEPLUGIN1
				,CVM.URI_PORT_CREATEPLUGIN2,CVM.URI_PORT_CREATEPLUGIN3,CVM.URI_PORT_CREATEPLUGIN4,CVM.URI_PORT_CREATEPLUGIN5);
		

		ReceiveTupleInboundPort gestionReceiveresultInPort=new ReceiveTupleInboundPort(this);
		gestionReceiveresultInPort.publishPort();
		
		
		/*
		String managementResInboundPort =AbstractPort.generatePortURI();
		String managementMapInboundPort=AbstractPort.generatePortURI();
		String managementReduceInboundPort=AbstractPort.generatePortURI();
		String managementResInboundPort2 =AbstractPort.generatePortURI();
		String managementMapInboundPort2=AbstractPort.generatePortURI();
		*/
		ArrayList<String> managementInboundPortUriList=new ArrayList<>();
		for(int i=0;i<5;i++) {
			String portUri=AbstractPort.generatePortURI();
			managementInboundPortUriList.add(portUri);
		}
		ArrayList<PluginManagementOut> pluginsManagementOut=createPluginsManagementOut(managementInboundPortUriList);
		
		
		
		String ResourceSendInboundPort=AbstractPort.generatePortURI();
		String mapSendInboundPort=AbstractPort.generatePortURI();
		String ResourceSendInboundPort2=AbstractPort.generatePortURI();
		String mapSendInboundPort2=AbstractPort.generatePortURI();
		
		
		
		/*
		ReflectionOutboundPort ResourcereflectionOutboundPort=connectWithComponentCalcul( CVM.URI_PORT_REFLEXION1);
		ReflectionOutboundPort MapreflectionOutboundPort=connectWithComponentCalcul( CVM.URI_PORT_REFLEXION2);
		ReflectionOutboundPort ReducereflectionOutboundPort=connectWithComponentCalcul( CVM.URI_PORT_REFLEXION3);
		ReflectionOutboundPort ResourcereflectionOutboundPort2=connectWithComponentCalcul( CVM.URI_PORT_REFLEXION4);
		ReflectionOutboundPort MapreflectionOutboundPort2=connectWithComponentCalcul( CVM.URI_PORT_REFLEXION5);
		*/
		
		PluginManagementOut pluginResOut=createPluginManagementOut(managementResInboundPort);
		PluginManagementOut pluginMapOut=createPluginManagementOut(managementMapInboundPort);
		PluginManagementOut pluginReduceOut=createPluginManagementOut(managementReduceInboundPort);
		PluginManagementOut pluginResOut2=createPluginManagementOut(managementResInboundPort2);
		PluginManagementOut pluginMapOut2=createPluginManagementOut(managementMapInboundPort2);
		
		
		/*	
		PluginResource pluginResourceIn=new PluginResource(managementResInboundPort, 2,data_generator,ResourceSendInboundPort);
		pluginResourceIn.setPluginURI("PluginResourceIn");
	

		PluginMap pluginMapIn=new PluginMap(managementMapInboundPort, 2,f_map,ResourceSendInboundPort,mapSendInboundPort);
		pluginMapIn.setPluginURI("pluginMapIn");
		
		
		PluginReduce pluginReduceIn=new PluginReduce(managementReduceInboundPort, 2,g_reduce,mapSendInboundPort,gestionReceiveresultInPort.getPortURI());
		pluginReduceIn.setPluginURI("PluginReduceIn");
		
		PluginResource pluginResourceIn2=new PluginResource(managementResInboundPort2, 2,data_generator,ResourceSendInboundPort2);
		pluginResourceIn.setPluginURI("PluginResourceIn2");
	

		PluginMap pluginMapIn2=new PluginMap(managementMapInboundPort2, 2,f_map,ResourceSendInboundPort2,mapSendInboundPort);
		pluginMapIn.setPluginURI("pluginMapIn2");
		
		ReducereflectionOutboundPort.installPlugin(pluginReduceIn);
		MapreflectionOutboundPort.installPlugin(pluginMapIn);
		ResourcereflectionOutboundPort.installPlugin(pluginResourceIn);
		/*
		 * ResourcereflectionOutboundPort2.installPlugin(pluginResourceIn2);
		 * MapreflectionOutboundPort2.installPlugin(pluginMapIn2);
		 */
		cpopResource.createPluginResource(managementResInboundPort, 2, data_generator, ResourceSendInboundPort, pluginid);
		pluginid++;
		cpopMap.createPluginMap(managementMapInboundPort, 2, f_map, ResourceSendInboundPort, mapSendInboundPort, pluginid);
		pluginid++;
		
		
		ArrayList<String>reduceReceiveInPortList=new ArrayList<String>();
		reduceReceiveInPortList.add(mapSendInboundPort);
		reduceReceiveInPortList.add(mapSendInboundPort2);
		
		Tuple pluginReduceinfo=createTuplePluginInfo(7, managementReduceInboundPort, 2,g_reduce,reduceReceiveInPortList,gestionReceiveresultInPort.getPortURI(), pluginid);
		cpopReduce.createPluginReduce(pluginReduceinfo);
		pluginid++;
		
		
		cpopResource2.createPluginResource(managementResInboundPort2, 2, data_generator, ResourceSendInboundPort2, pluginid);
		pluginid++;
		
		cpopMap2.createPluginMap(managementMapInboundPort2, 2, f_map, ResourceSendInboundPort2, mapSendInboundPort2, pluginid);
		pluginid++;
		
		this.installPlugin(pluginResOut);
		this.installPlugin(pluginResOut2);
		this.installPlugin(pluginMapOut);
		this.installPlugin(pluginMapOut2);
		this.installPlugin(pluginReduceOut);
		
		
		Thread.sleep(1000);
	
		pluginReduceOut.doManagementConnection();
		pluginMapOut.doManagementConnection();
		pluginResOut.doManagementConnection();
		pluginMapOut2.doManagementConnection();
		pluginResOut2.doManagementConnection();
		
		
		

		Runnable taskResourceConnect = () -> {
			 try {
				pluginResOut.getServicePort().DoPluginPortConnection();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		
		Runnable taskMapConnect = () -> {
			 try {
				 pluginMapOut.getServicePort().DoPluginPortConnection();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		};
		

		Runnable taskResourceConnect2 = () -> {
			 try {
				pluginResOut2.getServicePort().DoPluginPortConnection();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		
		Runnable taskMapConnect2 = () -> {
			 try {
				 pluginMapOut2.getServicePort().DoPluginPortConnection();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		};
		
		
		Runnable taskReduceConnect = () -> {
			 try {
				pluginReduceOut.getServicePort().DoPluginPortConnection();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
	
	
		runTaskExecutor.submit(taskReduceConnect);
		runTaskExecutor.submit(taskMapConnect);
		runTaskExecutor.submit(taskResourceConnect);
		runTaskExecutor.submit(taskMapConnect2);
		runTaskExecutor.submit(taskResourceConnect2);
		
		
		
		
		Runnable taskRessource = () -> {
			 try {
				pluginResOut.getServicePort().runTaskResource(data_generator, sizeTuple);
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		
		Runnable taskMap = () -> {
			 try {
				 pluginMapOut.getServicePort().runTaskMap(f_map, sizeTuple);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		};
		
		
		
		Runnable taskReduce = () -> {
			 try {
				 pluginReduceOut.getServicePort().runTaskReduce(g_reduce, sizeTuple3,Nature.ITERATIVE);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		};
		
		
	
		
		Runnable taskRessource2 = () -> {
			 try {
				pluginResOut2.getServicePort().runTaskResource(data_generator, sizeTuple2);
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		
		Runnable taskMap2 = () -> {
			 try {
				 pluginMapOut2.getServicePort().runTaskMap(f_map, sizeTuple2);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		};
		
		
		runTaskExecutor.submit(taskRessource);
		runTaskExecutor.submit(taskMap);
		runTaskExecutor.submit(taskReduce);
		runTaskExecutor.submit(taskRessource2);
		runTaskExecutor.submit(taskMap2);
		
		
		/*
		 pluginResOut.getResourceServicePort().runTaskResource(data_generator, sizeTuple);
		 pluginMapOut.getResMapServicePort().runTaskMap(f_map, sizeTuple);
		 pluginReduceOut.getReduceServicePort().runTaskReduce(g_reduce, sizeTuple);
		 
		 
		 
		 
		 ResourcereflectionOutboundPort.doDisconnection();
		 MapreflectionOutboundPort.doDisconnection();
		 ReducereflectionOutboundPort.doDisconnection();
		 ResourcereflectionOutboundPort2.doDisconnection();
		 MapreflectionOutboundPort2.doDisconnection();
		 
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
		 * 
		 */
		
		super.finalise();
	}
	
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		super.shutdown();
	}

	public boolean recieve_Tuple(Tuple t) {
		System.out.println("Task finished, the final result is : "+t.getIndiceData(0));
		long endTime=System.currentTimeMillis();
		System.out.println("Executetime "+(endTime-startTime)+"ms");   
		return true;
	}
	
	
	
	public ReflectionOutboundPort connectWithComponentCalcul(String inboundPortString) throws Exception {
		
		ReflectionOutboundPort reflectionOutboundPort=new ReflectionOutboundPort(this);
		reflectionOutboundPort.publishPort();
		this.doPortConnection(reflectionOutboundPort.getPortURI(),inboundPortString, ReflectionConnector.class.getCanonicalName());
		return reflectionOutboundPort;
		
	}
	
	
	public ArrayList<createPluginOutboundPort> createConnectionForinstallPlugins(String ...portUris) throws Exception{
		ArrayList<createPluginOutboundPort>portList=new ArrayList<>();
		for(String uri:portUris) {
			createPluginOutboundPort port=new createPluginOutboundPort(this);
			port.publishPort();
			doPortConnection(port.getPortURI(),uri, ConnectorCreatePlugin.class.getCanonicalName());
			portList.add(port);
		}
		
		
		return portList;
		
		
	}
	
	public ArrayList<PluginManagementOut> createPluginsManagementOut(ArrayList<String> inboundPortUris) throws Exception {
		
		ArrayList<PluginManagementOut>pluginOutList=new ArrayList<>();
		for(String inboundPortUri:inboundPortUris) {
			PluginManagementOut pluginOut=new PluginManagementOut();
			pluginOut.setInboundPortUri(inboundPortUri);
			pluginOut.setPluginURI("PluginOut"+pluginid);
			pluginid++;
			pluginOutList.add(pluginOut);
		}
			
		return pluginOutList;
	}
	
	
	
	
	public PluginManagementOut createPluginManagementOut(String inboundPortUri) throws Exception {
		
		PluginManagementOut pluginOut=new PluginManagementOut();
		pluginOut.setInboundPortUri(inboundPortUri);
		pluginOut.setPluginURI("PluginOut"+pluginid);
		pluginid++;
		return pluginOut;
		
	}
	
	 public Tuple createTuplePluginInfo(int size, Object ...arg) {
		 Tuple tupleinfo=new Tuple(size);
		 int indice=0;
		 for(Object info:arg) {
			 tupleinfo.setIndiceTuple(indice, info);
			 indice++;
		 }
		return tupleinfo;
		 
	 }
	
	
	 
	
	
	

}
