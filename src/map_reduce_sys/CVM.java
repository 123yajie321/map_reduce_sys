package map_reduce_sys;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;


import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import map_reduce_sys.connector.ConnectorMapGestion;
import map_reduce_sys.connector.ConnectorMapReduce;
import map_reduce_sys.connector.ConnectorRessourceMap;
import map_reduce_sys.gestion.ComponentGestion;
import map_reduce_sys.map.ComponentMap;
import map_reduce_sys.reduce.ComponentReduce;
import ressource.ComponentRessource;

public class CVM extends AbstractCVM {
	
	public CVM() throws Exception {
		// TODO Auto-generated constructor stub
	}

	
	@Override
	public void deploy() throws Exception {
		
		/*
		 * Function<Tuple, Tuple> f_map = a -> { Double resDouble =
		 * (Double)a.getIndiceData(0)*10.0; Tuple tuple = new Tuple(1);
		 * tuple.setIndiceTuple(0, resDouble); return tuple; };
		 * 
		 * BiFunction<Tuple, Tuple, Tuple> g_reduce = (a,b) -> { Double resDouble =
		 * (Double)a.getIndiceData(0)+(Double)b.getIndiceData(0); Tuple tuple = new
		 * Tuple(1); tuple.setIndiceTuple(0, resDouble); return tuple; };
		 */
		
		/*
		 * Function<Void, Tuple> s_generator = (Void v) -> { Object[] randomNum=new
		 * Object[10]; for(int i=0;i<randomNum.length;i++){
		 * randomNum[i]=(int)(Math.random()*20); } Tuple tuple = new Tuple(1);
		 * tuple.setIndiceTuple(0, randomNum); return tuple; };
		 * 
		 * SimpleJob job = new
		 * SimpleJob(f_map,g_reduce,s_generator,Nature.COMMUTATIVE_ASSOCIATIVE);
		 */
		
		/*
		 * Function<Tuple, Tuple> f_map = a -> { ArrayList<Tuple>
		 * data=(ArrayList<Tuple>)a.getIndiceData(0); for(int i=0;i<data.size();i++) {
		 * Double resDouble =(Double)data.get(i).getIndiceData(0)*10.0; Tuple tuple =
		 * new Tuple(1); tuple.setIndiceTuple(0, resDouble); data.set(i,tuple); } Tuple
		 * result=new Tuple(1); result.setIndiceTuple(0, data); return result;
		 * 
		 * };
		 * 
		 * BiFunction<Tuple, Tuple, Tuple> g_reduce = (acc,y) -> { BiFunction<Tuple,
		 * Tuple, Tuple> g = (a,b) -> { Double resDouble =
		 * (Double)a.getIndiceData(0)+(Double)b.getIndiceData(0); Tuple tuple = new
		 * Tuple(1); tuple.setIndiceTuple(0, resDouble); return tuple; }; Double
		 * cpt=(Double)acc.getIndiceData(0);
		 * ArrayList<Tuple>tuples=(ArrayList<Tuple>)y.getIndiceData(0);
		 * 
		 * 
		 * for(int i=0;i<tuples.size()-1;i++){
		 * 
		 * Tuple tmp=g.apply(tuples.get(i), tuples.get(i+1)); tuples.remove(i);
		 * tuples.remove(i); tuples.add(tmp); i=i-1;
		 * 
		 * }
		 * 
		 * Tuple reult=new Tuple(1); reult.setIndiceTuple(0, tuples.get(0)); return
		 * reult;
		 * 
		 * };
		 */
		
		/*
		 * AbstractComponent.createComponent(ComponentGestion.class.getCanonicalName(),
		 * new Object[] {}); Object[] paramObjects = new Object[1]; paramObjects[0] =
		 * f_map; String cmURI =
		 * AbstractComponent.createComponent(ComponentMap.class.getCanonicalName(),
		 * paramObjects); //
		 * System.out.println(cmURI+" ; "+ComponentGestion.GMOP_URI+" ; "+ComponentMap.
		 * MGIP_URI+" ; "+ConnectorMapGestion.class.getCanonicalName());
		 * 
		 * this.doPortConnection(cmURI,ComponentGestion.GMOP_URI,ComponentMap.MGIP_URI
		 * ,ConnectorMapGestion.class.getCanonicalName()); Object[] param = new
		 * Object[1]; paramObjects[0] = g_reduce; String
		 * reduceUriString=AbstractComponent.createComponent(ComponentReduce.class.
		 * getCanonicalName(),param); this.doPortConnection(reduceUriString,
		 * ComponentMap.MSROP_URI, ComponentReduce.RRMIP_URI,
		 * ConnectorMapGestion.class.getCanonicalName());
		 */
	
		Function<Void, Tuple> data_generator = (v)->{
			
			double number=Math.random()*20;
			Tuple tuple=new Tuple(1);
			tuple.setIndiceTuple(0, number);
			return tuple;	
			
		};
				
		Object[] paramRessources= new Object[1];	
		paramRessources[0]=data_generator;
		
		Function<Tuple, Tuple> f_map = a -> { 
			Double resDouble =(Double)a.getIndiceData(0)*10.0;
			Tuple tuple = new Tuple(1);
			tuple.setIndiceTuple(0, resDouble); 
			return tuple;
		};
		
		Object[] paramMap= new Object[1];	
		paramMap[0]=f_map;
		
		
		
		
		BiFunction<Tuple, Tuple, Tuple> g_reduce = (a,b) -> { 
			Double resDouble =(Double)a.getIndiceData(0)+(Double)b.getIndiceData(0); 
			Tuple tuple = new Tuple(1); 
			tuple.setIndiceTuple(0, resDouble); 
			return tuple; 
				  
		};
		Object[] paramReduce= new Object[1];	
		paramReduce[0]=g_reduce;
		
	
		String uriRessource=AbstractComponent.createComponent(ComponentRessource.class.getCanonicalName(), paramRessources);
		String uriMap=AbstractComponent.createComponent(ComponentMap.class.getCanonicalName(), paramMap);
	    String uriReduce=AbstractComponent.createComponent(ComponentReduce.class.getCanonicalName(), paramReduce);
	 
		this.doPortConnection(uriRessource, ComponentRessource.RSMOP_URI, ComponentMap.MRRIP_URI, ConnectorRessourceMap.class.getCanonicalName());
		this.doPortConnection(uriMap,ComponentMap.MSROP_URI,ComponentReduce.RRMIP_URI,ConnectorMapReduce.class.getCanonicalName() );
	 
		super.deploy();
	}


	public static void main(String[] args) {
		
		try {
			CVM c = new CVM();
			c.startStandardLifeCycle(4000L);
			System.exit(0);
		} catch (Exception e) {
		
			e.printStackTrace();
		}

	}


}
