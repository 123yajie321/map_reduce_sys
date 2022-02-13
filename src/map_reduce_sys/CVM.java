package map_reduce_sys;

import java.util.function.BiFunction;
import java.util.function.Function;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import map_reduce_sys.connector.ConnectorMapGestion;
import map_reduce_sys.gestion.ComponentGestion;
import map_reduce_sys.map.ComponentMap;

public class CVM extends AbstractCVM {
	
	public CVM() throws Exception {
		// TODO Auto-generated constructor stub
	}

	
	@Override
	public void deploy() throws Exception {
		AbstractComponent.createComponent(ComponentGestion.class.getCanonicalName(), new Object[] {});
		
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
		Object[] paramObjects = new Object[1];
		paramObjects[0] = f_map;
		String cmURI = AbstractComponent.createComponent(ComponentMap.class.getCanonicalName(), paramObjects);
		
		this.doPortConnection(cmURI, ComponentMap.CMOP_URI, 
				ComponentGestion.CGOP_URI, ConnectorMapGestion.class.getCanonicalName());
		super.deploy();
	}


	public static void main(String[] args) {
		
		try {
			CVM c = new CVM();
			c.startStandardLifeCycle(5000L);
			System.exit(0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


}
