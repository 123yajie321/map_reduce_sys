package map_reduce_sys;

import java.util.ArrayList;
import java.util.concurrent.Callable;

import fr.sorbonne_u.components.AbstractComponent;

public  class SimpleJob  {
	
	protected ArrayList<Tuple> data;
	protected Nature nature;
	protected Callable f;
	protected Callable g;
	
	
	protected SimpleJob(ArrayList<Tuple>data, Callable f,Callable g,Nature nature) {
		this.data=data;
		this.f=f;
		this.g=g;
		this.nature=nature;
		
	}
	
	public ArrayList<Tuple> getData() {
		return data;
	}
	
     
	
	
	public Callable getF() {
		return f;
	}
	
	public Callable getG() {
		return g;
	}
	
	
	public Nature getNature() {
			
			return nature;
		}

	
	
	

}
