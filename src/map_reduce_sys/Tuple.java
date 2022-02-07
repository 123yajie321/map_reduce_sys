package map_reduce_sys;

import java.io.Serializable;
import java.util.ArrayList;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.IconifyAction;

public class Tuple implements Serializable {
	
	  private static final long serialVersionUID = 1L;
	  private  ArrayList<Object> tuple;
	 
	  

	  public Tuple (ArrayList<Object>tuple){
	
		  	this.tuple=tuple; 
	 }

    public ArrayList<Object> getTuple() {
		return tuple;
	}
		
	

}
