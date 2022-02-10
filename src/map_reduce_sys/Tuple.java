package map_reduce_sys;

import java.io.Serializable;
import java.util.ArrayList;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.IconifyAction;

public class Tuple implements Serializable {
	
	  private static final long serialVersionUID = 1L;
	  private Object[] tuple    ;
	  private int dimention; 
	  

	  public Tuple (int dimention){
		  	this.dimention=dimention;
		  	tuple=new Object [dimention]; 
	  }

      public Object getIndiceData(int indice) {
    	  return tuple[indice];
      }
      
      

}
