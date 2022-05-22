package map_reduce_sys.structure;

import java.io.Serializable;
import java.util.ArrayList;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.IconifyAction;
/**
 * The class <code>Tuple</code>This class defines 
 * Basic structure for data transfer between components,
 * @author Yajie LIU, Zimeng ZHANG
 */

public class Tuple implements Serializable {
	
	  private static final long serialVersionUID = 1L;
	  /**Array that can be used to store different types of data*/
	  private Object[] tuple;
	  /**The size of the Array */
	  private int dimension; 
	  

	  public Tuple (int dimension){
		  	this.dimension=dimension;
		  	tuple=new Object [dimension]; 
	  }
	  public Tuple (int dimension,Object[]data){
		  	this.dimension=dimension;
		  	tuple=data ;
	  }
	  

      public Object getIndiceData(int indice) {
    	  return tuple[indice];
      }
      
      public void setIndiceTuple(int indice,Object o) {
    	  tuple[indice] = o;
      }
      
      public int getDimension() {
    	  
    	  return this.dimension;
      }
      
      
      

}
