package map_reduce_sys.structure;

import java.io.Serializable;
/**
 * The class <code>OrderedTuple</code>This class defines 
 * Basic structure for data transfer between components,
 * its instances are orderable
 * @author Yajie LIU, Zimeng ZHANG
 */

public class OrderedTuple extends Tuple implements Comparable<OrderedTuple> {
	
	  private static final long serialVersionUID = 1L;
	  /**generate id for tuple */
	  private static int TupleId=0;
	  
	  /**identifier of the tuple, 
	   * it'a also can be the biggest tuple that is fused*/
	  private int id;
	  /**Used to record the id of the smallest Tuple that is fused*/
	  private int rangeMin;
	  
	  
	  
	  public OrderedTuple (int dimention){
		    super(dimention);
		    this.id=TupleId;
		    this.rangeMin=-100;
		    TupleId++; 
	  }
	  
	   
	  public OrderedTuple (int dimention,int id){
		    super(dimention);
		    this.id=id;
		    this.rangeMin=-100;
		    
	  }
	  
	  public OrderedTuple (int dimention,int id,int rangeMin){
		    super(dimention);
		    this.id=id;
		    this.rangeMin=rangeMin;
		  
		 
	  }
	  
	  public OrderedTuple (int dimention,Object[]data){
		  super(dimention,data);
		  this.id=TupleId;
		  this.rangeMin=-100;
		    TupleId++;
		  
	  }
	  
   public int getId() {
	   return this.id;
   }
   
   
   public int getRangeMin() {
	   
	   return this.rangeMin;
   }

   
 public void setRangeMin(int rangemin) {
	   
	    this.rangeMin=rangemin;
   }
 
 public void setId(int id) {
	   
	    this.id=id;
}
   
   
@Override
public int compareTo(OrderedTuple o) {
	
	return this.id-o.id;
}
      
      
      

}
