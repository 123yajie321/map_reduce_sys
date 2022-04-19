package map_reduce_sys.structure;

import java.io.Serializable;
import java.util.ArrayList;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.IconifyAction;

public class OrderedTuple extends Tuple implements Comparable<OrderedTuple> {
	
	
	
	  private static final long serialVersionUID = 1L;
	  private static int TupleId=0;
	  private int id;
	  //Used to record the id of the smallest Tuple that is fused
	  private int rangeMin;
	  
	  public OrderedTuple (int dimention){
		    super(dimention);
		    this.id=TupleId;
		    TupleId++;
		 
	  }
	  
	  public OrderedTuple (int dimention,int id){
		    super(dimention);
		    this.id=id;
		  
		 
	  }
	  
	  public OrderedTuple (int dimention,int id,int rangeMin){
		    super(dimention);
		    this.id=id;
		    this.rangeMin=rangeMin;
		  
		 
	  }
	  
	  public OrderedTuple (int dimention,Object[]data){
		  super(dimention,data);
		  this.id=TupleId;
		    TupleId++;
		  
	  }
	  
   public int getId() {
	   return this.id;
   }
   
   
   public int getRangeMin() {
	   
	   return this.rangeMin;
   }

@Override
public int compareTo(OrderedTuple o) {
	
	return this.id-o.id;
}
      
      
      

}
