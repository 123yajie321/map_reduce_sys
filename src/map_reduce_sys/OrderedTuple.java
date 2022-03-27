package map_reduce_sys;

import java.io.Serializable;
import java.util.ArrayList;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.IconifyAction;

public class OrderedTuple extends Tuple implements Comparable<OrderedTuple> {
	
	
	
	  private static final long serialVersionUID = 1L;
	  private static int TupleId=0;
	  private int id;
	  
	  public OrderedTuple (int dimention){
		    super(dimention);
		    this.id=TupleId;
		    TupleId++;
		 
	  }
	  
	  public OrderedTuple (int dimention,int id){
		    super(dimention);
		    this.id=id;
		  
		 
	  }
	  
	  public OrderedTuple (int dimention,Object[]data){
		  super(dimention,data);
		  this.id=TupleId;
		    TupleId++;
		  
	  }
	  
   public int getId() {
	   return this.id;
   }

@Override
public int compareTo(OrderedTuple o) {
	
	return this.id-o.id;
}
      
      
      

}
