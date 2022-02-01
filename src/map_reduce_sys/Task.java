package map_reduce_sys;

public interface Task {
	
	public  Resource getResource();
	public  Funtion get_map_f();
	public  Funtion get_reduce_g();
	public  String   getNature();
	
	

}
