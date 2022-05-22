package map_reduce_sys.interfaces;
import java.io.Serializable;
/**
 * The interface <code>BiFunction</code> is a serializable functional interface
 *
 * @author Yajie LIU, Zimeng ZHANG
 */
public interface BiFunction<T, U, R> extends java.util.function.BiFunction<T,U,R>, Serializable {

}
