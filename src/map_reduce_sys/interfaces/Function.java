package map_reduce_sys.interfaces;
import java.io.Serializable;
/**
 * The interface <code>Function</code> is a serializable functional interface
 *
 * @author Yajie LIU, Zimeng ZHANG
 */
public interface Function<T, R> extends java.util.function.Function<T, R>, Serializable {

}
