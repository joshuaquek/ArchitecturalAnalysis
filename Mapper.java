package aa;

import java.util.HashMap;
import java.util.List;

/**
 * @author kevinsteppe
 */
public interface Mapper
{
    /**
     * Implement this to provide the mapping function.
     * It is STRONGLY recommended that the map have no side-effects (no use of non-local variables).
     * @param list the data shard for this mapper
     * @return a map of key-value pairs which will be combined from all mappers and given to the reducers
     */
    public HashMap map(List list);
}
