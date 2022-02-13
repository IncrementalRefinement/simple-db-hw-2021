package simpledb.execution;

import jdk.nashorn.internal.runtime.regexp.joni.constants.StringType;
import simpledb.common.Type;
import simpledb.storage.*;

import java.net.Inet4Address;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator extends AbstractAggregator {

    private static final long serialVersionUID = 1L;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        super(gbfield, gbfieldtype, afield, what);
        if (!what.equals(Op.COUNT)) {
            throw new IllegalArgumentException();
        }
    }
}
