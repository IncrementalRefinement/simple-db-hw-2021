package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator extends AbstractAggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        super(gbfield, gbfieldtype, afield, what);
    }
}
