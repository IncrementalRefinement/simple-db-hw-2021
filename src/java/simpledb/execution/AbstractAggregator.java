package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

// FIXME: refactor this disgusting shit(especially the disaster if-else clause)

public class AbstractAggregator implements Aggregator {
    private final static String DEFAULT_KEY = "DEFAULT_KEY";

    private final int gbfield;
    private final Type gbfieldType;
    private final int afield;
    private final Op what;
    private final TupleDesc tupleDesc;
    private final Map<String, Integer> gbMap;
    private final Map<String, Integer> gbCountMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public AbstractAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.gbMap = new HashMap<>();
        this.gbCountMap = new HashMap<>();

        if (gbfield == NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        String key = null;
        int aggregateValue;
        int currentValue = 0;

        // TODO: The refactor is just a piece of shit
        if ((tup.getTupleDesc().getFieldType(afield)) == Type.INT_TYPE) {
            currentValue = ((IntField) tup.getField(afield)).getValue();
        }

        if (gbfield == NO_GROUPING) {
            key = DEFAULT_KEY;
        } else {
            key = tup.getField(gbfield).toString();
        }


        if (gbMap.containsKey(key)) {
            aggregateValue = gbMap.get(key);
        } else {
            // if the key doesn't exist, initialize and update the map
            switch (this.what) {
                case MAX: case MIN: {
                    aggregateValue = currentValue;
                    break;
                }
                case AVG: case SUM: case COUNT: {
                    aggregateValue = 0;
                    break;
                }
                default: {
                    throw new UnsupportedOperationException("The op for aggregate is not Implemented");
                }
            }
            gbCountMap.put(key, 0);
        }


        switch (this.what) {
            case MIN: {
                aggregateValue = Math.min(aggregateValue, currentValue);
                break;
            }
            case MAX: {
                aggregateValue = Math.max(aggregateValue, currentValue);
                break;
            }
            case AVG: case SUM: {
                aggregateValue += currentValue;
                break;
            }
            case COUNT: {
                aggregateValue += 1;
                break;
            }
            default:
                throw new UnsupportedOperationException("The op for aggregate is not Implemented");
        }

        gbMap.put(key, aggregateValue);
        gbCountMap.put(key, gbCountMap.get(key) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new LinkedList<>();

        if (gbfield == NO_GROUPING) {
            Tuple onlyTuple = new Tuple(tupleDesc);
            IntField valueField;
            if (this.what == Op.AVG) {
                int value = gbMap.get(DEFAULT_KEY) / gbCountMap.get(DEFAULT_KEY);
                valueField = new IntField(value);
            } else {
                valueField = new IntField(gbMap.get(DEFAULT_KEY));
            }
            onlyTuple.setField(GROUP_VALUE_INDEX, valueField);
            tuples.add(onlyTuple);
        } else {
            for (Map.Entry<String, Integer> entry : gbMap.entrySet()) {
                Tuple entryTuple = new Tuple(tupleDesc);
                Field groupField, valueField;

                if (gbfieldType.equals(Type.INT_TYPE)) {
                    groupField = new IntField(Integer.parseInt(entry.getKey()));
                } else {
                    groupField = new StringField(entry.getKey(), entry.getKey().length());
                }
                if (this.what == Op.AVG) {
                    int value = entry.getValue() / gbCountMap.get(entry.getKey());
                    valueField = new IntField(value);
                } else {
                    valueField = new IntField(entry.getValue());
                }
                entryTuple.setField(GROUP_VALUE_INDEX, groupField);
                entryTuple.setField(AGGREGATE_VALUE_INDEX, valueField);
                tuples.add(entryTuple);
            }
        }

        return new TupleIterator(tupleDesc, tuples);
    }

    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }
}
