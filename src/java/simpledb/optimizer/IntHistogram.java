package simpledb.optimizer;

import simpledb.execution.Predicate;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int[] buckets;
    private long totalNum;
    private final int min;
    private final int max;
    private final double bucketGap;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.totalNum = 0;
        this.bucketGap = (max - min) / (double) buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v > max || v < min) {
            return;
        }
        buckets[getBucketIndex(v)] += 1;
        totalNum++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        double ret;
        switch (op) {
            case EQUALS: {
                if (v < min || v > max) return 0;
                int index = getBucketIndex(v);
                ret = buckets[index] / bucketGap / totalNum;
                break;
            }
            case GREATER_THAN: {
                if (v < min) {
                    ret = 1;
                    break;
                } else if (v > max) {
                    ret = 0;
                    break;
                }
                int index = getBucketIndex(v);
                double prat_num = 0;
                prat_num += ((index + 1) * bucketGap + min - v) / bucketGap * buckets[index];
                for (int i = index + 1; i < buckets.length; i++) {
                    prat_num += buckets[i];
                }
                ret = prat_num / totalNum;
                break;
            }
            case LESS_THAN: {
                ret = 1 - estimateSelectivity(Predicate.Op.GREATER_THAN, v) - estimateSelectivity(Predicate.Op.EQUALS, v);
                break;
            }
            case LESS_THAN_OR_EQ: {
                ret = 1 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
                break;
            }
            case GREATER_THAN_OR_EQ: {
                ret = estimateSelectivity(Predicate.Op.EQUALS, v) + estimateSelectivity(Predicate.Op.GREATER_THAN, v);
                break;
            }
            case NOT_EQUALS: {
                ret = 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
                break;
            }
            default: {
                throw new NotImplementedException();
            }
        }
        return ret;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return totalNum / (double) (max - min);
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return super.toString();
    }

    private int getBucketIndex(int v) {
        int index = (int)((v - min) / bucketGap);
        if (index == buckets.length) {
            index--;
        }
        return index;
    }
}
