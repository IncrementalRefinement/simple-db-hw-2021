package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private static final TupleDesc TUPLE_DESC = new TupleDesc(new Type[]{Type.INT_TYPE});

    private final TransactionId txID;
    private OpIterator child;
    private int deletedNumber;
    private boolean isOpen;
    private boolean runOut;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.txID = t;
        this.child = child;
        this.deletedNumber = 0;
        this.isOpen = false;
        this.runOut = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return TUPLE_DESC;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        this.isOpen = true;
    }

    public void close() {
        // some code goes here
        super.close();
        this.isOpen = false;
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        if (!isOpen) {
            throw new IllegalStateException();
        }
        child.rewind();
        this.deletedNumber = 0;
        this.runOut = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (runOut) {
            return null;
        }

        BufferPool bufferPool = Database.getBufferPool();
        while (child.hasNext()) {
            Tuple nextTuple = child.next();
            try {
                bufferPool.deleteTuple(txID, nextTuple);
                deletedNumber++;
            } catch (IOException e) {
                // TODO: don't know if this is right
                throw new DbException("IOException");
            }
        }

        Tuple ret = new Tuple(getTupleDesc());
        ret.setField(0, new IntField(deletedNumber));
        runOut = true;
        return ret;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
