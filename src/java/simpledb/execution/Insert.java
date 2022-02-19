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
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private static final TupleDesc TUPLE_DESC = new TupleDesc(new Type[]{Type.INT_TYPE});

    private final TransactionId txID;
    private OpIterator child;
    private final int tableId;
    private int insertedNumber;
    private boolean isOpen;
    private boolean runOut;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.txID = t;
        this.child = child;
        this.tableId = tableId;
        this.insertedNumber = 0;
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
        this.insertedNumber = 0;
        this.runOut = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
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
                bufferPool.insertTuple(txID, tableId, nextTuple);
                insertedNumber++;
            } catch (IOException e) {
                // TODO: don't know if this is right
                throw new DbException("IOException");
            }
        }

        Tuple ret = new Tuple(getTupleDesc());
        ret.setField(0, new IntField(insertedNumber));
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
