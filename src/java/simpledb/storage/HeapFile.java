package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.bind.annotation.XmlType;
import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
        // Should I do this?
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    // offset: simpledb.storage.HeapPageId.pageNumber
    public Page readPage(PageId pid) {
        // some code goes here
        byte[] data = HeapPage.createEmptyPageData();
        long offset = (long) pid.getPageNumber() * data.length;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            randomAccessFile.seek(offset);
            randomAccessFile.read(data);
            HeapPageId heapPageId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(heapPageId, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        byte[] data = page.getPageData();
        long offset = (long) page.getId().getPageNumber() * data.length;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.seek(offset);
            randomAccessFile.write(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> dirtyPages = new ArrayList<>();
        boolean hasEmptySlot = false;
        for (int pageNumber = 0; pageNumber < numPages(); pageNumber++) {
            // FIXME: 此处的 pageID 是从 tuple 里面捞出来的，但是 pageID 的 tableID 还可以从当前 HeapFile#getID() 生成
            PageId thePageID = new HeapPageId(getId(), pageNumber);
            HeapPage thePage = (HeapPage) Database.getBufferPool().getPage(tid, thePageID, Permissions.READ_WRITE);
            if (thePage.getNumEmptySlots() > 0) {
                thePage.insertTuple(t);
                dirtyPages.add(thePage);
                hasEmptySlot = true;
                break;
            }
        }

        if (!hasEmptySlot) {
            // create a new page and write the new page into disk
            byte[] data = HeapPage.createEmptyPageData();
            HeapPageId newPageID = new HeapPageId(getId(), numPages());
            HeapPage newPage = new HeapPage(newPageID, data);
            newPage.insertTuple(t);
            writePage(newPage);
        }

        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // TODO: Do I really need this shit method?
        HeapPage thePage = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        ArrayList<Page> ret = new ArrayList<>();
        thePage.deleteTuple(t);
        ret.add(thePage);
        return ret;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid, tupleDesc);
    }

}

class HeapFileIterator extends AbstractDbFileIterator {

    private static final Permissions DEFAULT_PERMISSION = Permissions.READ_ONLY;

    private boolean open;
    private final TupleDesc tupleDesc;
    private final TransactionId txId;
    private final HeapFile heapFile;
    private int currentPgeNumber;
    private Iterator<Tuple> currentPageIterator;
    private final int tableID;

    public HeapFileIterator(HeapFile heapFile, TransactionId txId, TupleDesc tupleDesc) {
        this.heapFile = heapFile;
        this.tupleDesc = tupleDesc;
        this.txId = txId;
        this.currentPgeNumber = 0;
        tableID = heapFile.getId();
        open = false;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        open = true;
        currentPgeNumber = 0;
        HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(txId, new HeapPageId(tableID, currentPgeNumber), DEFAULT_PERMISSION);
        currentPageIterator = currentPage.iterator();
    }

    @Override
    public void close() {
        super.close();
        open = false;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (!open) {
            // Not open yet
            return null;
        }

        if (currentPgeNumber >= heapFile.numPages()) {
            return null;
        } else if (currentPgeNumber == heapFile.numPages() - 1) {
            if (!currentPageIterator.hasNext()) {
                return null;
            } else {
                return currentPageIterator.next();
            }
        } else {
            while (!currentPageIterator.hasNext() && currentPgeNumber < heapFile.numPages()) {
                currentPgeNumber++;
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(txId, new HeapPageId(tableID, currentPgeNumber), DEFAULT_PERMISSION);
                currentPageIterator = page.iterator();
            }
            if (!currentPageIterator.hasNext()) {
                return null;
            } else {
                return currentPageIterator.next();
            }
        }
    }
}
