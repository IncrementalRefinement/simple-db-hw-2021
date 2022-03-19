package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.util.LockManager;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
// TODO: should i put synchronized everywhere?
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int maxPageNumber;
    private final List<Page> pageList;
    private final Map<PageId, Page> pageId2PageMap;
    private final Map<TransactionId, Set<PageId>> txId2PageIdMap;
    private final Map<PageId, Set<TransactionId>> pageId2txIdMap;
    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        maxPageNumber = numPages;
        pageList = new LinkedList<>();
        pageId2PageMap = new ConcurrentHashMap<>();
        txId2PageIdMap = new ConcurrentHashMap<>();
        pageId2txIdMap = new ConcurrentHashMap<>();
        lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        // FIXME: the same tid request with different permission level
        //    Also, the same page might be requested by multiple transaction
        lockManager.lock(tid, pid, perm);
        if (pageId2PageMap.containsKey(pid)) {
            addTransactionPageRelation(tid, pid);
            return pageId2PageMap.get(pid);
        } else {
            while (pageList.size() >= maxPageNumber) {
               evictPage();
            }
            DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page newPage = dbfile.readPage(pid);
            pageList.add(newPage);
            pageId2PageMap.put(pid, newPage);
            addTransactionPageRelation(tid, pid);
            return newPage;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
        removeTransactionPageRelation(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        if (txId2PageIdMap.get(tid) == null) {
            return;
        }
        PageId[] pageIdToRelease = txId2PageIdMap.get(tid).toArray(new PageId[0]);
        for (PageId pid : pageIdToRelease) {
            unsafeReleasePage(tid, pid);
        }
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2

        if (commit) {
            // Flush all pages to disk and reset dirty sign
            for (PageId pid : txId2PageIdMap.get(tid)) {
                Page page = pageId2PageMap.get(pid);
                if (page.isDirty() == tid) {
                    try {
                        flushPage(pid);
                        page.setBeforeImage();
                        page.markDirty(false, tid);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException();
                    }
                }
            }
        } else {
            // discard all the pages that has been modified by this TX
            for (PageId pid : txId2PageIdMap.get(tid)) {
                Page page = pageId2PageMap.get(pid);
                if (page.isDirty() == tid) {
                    discardPage(pid);
                }
            }

        }
        transactionComplete(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // TODO: what if page size of one insertion exceeds the max capacity of bufferpool?
        DbFile theFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = theFile.insertTuple(tid, t);
        for (Page dirtyPage : dirtyPages) {
            if (!pageId2PageMap.containsKey(dirtyPage.getId())) {
                if (pageList.size() >= maxPageNumber) {
                    evictPage();
                }
                lockManager.lock(tid, dirtyPage.getId(), Permissions.READ_WRITE);
                pageList.add(dirtyPage);
                pageId2PageMap.put(dirtyPage.getId(), dirtyPage);
            }
            dirtyPage.markDirty(true, tid);
    }
}

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // TODO: delete the tuple in file, and mark the resut as dirty, not right here
        DbFile theFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> dirtyPages = theFile.deleteTuple(tid, t);
        for (Page dirtyPage : dirtyPages) {
            if (!pageId2PageMap.containsKey(dirtyPage.getId())) {
                if (pageList.size() >= maxPageNumber) {
                    evictPage();
                }
                lockManager.lock(tid, dirtyPage.getId(), Permissions.READ_WRITE);
                pageList.add(dirtyPage);
                pageId2PageMap.put(dirtyPage.getId(), dirtyPage);
            }
            dirtyPage.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Page page : pageList) {
            if (page.isDirty() != null) {
                flushPage(page.getId());
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        Page thePage = pageId2PageMap.get(pid);
        pageList.remove(thePage);
        pageId2PageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        // append an update record to the log, with
        // a before-image and after-image.
        Page p = pageId2PageMap.get(pid);
        TransactionId dirtier = p.isDirty();
        if (dirtier != null) {
            Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p);
            Database.getLogFile().force();
        }

        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        file.writePage(p);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pageId : txId2PageIdMap.get(tid)) {
            flushPage(pageId);
            // removeTransactionPageRelation(tid, pageId);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // TODO: the eviction policy is really naive and ineffective, refine this when necessary!
        Page chosenPage = choosePageToEvict();
        assert chosenPage.isDirty() == null;
        // 1. 释放锁，删除关系
        TransactionId[] relatedTransactionID = pageId2txIdMap.get(chosenPage.getId()).toArray(new TransactionId[0]);
        for (TransactionId tid : relatedTransactionID) {
            unsafeReleasePage(tid, chosenPage.getId());
        }
        // 2. 从内存中删除页
        discardPage(chosenPage.getId());
    }

    private void addTransactionPageRelation(TransactionId tid, PageId pid) {
        txId2PageIdMap.computeIfAbsent(tid, k -> new HashSet<>());
        txId2PageIdMap.get(tid).add(pid);
        pageId2txIdMap.computeIfAbsent(pid, k -> new HashSet<>());
        pageId2txIdMap.get(pid).add(tid);
    }

    private void removeTransactionPageRelation(TransactionId tid, PageId pid) {
        txId2PageIdMap.get(tid).remove(pid);
        pageId2txIdMap.get(pid).remove(tid);
    }

    private Page choosePageToEvict() throws DbException {
        assert pageList.size() > 0;
        Page ret = null;
        for (Page page : pageList) {
            if (page.isDirty() == null) {
                ret = page;
                break;
            }
        }
        if (ret == null) {
            throw new DbException("All pages are dirty.");
        }
        return ret;
    }

//    private void handleLocksOnCleanPage(Page thePage) {
//        // TODO: any locks transactions may already hold to the evicted page and handle them appropriately in your implementation.
//        PageId pageId = thePage.getId();
//        for (TransactionId txID : pageId2txIdMap.get(pageId)) {
//            transactionComplete(txID, false);
//        }
//    }
}
