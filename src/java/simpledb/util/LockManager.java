package simpledb.util;

import simpledb.common.Permissions;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {

    // TODO: The lock number might be expended too much in memory
    private Map<PageId, ReentrantReadWriteLock> pageId2LockMap;
    private Map<TransactionId, Map<PageId, Permissions>> tid2LockedPage2PermissionMap;

    public LockManager() {
        pageId2LockMap = new ConcurrentHashMap<>();
        tid2LockedPage2PermissionMap = new ConcurrentHashMap<>();
    }

    public void lock(TransactionId tid, PageId pageId, Permissions perm) {
        pageId2LockMap.putIfAbsent(pageId, new ReentrantReadWriteLock());
        ReentrantReadWriteLock pageLock = pageId2LockMap.get(pageId);

        switch (perm) {
            case READ_ONLY: {
                pageLock.readLock().lock();
                break;
            }
            case READ_WRITE: {
                pageLock.writeLock().lock();
                break;
            }
            default:
                throw new RuntimeException();
        }

        tid2LockedPage2PermissionMap.putIfAbsent(tid, new HashMap<>());
        tid2LockedPage2PermissionMap.get(tid).put(pageId, perm);
    }


    public void releaseLock(TransactionId tid, PageId pageId) {
        if (!holdsLock(tid, pageId)) {
            return;
        }
        ReentrantReadWriteLock pageLock = pageId2LockMap.get(pageId);
        Permissions perm = tid2LockedPage2PermissionMap.get(tid).remove(pageId);

        switch (perm) {
            case READ_ONLY: {
                pageLock.readLock().unlock();
                break;
            }
            case READ_WRITE: {
                pageLock.writeLock().unlock();
                break;
            }
            default:
                throw new RuntimeException();
        }

    }

    public boolean holdsLock(TransactionId tid, PageId pageId) {
        return tid2LockedPage2PermissionMap.get(tid).containsKey(pageId);
    }
}
