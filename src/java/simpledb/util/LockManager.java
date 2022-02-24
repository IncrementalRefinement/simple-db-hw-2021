package simpledb.util;

import simpledb.common.Permissions;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

public class LockManager {

    // TODO: The lock number might be expended too much in memory
    private Map<PageId, ReadWriteLock> pageId2LockMap;
    private Map<TransactionId, Map<PageId, Permissions>> tid2LockedPage2PermissionMap;

    public LockManager() {
        pageId2LockMap = new ConcurrentHashMap<>();
        tid2LockedPage2PermissionMap = new ConcurrentHashMap<>();
    }

    // 一个事务对于一个页只能持有一把锁，这里支持锁的 upgrade，但不支持 downgrade
    public void lock(TransactionId tid, PageId pageId, Permissions perm) {
        pageId2LockMap.putIfAbsent(pageId, new StampedLock().asReadWriteLock());
        ReadWriteLock pageLock = pageId2LockMap.get(pageId);

        switch (perm) {
            case READ_ONLY: {
                if (holdsLock(tid, pageId)) {
                    // 已经持有锁，既不重入，也不降级，也不需要更新 Map 中锁的等级
                    return;
                }
                pageLock.readLock().lock();
                break;
            }
            case READ_WRITE: {
                if (holdsLock(tid, pageId)) {
                    if (Permissions.READ_ONLY == tid2LockedPage2PermissionMap.get(tid).get(pageId)) {
                        // 如果需要进行 upgrade，先释放，再尝试获取
                        pageLock.readLock().unlock();
                        pageLock.writeLock().lock();
                    }
                    break;
                }
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
        ReadWriteLock pageLock = pageId2LockMap.get(pageId);
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
        if (!tid2LockedPage2PermissionMap.containsKey(tid)) {
            return false;
        }

        if (tid2LockedPage2PermissionMap.get(tid) == null) {
            return false;
        }

        return tid2LockedPage2PermissionMap.get(tid).containsKey(pageId);
    }
}
