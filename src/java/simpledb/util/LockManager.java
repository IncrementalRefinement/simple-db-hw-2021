package simpledb.util;

import simpledb.common.Permissions;
import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LockManager {

    // TODO: Refactor this shit
    private Map<PageId, Set<Lock>> pageId2LocksMap;

    private class Lock {
        private TransactionId tid;
        private Permissions permission;

        public Lock(TransactionId tid, Permissions permission) {
            this.tid = tid;
            this.permission = permission;
        }

        public TransactionId getTid() {
            return  tid;
        }

        public Permissions getPermission() {
            return this.permission;
        }

        public void setPermission(Permissions permission) {
            this.permission = permission;
        }
    }

    public LockManager() {
        pageId2LocksMap = new ConcurrentHashMap<>();
    }

    public void lock(TransactionId tid, PageId pageId, Permissions perm) throws TransactionAbortedException {
        long beginTime = System.currentTimeMillis();
        long timeOut = beginTime + getRandomTimeOut();
        boolean lockAcquired = false;
        while (!lockAcquired) {
            long now = System.currentTimeMillis();
            if (now > timeOut) {
                break;
            }
            lockAcquired = acquireLock(tid, pageId, perm);
        }
        if (!lockAcquired) {
            throw new TransactionAbortedException();
        }
    }

    private synchronized boolean acquireLock(TransactionId tid, PageId pageId, Permissions perm) {
        // FIXME: the if-else clause is too tedious here
        if (pageId2LocksMap.get(pageId) == null) {
            pageId2LocksMap.put(pageId, new HashSet<>());
            pageId2LocksMap.get(pageId).add(new Lock(tid, perm));
            return true;
        } else {
            Set<Lock> locks = pageId2LocksMap.get(pageId);
            if (locks.size() == 0) {
                locks.add(new Lock(tid, perm));
                return true;
            }
            if (perm == Permissions.READ_WRITE) {
                if (locks.size() != 1) {
                    return false;
                } else {
                    Lock theLock = pageId2LocksMap.get(pageId).toArray(new Lock[0])[0];
                    if (!theLock.getTid().equals(tid)) {
                        return false;
                    }
                    if (theLock.getPermission() == Permissions.READ_ONLY) {
                        theLock.setPermission(Permissions.READ_WRITE);
                    }
                    return true;
                }
            } else if (perm == Permissions.READ_ONLY) {
                for (Lock lock : locks) {
                    if (lock.getTid().equals(tid)) {
                        return true;
                    }
                }
                if (locks.size() > 1) {
                    locks.add(new Lock(tid, perm));
                    return true;
                } else {
                    Lock theLock = pageId2LocksMap.get(pageId).toArray(new Lock[0])[0];
                    if (theLock.getPermission() == Permissions.READ_WRITE) {
                        return false;
                    } else {
                        locks.add(new Lock(tid, perm));
                        return true;
                    }
                }
            } else {
                throw new RuntimeException();
            }
        }
    }


    public synchronized void releaseLock(TransactionId tid, PageId pageId) {
        if (!holdsLock(tid, pageId)) {
            return;
        }
        pageId2LocksMap.get(pageId).removeIf(lock -> lock.getTid().equals(tid));
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pageId) {
        if (!pageId2LocksMap.containsKey(pageId)) {
            return false;
        }

        for (Lock lock : pageId2LocksMap.get(pageId)) {
            if (lock.getTid().equals(tid)) {
                return true;
            }
        }

        return false;
    }

    private long getRandomTimeOut() {
        return 1000 + new Random().nextInt(2000);
    }
}
