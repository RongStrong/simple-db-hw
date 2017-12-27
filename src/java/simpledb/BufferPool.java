package simpledb;

import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.io.*;

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
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private int bufferSize;
    private Map<PageId,Page> pageBuffer = new HashMap<>();
    private int pagesNum;
    private Lock lock;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	bufferSize = numPages;
    	lock = new Lock();
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
    		lock.lockPage(tid, pid, perm);
    	
    	if(pageBuffer.containsKey(pid))
    		return pageBuffer.get(pid);
    	else {
    		if(pageBuffer.size()==bufferSize)
    			//throw new TransactionAbortedException();
    			evictPage();
    		DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
    		Page p = file.readPage(pid);
    		pageBuffer.put(pid, p);
    		return p;
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    		lock.unLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	
    	transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lock.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	if(commit){
    		this.flushPages(tid);
    		lock.transactionComplete(tid);
    	}
    	else{
    		List<PageId> fPages=lock.getFlushPages(tid);
    		lock.transactionComplete(tid);
    		for(PageId pid:fPages){
    			this.discardPage(pid);
    		}
    		
    		for(PageId pid:fPages){
    			int tableId=pid.getTableId();
    			pageBuffer.put(pid,Database.getCatalog().getDatabaseFile(tableId).readPage(pid));
    		}
    	}
    	
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
    		DbFile table = Database.getCatalog().getDatabaseFile(tableId);
    		ArrayList<Page> pList = table.insertTuple(tid, t);
    		for(Page p : pList) {
    			p.markDirty(true, tid);
    			pageBuffer.put(p.getId(), p);
    			
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
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	    DbFile table = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
    	    
    	    ArrayList<Page> pList = table.deleteTuple(tid, t);;
    		for(Page p : pList) {
    			p.markDirty(true, tid);
    			pageBuffer.put(p.getId(), p);
    			
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
    	for(PageId pid:pageBuffer.keySet()) {
    		flushPage(pid);
    	}
    	return;
    	
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
    	if(pageBuffer.containsKey(pid))
    		pageBuffer.remove(pid);
    	return;
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	try {
    		Page p = pageBuffer.get(pid);
    		TransactionId isDir = p.isDirty();
    		if(isDir!=null) {
    			Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
    			p.markDirty(false, isDir);
    		}
    	}
    	catch (Exception e){
    		throw new IOException();
    	}
    	return;
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	List<PageId> fPages=lock.getFlushPages(tid);
    	
    	for(PageId pid:fPages){
    		flushPage(pid);
    	}

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	try {
    		for(PageId pid:pageBuffer.keySet()) {
    			flushPage(pid);
    			discardPage(pid);
    			break;
    		}
			
    	}
    	catch(Exception e) {
    		throw new DbException(e.getMessage());
    	}
    	return;
    }

}