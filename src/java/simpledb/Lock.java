package simpledb;

import java.io.*;
import java.util.*;


public class Lock {

	private final Map<PageId, TransactionId> exLock;
	private final Map<PageId, Set<TransactionId>> shLock;
	
	public Lock() {
		exLock = new HashMap<PageId, TransactionId>();
		shLock = new HashMap<PageId, Set<TransactionId>>();
	}
	
	public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
		
		if(exLock.containsKey(pid) && exLock.get(pid).equals(tid)) {
			return true;
		}
		if(shLock.containsKey(pid) && shLock.get(pid).contains(tid)) {
			return true;
		}
		return false;
	}
	
	public synchronized void unLock(TransactionId tid, PageId pid) {
		if(exLock.containsKey(pid) && exLock.get(pid).equals(tid)) 
			exLock.remove(pid);
		if(shLock.containsKey(pid) && shLock.get(pid).contains(tid)) {
			//shLock.get(pid).remove(tid);
			Set<TransactionId> curr = shLock.get(pid);
			curr.remove(tid);
			shLock.put(pid, curr);
			if(shLock.get(pid).size() == 0) {
				shLock.remove(pid);
			}
		}
	}
	
	public synchronized void lockPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException {
		
		long start = System.currentTimeMillis();
		if(perm.toString().equals("READ_ONLY")) {
			while(exLock.containsKey(pid)) {
				if(System.currentTimeMillis() > start + 1000)
					throw new TransactionAbortedException();
				if(exLock.get(pid).equals(tid)) {
					exLock.remove(pid);
					Set<TransactionId> curr = new HashSet<TransactionId>();
					curr.add(tid);
					shLock.put(pid, curr);
					return;
				}
				try {
					Thread.sleep(20);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			if(shLock.containsKey(pid)) {
				Set<TransactionId> curr = shLock.get(pid);
				curr.add(tid);
				shLock.put(pid, curr);
			}
			else {
				Set<TransactionId> curr = new HashSet<TransactionId>();
				curr.add(tid);
				shLock.put(pid, curr);
			}
		}
		
		
		if(perm.toString().equals("READ_WRITE")) {
			while(exLock.containsKey(pid) || shLock.containsKey(pid)) {
				if(System.currentTimeMillis() > start + 1000)
					throw new TransactionAbortedException();
				if(exLock.containsKey(pid) && exLock.get(pid).equals(tid))
					return;
				if(shLock.containsKey(pid) && shLock.get(pid).contains(tid) && shLock.get(pid).size() == 1) {
					shLock.remove(pid);
					exLock.put(pid, tid);
					return;
				}
				try {
					Thread.sleep(20);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			exLock.put(pid, tid);
			
		}
	}
	
	
	public synchronized void transactionComplete(TransactionId tid) {
		List<PageId> curr = new ArrayList<>();
		for(PageId pid:exLock.keySet()) {
			if(exLock.get(pid).equals(tid))
				curr.add(pid);
		}
		for(PageId pid:curr)
			exLock.remove(pid);
		curr.clear();
		Set<TransactionId> tmp = new HashSet<>();
		for(PageId pid:shLock.keySet()) {
			tmp = shLock.get(pid);
			if(tmp.contains(tid)) {
				tmp.remove(tid);
			}
			if(tmp.isEmpty())
				curr.add(pid);
			shLock.put(pid, tmp);
		}
		for(PageId pid:curr)
			shLock.remove(pid);
		return;
	}
	
	public synchronized  List<PageId> getFlushPages(TransactionId tid) throws IOException{
		
		List<PageId> fPages = new ArrayList<>();
		for(PageId pid:exLock.keySet()) {
			if(exLock.get(pid).equals(tid))
				fPages.add(pid);
		}
		
		Set<TransactionId> tmp = new HashSet<>();
		for(PageId pid:shLock.keySet()) {
			tmp = shLock.get(pid);
			if(tmp.contains(tid)) {
				fPages.add(pid);
			}
		}
	    
		return fPages;
		
	}
	
	
	
}
