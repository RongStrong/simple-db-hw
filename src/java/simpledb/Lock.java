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
	
	public synchronized void lockPage(TransactionId tid, PageId pid, Permissions perm) {
		
		
		if(perm.toString().equals("READ_ONLY")) {
			while(exLock.containsKey(pid)) {
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
<<<<<<< HEAD
=======
				if(exLock.containsKey(pid) && exLock.get(pid).equals(tid))
					return;
>>>>>>> c421a43e6bd96f078444cfff5230addef5331ec0
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
	
	
	
	
	
}
