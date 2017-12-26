package simpledb;

import java.io.*;
import java.util.*;

public class Lock {

	Map<PageId, TransactionId> exLock;
	Map<PageId, Set<TransactionId>> shLock;
	
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
	
	
	
	
	
}
