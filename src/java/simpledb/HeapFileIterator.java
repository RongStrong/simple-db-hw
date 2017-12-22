package simpledb;

import java.util.*;
import java.io.*;

public class HeapFileIterator implements DbFileIterator {
	
	List<Tuple> tupleList = new ArrayList<>();
	Iterator<Tuple> tpIterator = null;
	
	
	public HeapFileIterator(TransactionId tid, HeapFile file) {
		for(int pgNo = 0; pgNo < file.numPages(); pgNo++) {
    		HeapPageId pid = new HeapPageId(file.getId(), pgNo);
    		HeapPage p = (HeapPage) file.readPage(pid);
    		for(Tuple tp : p.tuples) {
    			tupleList.add(tp);
    		}
    		
    	}
	}
	
	public void open() throws DbException, TransactionAbortedException {
		tpIterator = tupleList.iterator();
	}
	
	public boolean hasNext() throws DbException, TransactionAbortedException {
		return tpIterator.hasNext();
	}
	
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		return tpIterator.next();
	}
	
	public void rewind() throws DbException, TransactionAbortedException {
		this.close();
		tpIterator = tupleList.iterator();
	}
	
	public void close() {
		tpIterator = null;
	}
	
}