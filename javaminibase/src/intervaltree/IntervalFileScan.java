package intervaltree;

import java.io.*;

import btree.IndexFileScan;
import btree.KeyDataEntry;
import btree.ScanDeleteException;
import btree.ScanIteratorException;
import global.*;
import heap.*;

/**
 * BTFileScan implements a search/iterate interface to B+ tree 
 * index files (class BTreeFile).  It derives from abstract base
 * class IndexFileScan.  
 */
public class IntervalFileScan  extends IndexFileScan
             implements  GlobalConst
{

	public IntervalFileScan() {
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public KeyDataEntry get_next() throws ScanIteratorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void delete_current() throws ScanDeleteException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int keysize() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void DestroyIntervalTreeFileScan() {
		
	}
	
}






