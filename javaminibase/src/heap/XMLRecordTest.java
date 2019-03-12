package heap;

import java.io.IOException;

import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import heap.XMLRecord;

import tests.TestDriver;

public class XMLRecordTest extends TestDriver implements GlobalConst {

	private final static boolean OK = true;
	private final static boolean FAIL = false;
	
	private int choice;
	private final static int reclen = 32;
	
	public XMLRecordTest() {
		super("hptest");
		choice = 100;  
	}
	
	public boolean runTests () {

		System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");

		SystemDefs sysdef = new SystemDefs(dbpath,100,100,"Clock");

		// Kill anything that might be hanging around
		String newdbpath;
		String newlogpath;
		String remove_logcmd;
		String remove_dbcmd;
		String remove_cmd = "/bin/rm -rf ";

		newdbpath = dbpath;
		newlogpath = logpath;

		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;

		// Commands here is very machine dependent.  We assume
		// user are on UNIX system here
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println ("IO error: "+e);
		}

		remove_logcmd = remove_cmd + newlogpath;
		remove_dbcmd = remove_cmd + newdbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println ("IO error: "+e);
		}

		//Run the tests. Return type different from C++
		boolean _pass = runAllTests();

		//Clean up again
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		}
		catch (IOException e) {
			System.err.println ("IO error: "+e);
		}

		System.out.print ("\n" + "..." + testName() + " tests ");
		System.out.print (_pass==OK ? "completely successfully" : "failed");
		System.out.print (".\n\n");

		return _pass;
	}
	
	protected boolean test1() {
		System.out.println ("\n Test: Insert and scan fixed-size records\n");
		boolean status = OK;
		RID rid = new RID();
		Heapfile f = null;

		System.out.println ("  - Create a heap file\n");
		try {
			f = new Heapfile("file_1");
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println ("*** Could not create heap file\n");
			e.printStackTrace();
		}

		if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
				!= SystemDefs.JavabaseBM.getNumBuffers() ) {
			System.err.println ("*** The heap file has left pages pinned\n");
			status = FAIL;
		}

		if ( status == OK ) {
			System.out.println ("  - Add " + choice + " records to the file\n");
			for (int i =0; (i < choice) && (status == OK); i++) {

				//fixed length record
				XMLRecord rec = new XMLRecord(reclen);
				rec.start = i;
				rec.end = i+1;
				rec.level = i+2;
				rec.tagName = "Akshay";

				try {
					rid = f.insertRecord(rec.toByteArray());
				}
				catch (Exception e) {
					status = FAIL;
					System.err.println ("*** Error inserting record " + i + "\n");
					e.printStackTrace();
				}

				if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
						!= SystemDefs.JavabaseBM.getNumBuffers() ) {

					System.err.println ("*** Insertion left a page pinned\n");
					status = FAIL;
				}
			}

			try {
				if ( f.getRecCnt() != choice ) {
					status = FAIL;
					System.err.println ("*** File reports " + f.getRecCnt() + 
							" records, not " + choice + "\n");
				}
			}
			catch (Exception e) {
				status = FAIL;
				System.out.println (""+e);
				e.printStackTrace();
			}
		}

		// In general, a sequential scan won't be in the same order as the
		// insertions.  However, we're inserting fixed-length records here, and
		// in this case the scan must return the insertion order.

		Scan scan = null;

		if ( status == OK ) {	
			System.out.println ("  - Scan the records just inserted\n");

			try {
				scan = f.openScan();
			}
			catch (Exception e) {
				status = FAIL;
				System.err.println ("*** Error opening scan\n");
				e.printStackTrace();
			}

			if ( status == OK &&  SystemDefs.JavabaseBM.getNumUnpinnedBuffers() 
					== SystemDefs.JavabaseBM.getNumBuffers() ) {
				System.err.println ("*** The heap-file scan has not pinned the first page\n");
				status = FAIL;
			}
		}	

		if ( status == OK ) {
			int len, i = 0;
			XMLRecord rec = null;
			Tuple tuple = new Tuple();

			boolean done = false;
			while (!done) { 
				try {
					tuple = scan.getNext(rid);
					if (tuple == null) {
						done = true;
						break;
					}
				}
				catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}

				if (status == OK && !done) {
					try {
						rec = new XMLRecord(tuple);
					}
					catch (Exception e) {
						System.err.println (""+e);
						e.printStackTrace();
					}

					len = tuple.getLength();
					if ( len != reclen ) {
						System.err.println ("*** Record " + i + " had unexpected length " 
								+ len + "\n");
						status = FAIL;
						break;
					}
					else if ( SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
							== SystemDefs.JavabaseBM.getNumBuffers() ) {
						System.err.println ("On record " + i + ":\n");
						System.err.println ("*** The heap-file scan has not left its " +
								"page pinned\n");
						status = FAIL;
						break;
					}
					String name = "Akshay";
					
					System.out.println(rec);

//					if( (rec.start != 1)
//							|| (rec.end != 2)
//							|| (!name.equals(rec.tagName)) || (rec.level != 1) ) {
//						System.err.println ("*** Record " + i
//								+ " differs from what we inserted\n");
//						
//						System.err.println ("rec.start: "+ rec.start
//								+ " should be " + 1 + "\n");
//						System.err.println ("rec.end: "+ rec.end
//								+ " should be " + 2 + "\n");
//						System.err.println ("rec.level: "+ rec.level
//								+ " should be " + 1 + "\n");
//						System.err.println ("rec.name: " + rec.tagName
//								+ " should be " + "Akshay" + "\n");
//						status = FAIL;
//						break;
//					}
				}	
				++i;
			}

			//If it gets here, then the scan should be completed
			if (status == OK) {
				if ( SystemDefs.JavabaseBM.getNumUnpinnedBuffers() 
						!= SystemDefs.JavabaseBM.getNumBuffers() ) {
					System.err.println ("*** The heap-file scan has not unpinned " + 
							"its page after finishing\n");
					status = FAIL;
				}
				else if ( i != (choice) )
				{
					status = FAIL;

					System.err.println ("*** Scanned " + i + " records instead of "
							+ choice + "\n");
				}
			}	
		}

		if ( status == OK )
			System.out.println ("  Test completed successfully.\n");

		return status;
	}
	
	public static void main(String[] args) {
		XMLRecordTest hd = new XMLRecordTest();
		boolean dbstatus;

		dbstatus = hd.runTests();

		if (dbstatus != true) {
			System.err.println ("Error encountered during buffer manager tests:\n");
			Runtime.getRuntime().exit(1);
		}

		Runtime.getRuntime().exit(0);
	}
}