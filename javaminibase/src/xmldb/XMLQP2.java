package xmldb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import btree.BTreeFile;
import btree.IntervalKey;
import btree.StringKey;
import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.IntervalType;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import intervaltree.IntervalTreeFile;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.Iterator;
import iterator.NestedLoopsJoins;
import iterator.Projection;
import iterator.RelSpec;
import iterator.SortMerge2;
import iterator.TupleUtils;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;

public class XMLQP2 {

	private boolean OK = true;
	private boolean FAIL = false;
	boolean status = OK;
	private Vector<XMLTuple> xmlTuples;
	static XMLParser xmlParser = new XMLParser();
	static XMLQP2 qp2;
	
    static int currentInstanceIndex = 1;
	static int currentTagIndex = 1; // Used in the hashmap
	
    private static int colLength = 0;
    static int currentProjCount = 6;
	
	ArrayList<ArrayList<String>> sortedRules;
    HashMap<String, Integer> tagIndex = new HashMap<>();

	public void xmlDataInsert() throws HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, IOException {

		xmlTuples = new Vector<XMLTuple>();

		int numTuples = xmlParser.listOfXMLObjects.size();
		int numTuplesAttrs = 2;

		for (int i = 0; (i < numTuples); i++) {
			//fixed length record
			XMLInputTreeNode node = xmlParser.listOfXMLObjects.get(i);
			xmlTuples.addElement(new XMLTuple(node.interval.start, node.interval.end, node.interval.level, node.tagName));
		}


		String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointestdb";
		String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";

		String remove_cmd = "/bin/rm -rf ";
		String remove_logcmd = remove_cmd + logpath;
		String remove_dbcmd = remove_cmd + dbpath;
		String remove_joincmd = remove_cmd + dbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
			Runtime.getRuntime().exec(remove_joincmd);
		} catch (IOException e) {
			System.err.println("" + e);
		}


		SystemDefs sysdef = new SystemDefs(dbpath, 10000, 10000, "Clock");

		// creating the sailors relation
		AttrType[] Stypes = new AttrType[numTuplesAttrs];
		Stypes[0] = new AttrType(AttrType.attrInterval);
		Stypes[1] = new AttrType(AttrType.attrString);

		//SOS
		short[] Ssizes = new short[1];
		Ssizes[0] = 10; //first elt. is 30

		Tuple t = new Tuple();
		try {
			t.setHdr((short) numTuplesAttrs, Stypes, Ssizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		int size = t.size();

		// inserting the tuple into file "sailors"
		RID rid;
		Heapfile f = null;
		try {
			f = new Heapfile("test.in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) numTuplesAttrs, Stypes, Ssizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		for (int i = 0; i < numTuples; i++) {
			try {

				t.setIntervalFld(1, ((XMLTuple) xmlTuples.elementAt(i)).interval);
				System.out.println(((XMLTuple) xmlTuples.elementAt(i)).tagName + " " + i);
				t.setStrFld(2, ((XMLTuple) xmlTuples.elementAt(i)).tagName);

			} catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				status = FAIL;
				e.printStackTrace();
			}

			try {
				rid = f.insertRecord(t.returnTupleByteArray());
			} catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				status = FAIL;
				e.printStackTrace();
			}
		}
		// System.out.println(xmlParser.listOfXMLObjects.size());
		//		SystemDefs.JavabaseBM.flushAllPages();

		if (status != OK) {
			//bail out
			System.err.println("*** Error creating relation for sailors");
			Runtime.getRuntime().exit(1);
		}
		System.out.println("");
		System.out.println("");
		System.out.println("DONE");


		//------------------------------------------------------------------------------------------------------------------------------------//

		Scan scan = null;

		try {
			scan = new Scan(f);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		// create the index file
		IntervalTreeFile itf = null;
		BTreeFile btf = null;

		try {

			itf = new IntervalTreeFile("IntervalTreeIndex", AttrType.attrInterval, 12, 1/*delete*/);
			btf = new BTreeFile("BTreeIndex", AttrType.attrString, 10, 1/*delete*/);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		System.out.println("BTreeIndex created successfully.\n");

		rid = new RID();
		IntervalType iKey = null;
		String tKey = null;
		Tuple temp = null;

		try {
			temp = scan.getNext(rid);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		while (temp != null) {
			t.tupleCopy(temp);

			try {
				iKey = t.getIntervalFld(1);
				tKey = t.getStrFld(2);
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}

			try {
				itf.insert(new IntervalKey(iKey), rid);
				btf.insert(new StringKey(tKey), rid);
				//
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}

			try {
				temp = scan.getNext(rid);
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
		}

		// close the file scan
		scan.closescan();

	}

	/*
	 * firstTuple -> tuples from tagIndexSearch for first tag in the rule
	 * lastTag -> name of the second tag in the rule
	 * op -> AD/PC
	 * f -> unique file in which single rule results are written
	 * */
	public Heapfile indexIntervalSearchFile(Tuple firstTuple, String lastTag, String op, Heapfile f) throws IndexException, IOException, FieldNumberOutOfBoundException, UnknowAttrType, TupleUtilsException, HFException, HFBufMgrException, HFDiskMgrException {

		System.out.println("------------------------------------------------------------Index Interval File Search Started ------------------------------------------------------------");
		
		
		
		AttrType[] Stypes = new AttrType[2];
		Stypes[0] = new AttrType(AttrType.attrInterval);
		Stypes[1] = new AttrType(AttrType.attrString);

		
		short[] Ssizes = new short[1];
		Ssizes[0] = 10; //first elt. is 30

		FldSpec[] projlist = new FldSpec[2];
		projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		projlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		
		AttrType[] ResultTypes = new AttrType[4];
		ResultTypes[0] = new AttrType(AttrType.attrInterval);
		ResultTypes[1] = new AttrType(AttrType.attrString);
		ResultTypes[2] = new AttrType(AttrType.attrInterval);
		ResultTypes[3] = new AttrType(AttrType.attrString);
		
		short[] resultSizes = new short[2];
		resultSizes[0] = 10;
		resultSizes[1] = 10;
		
		FldSpec[] resultProjlist = new FldSpec[4];
		resultProjlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		resultProjlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		resultProjlist[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		resultProjlist[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);

		int attrOperator;
		if("PC".equals(op)) {
			attrOperator = AttrOperator.aopCP;
		} else {
			attrOperator = AttrOperator.aopAD;
		}
		
		//Get FirstTag interval
		IntervalType intTest = new IntervalType(firstTuple.getIntervalFld(1).start, firstTuple.getIntervalFld(1).end, firstTuple.getIntervalFld(1).level);

		//Set up CondExpr for A B AD using 
		CondExpr[] select = new CondExpr[3];

		select[0] = new CondExpr();
		select[0].op = new AttrOperator(attrOperator);
		select[0].type1 = new AttrType(AttrType.attrSymbol);
		select[0].type2 = new AttrType(AttrType.attrInterval);
		select[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		select[0].operand2.interval = intTest;
		select[0].flag = 1;
		select[0].next = select[1];
		
		select[1] = new CondExpr();
		select[1].op = new AttrOperator(AttrOperator.aopEQ);
		select[1].type1 = new AttrType(AttrType.attrSymbol);
		select[1].type2 = new AttrType(AttrType.attrString);
		select[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		select[1].operand2.string = lastTag;
		select[2] = null;

		

		if("*".equals(lastTag)) {
			select[1] = null;
		}


		// start index scan
		IndexScan iscan = null;
		try {
			iscan = new IndexScan(new IndexType(IndexType.I_Index), "test.in", "IntervalTreeIndex", Stypes, Ssizes, 2, 2, projlist, select, 1, false);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		iscan.close();		
		

		Tuple secondTuple = null;
		Tuple resultTuple = new Tuple();
		TupleUtils.setup_op_tuple(resultTuple, ResultTypes, Stypes, 2, Stypes, 2, Ssizes, Ssizes, resultProjlist, 4);
		String outval = null;
		String outval2 = null;
		

		try {
			secondTuple = iscan.get_next();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		boolean flag = true;

		while (secondTuple != null) {
			
			//For every result from intervalIndexSearch join with first to get a new tuple
			Projection.Join(firstTuple, Stypes, secondTuple, Stypes, resultTuple, resultProjlist, 4);

			IntervalType intervalResult = null;
			IntervalType intervalResult2 = null;
			
			try {
				f.insertRecord(resultTuple.returnTupleByteArray());
			} catch(Exception e) {
				e.printStackTrace();
			}
			
			
			try {
				outval = resultTuple.getStrFld(2);
				intervalResult = resultTuple.getIntervalFld(1);
				outval2 = resultTuple.getStrFld(4);
				intervalResult2 = resultTuple.getIntervalFld(3);
//				
				System.out.print("TagName = " + outval + " Start = " + intervalResult.start + " End = " + intervalResult.end + " Level = " + intervalResult.level);
				System.out.println("|| TagName = " + outval2 + " Start = " + intervalResult2.start + " End = " + intervalResult2.end + " Level = " + intervalResult2.level);

			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}


			try {
				secondTuple = iscan.get_next();
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
		}
		// clean up
		try {
			iscan.close();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		} 
		System.out.println("------------------------------------------------------------Index Interval File Search Complete ------------------------------------------------------------\n\n\n");
		return f;
	}

	
	public FileScan indexIntervalSearchScan(Tuple firstTuple, String lastTag, String op, int fileCounter) throws IndexException, IOException, FieldNumberOutOfBoundException, UnknowAttrType, TupleUtilsException, FileScanException, InvalidRelation {

		System.out.println("------------------------------------------------------------Index Interval Search Started ------------------------------------------------------------");
		
		
		AttrType[] Stypes = new AttrType[2];
		Stypes[0] = new AttrType(AttrType.attrInterval);
		Stypes[1] = new AttrType(AttrType.attrString);

		
		short[] Ssizes = new short[1];
		Ssizes[0] = 10; //first elt. is 30

		FldSpec[] projlist = new FldSpec[2];
		projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		projlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		
		AttrType[] ResultTypes = new AttrType[4];
		ResultTypes[0] = new AttrType(AttrType.attrInterval);
		ResultTypes[1] = new AttrType(AttrType.attrString);
		ResultTypes[2] = new AttrType(AttrType.attrInterval);
		ResultTypes[3] = new AttrType(AttrType.attrString);
		
		short[] resultSizes = new short[2];
		resultSizes[0] = 10;
		resultSizes[1] = 10;
		
		FldSpec[] resultProjlist = new FldSpec[4];
		resultProjlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		resultProjlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		resultProjlist[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		resultProjlist[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);

		int attrOperator;
		if("PC".equals(op)) {
			attrOperator = AttrOperator.aopCP;
		} else {
			attrOperator = AttrOperator.aopAD;
		}
		
		//Get FirstTag interval
		IntervalType intTest = new IntervalType(firstTuple.getIntervalFld(1).start, firstTuple.getIntervalFld(1).end, firstTuple.getIntervalFld(1).level);

		//Set up CondExpr for A B AD using 
		CondExpr[] select = new CondExpr[3];

		select[0] = new CondExpr();
		select[0].op = new AttrOperator(attrOperator);
		select[0].type1 = new AttrType(AttrType.attrSymbol);
		select[0].type2 = new AttrType(AttrType.attrInterval);
		select[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		select[0].operand2.interval = intTest;
		select[0].flag = 1;
		select[0].next = select[1];
		
		select[1] = new CondExpr();
		select[1].op = new AttrOperator(AttrOperator.aopEQ);
		select[1].type1 = new AttrType(AttrType.attrSymbol);
		select[1].type2 = new AttrType(AttrType.attrString);
		select[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		select[1].operand2.string = lastTag;
		select[2] = null;

		

		if("*".equals(lastTag)) {
			select[1] = null;
		}


		// start index scan
		IndexScan iscan = null;
		try {
			iscan = new IndexScan(new IndexType(IndexType.I_Index), "test.in", "IntervalTreeIndex", Stypes, Ssizes, 2, 2, projlist, select, 1, false);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		iscan.close();

		//return iscan;
		
		//File that stores the singlequery results
		Heapfile f = null;
		String filename = "singlequeryresults" + fileCounter + ".in";
		try {
			f = new Heapfile(filename);
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}
		
		
		

		Tuple secondTuple = null;
		Tuple resultTuple = new Tuple();
		TupleUtils.setup_op_tuple(resultTuple, ResultTypes, Stypes, 2, Stypes, 2, Ssizes, Ssizes, resultProjlist, 4);
		String outval = null;
		String outval2 = null;
		

		try {
			secondTuple = iscan.get_next();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		boolean flag = true;

		while (secondTuple != null) {
			
			//For every result from intervalIndexSearch join with first to get a new tuple
			Projection.Join(firstTuple, Stypes, secondTuple, Stypes, resultTuple, resultProjlist, 4);

			IntervalType intervalResult = null;
			IntervalType intervalResult2 = null;
			
			try {
				f.insertRecord(resultTuple.returnTupleByteArray());
			} catch(Exception e) {
				e.printStackTrace();
			}
			
			
			try {
				outval = resultTuple.getStrFld(2);
				intervalResult = resultTuple.getIntervalFld(1);
				outval2 = resultTuple.getStrFld(4);
				intervalResult2 = resultTuple.getIntervalFld(3);
//				
				System.out.print("TagName = " + outval + " Start = " + intervalResult.start + " End = " + intervalResult.end + " Level = " + intervalResult.level);
				System.out.println("|| TagName = " + outval2 + " Start = " + intervalResult2.start + " End = " + intervalResult2.end + " Level = " + intervalResult2.level);

			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}


			try {
				secondTuple = iscan.get_next();
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
		}
		// clean up
		try {
			iscan.close();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		} 
		
		FileScan scan = new FileScan(filename, ResultTypes, resultSizes,(short) 4, (short) 4, resultProjlist, null);
		Tuple dummy = null;
		try {
			dummy = iscan.get_next();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		
		while(dummy != null) {
			try {
				String outvalDummy = dummy.getStrFld(2);
				IntervalType intervalResultDummy = dummy.getIntervalFld(1);
				String outval2Dummy = dummy.getStrFld(4);
				IntervalType intervalResult2Dummy = dummy.getIntervalFld(3);
				
				System.out.print("TagName = " + outvalDummy + " Start = " + intervalResultDummy.start + " End = " + intervalResultDummy.end + " Level = " + intervalResultDummy.level);
				System.out.println("|| TagName = " + outval2Dummy + " Start = " + intervalResult2Dummy.start + " End = " + intervalResult2Dummy.end + " Level = " + intervalResult2Dummy.level);

			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
		}
		
		System.out.println("------------------------------------------------------------Index Tag Search Complete ------------------------------------------------------------\n\n\n");
		return scan;
	}
	
	public IndexScan indexTagSearch(String tag){

		System.out.println("------------------------------------------------------------Index Tag Search Started ------------------------------------------------------------");
		int numTuplesAttrs = 2;

		//Set up atrributes types
		AttrType[] Stypes = new AttrType[numTuplesAttrs];
		Stypes[0] = new AttrType (AttrType.attrInterval);
		Stypes[1] = new AttrType (AttrType.attrString);

		//Set up the size for String type attribute
		short[] Ssizes;
		Ssizes = new short [1];
		Ssizes[0] = 10;


		// create a tuple of appropriate size
		Tuple t = new Tuple();
		try {
			t.setHdr((short) 2, Stypes, Ssizes);
		}
		catch (Exception e) {

			e.printStackTrace();
		}
		//Fetch the tuple size
		int size = t.size();
		t = new Tuple(size);

		try {
			t.setHdr((short) 2, Stypes, Ssizes);
		}
		catch (Exception e) {

			e.printStackTrace();
		}


		//Set up the projections for the result
		FldSpec[] projlist = new FldSpec[2];
		projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		projlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);


		//Set up the condition expression for tag based index scan search
		CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrString);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		expr[0].operand2.string = tag;
		expr[0].next = null;
		expr[1] = null;

		//Start index scan
		IndexScan iscan = null;
		try {
			iscan = new IndexScan(new IndexType(IndexType.B_Index), "test.in", "BTreeIndex", Stypes, Ssizes, 2, 2, projlist, expr, 4, false);
		}
		catch (Exception e) {

			e.printStackTrace();
		}

		return iscan;
//		
//		//Set up variables to hold results
//		String stringResultFirst = null;
//		IntervalType intervalResultFirst = new IntervalType();
//		List<XMLInputTreeNode> tagResults = new ArrayList<XMLInputTreeNode>();
//		List<Tuple> tagTuples = new ArrayList<Tuple>();
//
//		//Get tuples one at a time
//		try {
//			t = iscan.get_next();
//			tagTuples.add(t);
//		}
//		catch (Exception e) {
//
//			e.printStackTrace();
//		}
//
//		if (t == null) {
//			System.err.println("Index tag search -- no record retrieved.");
//			return null;
//		}
//
//		//Fetch and print one tuple at a time
//		while( t!= null){
//
//			try {
//
//				stringResultFirst = t.getStrFld(2);
//				intervalResultFirst = t.getIntervalFld(1);
//
//
//				tagResults.add(new XMLInputTreeNode(stringResultFirst, intervalResultFirst));
//
//
//			}
//			catch (Exception e) {
//
//				e.printStackTrace();
//			}
//
//			System.out.println("Tag name = " + stringResultFirst + " Start = " + intervalResultFirst.start + " End = " + intervalResultFirst.end + " Level = " + intervalResultFirst.level);
//
//			try {
//				t = iscan.get_next();
//			}
//			catch (Exception e) {
//
//				e.printStackTrace();
//			}
//		}
//
//
//		//Clean up
//		try {
//			iscan.close();
//		}
//		catch (Exception e) {
//
//			e.printStackTrace();
//		}
//
//		System.out.println("------------------------------------------------------------Index Tag Search Complete ------------------------------------------------------------\n\n\n");
//
//		return tagResults;


	}

	/*
	 * firstTag -> first tag of the rule
	 * lastTag -> second tag of the rule
	 * op -> AD/PC
	 * fileCounter -> unique number used to create unique filenames for each rule. Please ensure you pass some unique rule
	 * */
	public String queryRuleIteratorFile(String firstTag, String lastTag, String op, int fileCounter, String prefix) throws HFException, HFBufMgrException, HFDiskMgrException, IOException, FileScanException, TupleUtilsException, InvalidRelation, IndexException {
		IndexScan iscan = qp2.indexTagSearch(firstTag);
		
		String filename = "singlequeryresults" + prefix + fileCounter + ".in";
		Heapfile f = null;
		try {
			f = new Heapfile(filename);
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}
		
		Tuple t = null;
		try {
			t = iscan.get_next();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}	


		while(t != null) {
			try {
				qp2.indexIntervalSearchFile(t, lastTag, op, f);
				t = iscan.get_next();
				
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
			
		}
		
		return filename;
		
		
//		Heapfile f = null;
//
//		String filename = "singlequeryresults" + fileCounter + ".in";
//		try {
//			f = new Heapfile(filename);
//		} catch (Exception e) {
//			System.err.println("*** error in Heapfile constructor ***");
//			status = FAIL;
//			e.printStackTrace();
//		}

//		for(XMLInputTreeNode node : tagResults) {
//			IndexScan it = qp1.indexIntervalSearch(node.interval, lastTag, op);
//
//			Tuple t = null;
//			try {
//				t = it.get_next();
//			} catch (Exception e) {
//				status = FAIL;
//				e.printStackTrace();
//			}	
//
//
//			while(t != null) {
//				try {
//					f.insertRecord(t.returnTupleByteArray());
//				} catch(Exception e) {
//					e.printStackTrace();
//				}
//				try {
//					t = it.get_next();
//				} catch (Exception e) {
//					status = FAIL;
//					e.printStackTrace();
//				}	
//			}
//
//
//		}


		// creating the sailors relation
//		AttrType[] Stypes = new AttrType[2];
//		Stypes[0] = new AttrType(AttrType.attrInterval);
//		Stypes[1] = new AttrType(AttrType.attrString);
//
//
//		//SOS
//		short[] Ssizes = new short[1];
//		Ssizes[0] = 10; 
//
//
//		FldSpec[] projlist = new FldSpec[2];
//		projlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
//		projlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
//		//				projlist[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
//		//				projlist[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
//
//
//		FileScan scan = new FileScan(filename, Stypes, Ssizes,(short) 2, (short) 2, projlist, null);
//
//
//		Tuple t = null;
//
//		try {
//			t = scan.get_next();
//		} catch (Exception e) {
//			status = FAIL;
//			e.printStackTrace();
//		}	
//		while(t!=null) {
//			String outval;
//			IntervalType intervalResult;
//			try {
//				outval = t.getStrFld(2);
//				intervalResult = t.getIntervalFld(1);
//				System.out.println("TagName = " + outval + " Start = " + intervalResult.start + " End = " + intervalResult.end + " Level = " + intervalResult.level);
//
//			} catch (Exception e) {
//				status = FAIL;
//				e.printStackTrace();
//			}
//			try {
//				t = scan.get_next();
//			} catch (Exception e) {
//				status = FAIL;
//				e.printStackTrace();
//			}
//		}



	}

	public FileScan queryRuleIteratorScan(String firstTag, String lastTag, String op, int fileCounter, String prefix) throws HFException, HFBufMgrException, HFDiskMgrException, IOException, IndexException, FileScanException, TupleUtilsException, InvalidRelation {
		
		
		String filename = queryRuleIteratorFile(firstTag, lastTag, op, fileCounter, prefix);
		Heapfile f = null;
		try {
			f = new Heapfile(filename);
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}
		
		AttrType[] ResultTypes = new AttrType[4];
		ResultTypes[0] = new AttrType(AttrType.attrInterval);
		ResultTypes[1] = new AttrType(AttrType.attrString);
		ResultTypes[2] = new AttrType(AttrType.attrInterval);
		ResultTypes[3] = new AttrType(AttrType.attrString);
		
		short[] resultSizes = new short[2];
		resultSizes[0] = 10;
		resultSizes[1] = 10;
		
		FldSpec[] resultProjlist = new FldSpec[4];
		resultProjlist[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		resultProjlist[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		resultProjlist[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		resultProjlist[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
		
		FileScan scan = new FileScan(filename, ResultTypes, resultSizes,(short) 4, (short) 4, resultProjlist, null);
		
		
		return scan;
	}
	
	public SortMerge2 SMJQP(FileScan it1, FileScan it2) {
		    	        
        AttrType [] Stypes = new AttrType[4];
        Stypes[0] = new AttrType (AttrType.attrInterval);
        Stypes[1] = new AttrType (AttrType.attrString);
        Stypes[2] = new AttrType (AttrType.attrInterval);
        Stypes[3] = new AttrType (AttrType.attrString);
        
        short [] Ssizes = new short[2];
        Ssizes[0] = 10;
        Ssizes[1] = 10;
        

//        AttrType [] Stypes2 = new AttrType[2];
//        Stypes2[0] = new AttrType (AttrType.attrInterval);
//        Stypes2[1] = new AttrType (AttrType.attrString);

        FldSpec [] projectionList = new FldSpec[6];
        projectionList[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projectionList[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        projectionList[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        projectionList[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
        projectionList[4] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
        projectionList[5] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
        
        
        
        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
        SortMerge2 tempInstance = null;
        
    	CondExpr[] expr = new CondExpr[2];
    	expr[0] = new CondExpr();
        expr[1] = null;
        expr[0].next  = null;

        expr[0].op = new AttrOperator(AttrOperator.aopEQ);
        
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        expr[0].flag = 1;       
        
        try {
            tempInstance = new SortMerge2(Stypes, 4, Ssizes, Stypes, 4, Ssizes, 1, 12, 1, 12, 10, it1, it2, false, false, ascending, expr, projectionList, 6);
        }
        catch (Exception e) {
            System.err.println("*** join error in SortMerge constructor ***");
            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }
        
        return tempInstance;
	}
	
	public SortMerge2 SMJQPMixed(SortMerge2 it1, FileScan it2) {
        
        AttrType [] Stypes = new AttrType[4];
        Stypes[0] = new AttrType (AttrType.attrInterval);
        Stypes[1] = new AttrType (AttrType.attrString);
        Stypes[2] = new AttrType (AttrType.attrInterval);
        Stypes[3] = new AttrType (AttrType.attrString);
        
        short [] Ssizes = new short[2];
        Ssizes[0] = 10;
        Ssizes[1] = 10;
        

//        AttrType [] Stypes2 = new AttrType[2];
//        Stypes2[0] = new AttrType (AttrType.attrInterval);
//        Stypes2[1] = new AttrType (AttrType.attrString);

        FldSpec [] projectionList = new FldSpec[6];
        projectionList[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projectionList[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        projectionList[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        projectionList[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
        projectionList[4] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
        projectionList[5] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
        
        
        
        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
        SortMerge2 tempInstance = null;
        
    	CondExpr[] expr = new CondExpr[2];
    	expr[0] = new CondExpr();
        expr[1] = null;
        expr[0].next  = null;

        expr[0].op = new AttrOperator(AttrOperator.aopEQ);
        
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        expr[0].flag = 1;       
        
        try {
            tempInstance = new SortMerge2(Stypes, 4, Ssizes, Stypes, 4, Ssizes, 1, 12, 1, 12, 10, it1, it2, false, false, ascending, expr, projectionList, 6);
        }
        catch (Exception e) {
            System.err.println("*** join error in SortMerge constructor ***");
            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }
        
        return tempInstance;
	}
	
	public static ArrayList<ArrayList<String>> getSortedRules(ArrayList<String> tags, ArrayList<ArrayList<String>> rules){
        ArrayList<Integer> isRuleVisited = new ArrayList<>();
        ArrayList<Integer> isTagVisited = new ArrayList<>();
        ArrayList<ArrayList<String>> sortedRules = new ArrayList<ArrayList<String>>();
        Queue<String> q = new LinkedList<>();

        //Mark all rules as unvisited first
        for(int i=0; i<rules.size(); i++){
            isRuleVisited.add(0);
        }
        // isRuleVisited.set(0,1);
        for(int i=0; i<tags.size(); i++){
            isTagVisited.add(0);
        }

        ((LinkedList<String>) q).add(rules.get(0).get(0));
        ((LinkedList<String>) q).add(rules.get(0).get(1));
        isTagVisited.set(tags.indexOf(rules.get(0).get(0)), 1);
        // isTagVisited.set(tags.indexOf(rules.get(0).get(1)), 1);


        while(q.size() != 0){
            String tag = q.remove();
            isTagVisited.set(tags.indexOf(tag),1);
            for(int i = 0; i< rules.size(); i++){
                if(isRuleVisited.get(i) == 0){

                    if(rules.get(i).get(0).equals(tag)){
                        isRuleVisited.set(i, 1);
                        sortedRules.add(rules.get(i));
                        if(isTagVisited.get(tags.indexOf(tag)) == 0){
                            q.add(tag);

                        }
                        if(isTagVisited.get(tags.indexOf(rules.get(i).get(1))) == 0){
                            q.add(rules.get(i).get(1));
                        }

                    }
                    if(rules.get(i).get(1).equals(tag)){
                        isRuleVisited.set(i, 1);
                        sortedRules.add(rules.get(i));
                        if(isTagVisited.get(tags.indexOf(tag)) == 0){
                            q.add(tag);
                        }
                        if(isTagVisited.get(tags.indexOf(rules.get(i).get(0))) == 0){
                            q.add(rules.get(i).get(0));
                        }

                    }
                }
            }
        }



        return sortedRules;
    }
    
    public ArrayList<ArrayList<String>> wrapperForSortedRules(String fileName) {
    	// assume this reads the query file, and produces a list of tag names
    	try{
            File file = new File("/home/ronak/DBMSi Project/Phase3/dbmsiPhase2/javaminibase/src/xmldbTestXML/" + fileName + ".txt");
            Scanner scan =new Scanner(file);

            ArrayList<String> tags = new ArrayList<>();
            ArrayList<ArrayList<String>> rules = new ArrayList<ArrayList<String>>();


            //Scan numberoftags, tags and rules from file
            int numberOfTags = scan.nextInt();
            for(int i=0; i<numberOfTags; i++){
                String tag = scan.next();
                if(tag.length() > 5)
                    tag = tag.substring(0,5);
                tags.add(tag);
                //  System.out.println(tags.get(i));
            }
            int j = 0;
            while(scan.hasNext()){
                ArrayList<String> temp = new ArrayList<>();
                int leftTag = scan.nextInt();
                int rightTag = scan.nextInt();
                String relation = scan.next();
                //System.out.println(leftTag + " " + rightTag + " " + relation);
                temp.add(tags.get(leftTag-1));
                temp.add(tags.get(rightTag-1));
                temp.add(relation);
                //   System.out.println(temp.get(0) + " " + temp.get(1) + " " + temp.get(2));
                rules.add(temp);

            }
            ArrayList<String> reversedtags = new ArrayList<>();
            for(int i= tags.size()-1; i >=0; i--){
                reversedtags.add(tags.get(i));
            }

            ArrayList<ArrayList<String>> reversedRules = new ArrayList<ArrayList<String>>();
            for(int i = rules.size()-1; i >= 0; i--) {
                reversedRules.add(rules.get(i));
            }

            sortedRules = XMLQueryParsing.getSortedRules(tags, rules);
            return sortedRules;
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    	return new ArrayList<ArrayList<String>>();
    }
    
	public String insertResultsIntoHeapFile(String fileName, Iterator it) {
		Heapfile f = null;

		String heapFileName = fileName + ".in";
		try {
			f = new Heapfile(heapFileName);
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}

		Tuple t = null;
		try {
			t = it.get_next();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		while (t != null) {
			try {
				f.insertRecord(t.returnTupleByteArray());
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				t = it.get_next();
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
		}

		System.out.println("Inserted into the heapfile successfully...");
		return heapFileName;
	}
	
	public void printResultsOfPatternTree(Iterator it) {
		Tuple t = null;

		try {
			t = it.get_next();
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
		int counter = 0;
		while(t!=null) {
			String outval;
			IntervalType intervalResult;
			String outval2;
			IntervalType intervalResult2;
			String outval3;
			IntervalType intervalResult3;
			
			try {
				outval = t.getStrFld(2);
				intervalResult = t.getIntervalFld(1);
				outval2 = t.getStrFld(4);
				intervalResult2 = t.getIntervalFld(3);
				intervalResult3 = t.getIntervalFld(5);
				outval3 = t.getStrFld(6);
				
				System.out.print("TagName = " + outval + " Start = " + intervalResult.start + " End = " + intervalResult.end + " Level = " + intervalResult.level);
				System.out.print("|| TagName = " + outval2 + " Start = " + intervalResult2.start + " End = " + intervalResult2.end + " Level = " + intervalResult2.level);
				System.out.println("|| TagName = " + outval3 + " Start = " + intervalResult3.start + " End = " + intervalResult3.end + " Level = " + intervalResult3.level);
				counter++;
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				t = it.get_next();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("Total Records: " + counter);
	}
	
	public void reinitialize() {
		OK = true;
		FAIL = false;
		status = OK;
		currentInstanceIndex = 1;
		currentTagIndex = 1; // Used in the hashmap
		
	    colLength = 0;
	    currentProjCount = 6;
	    
		sortedRules = new ArrayList<ArrayList<String>>();
	    tagIndex = new HashMap<>();
	}
	
	public void QP2Wrapper() throws HFException, HFBufMgrException, HFDiskMgrException, IndexException, FileScanException, TupleUtilsException, InvalidRelation, IOException {
		
		// PATTERN TREE 1
		// get the sorted rules
    	ArrayList<ArrayList<String>> sortedRules = this.wrapperForSortedRules("XMLPatternTree1");
    	
    	// populate first two sort merge instances
    	FileScan it11 = this.queryRuleIteratorScan(sortedRules.get(0).get(0), sortedRules.get(0).get(1), sortedRules.get(0).get(2), 1, "ONE");
    	FileScan it12 = this.queryRuleIteratorScan(sortedRules.get(1).get(0), sortedRules.get(1).get(1), sortedRules.get(1).get(2), 2, "ONE");
    	
    	
    	// execute the query plan for the first pattern tree
    	SortMerge2 patternTreeOneIterator = this.executeQP2ForAPatternTree(it11, it12, "ONE");
    	
    	// Print Results for pattern tree 1
    	this.printResultsOfPatternTree(patternTreeOneIterator);
    	
    	// write the results into a heap file
    	this.insertResultsIntoHeapFile("patternTree1Results", patternTreeOneIterator);
    	
    	/* PATTERN TREE 2 */
    	// get the sorted rules
    	this.reinitialize();
    	
    	System.out.println("Pattern Tree 2...");
    	sortedRules = new ArrayList<ArrayList<String>>();
    	
    	sortedRules = this.wrapperForSortedRules("XMLPatternTree2");
    	
    	System.out.println(sortedRules);
    	    	
    	// populate the first two sort merge instances
    	FileScan it21 = this.queryRuleIteratorScan(sortedRules.get(0).get(0), sortedRules.get(0).get(1), sortedRules.get(0).get(2), 1, "TWO");
    	FileScan it22 = this.queryRuleIteratorScan(sortedRules.get(1).get(0), sortedRules.get(1).get(1), sortedRules.get(1).get(2), 2, "TWO");
    	
    	// execute the query plan for the second pattern tree    	
    	SortMerge2 patternTreeTwoIterator = this.executeQP2ForAPatternTree(it21, it22, "TWO");
    	
    	// Print Results for pattern tree 2
    	this.printResultsOfPatternTree(patternTreeTwoIterator);
    	
    	// write the results into a heap file
    	this.insertResultsIntoHeapFile("patternTree2Results", patternTreeTwoIterator);
	}
	
	public SortMerge2 executeQP2ForAPatternTree(FileScan it1, FileScan it2, String fileNamePrefix) throws HFException, HFBufMgrException, HFDiskMgrException, IndexException, FileScanException, TupleUtilsException, InvalidRelation, IOException {
		// ArrayList<NestedLoopsJoins> instances = nestedLoopInstanceList;
    	FileScan sm1, sm2;
    	FileScan it;
    	SortMerge2 smInstance;
    	
    	int joinColumnIndex = 1; // incremented each time a new "tag" is added, because a tag will bring 2 columns
    	
        boolean status = OK;
        
        AttrType [] Stypes = new AttrType[4];
        Stypes[0] = new AttrType (AttrType.attrInterval);
        Stypes[1] = new AttrType (AttrType.attrString);
        Stypes[2] = new AttrType (AttrType.attrInterval);
        Stypes[3] = new AttrType (AttrType.attrString);
        
        short [] Ssizes = new short[2];
        Ssizes[0] = 10;
        Ssizes[1] = 10;

        FldSpec [] projectionList = new FldSpec[6];
        projectionList[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projectionList[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        projectionList[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        projectionList[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
        projectionList[4] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
        projectionList[5] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
        
        
        
        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
        SortMerge2 tempInstance =null;
        
        if (!tagIndex.containsKey(sortedRules.get(0).get(0))) {
        	tagIndex.put(sortedRules.get(0).get(0), currentTagIndex);
        	 currentTagIndex+= 2;
        }
        
        if (!tagIndex.containsKey(sortedRules.get(0).get(1))) {
        	tagIndex.put(sortedRules.get(0).get(1), currentTagIndex);
        	currentTagIndex+= 2;
        }
        
        joinColumnIndex = tagIndex.get(sortedRules.get(currentInstanceIndex).get(0));
        
    	CondExpr[] expr = new CondExpr[2];
    	expr[0] = new CondExpr();
        expr[1] = null;
        expr[0].next  = null;

        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),joinColumnIndex);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        expr[0].flag = 1;
        
        if (!tagIndex.containsKey(sortedRules.get(currentInstanceIndex).get(1))) {
        	tagIndex.put(sortedRules.get(currentInstanceIndex).get(1), currentTagIndex);
        	currentTagIndex+= 2;
        }
        
        sm1 = it1;
        sm2 = it2;     
        
        try {
            tempInstance = new SortMerge2(Stypes, 4, Ssizes, Stypes, 4, Ssizes, joinColumnIndex, 12, 1, 12, 10, sm1, sm2, false, false, ascending, expr, projectionList, 6);
        }
        catch (Exception e) {
            System.err.println("*** join error in SortMerge constructor ***");
            status = FAIL;
            System.err.println (""+e);
            e.printStackTrace();
        }
        
        
    	currentInstanceIndex++;
    	CondExpr[] condExpr;
    	SortMerge2 sortMerge2Instance;
    	
    	int columnCount, tempVal;
    	boolean flag = false;
    	AttrType[] Stypes2;
    	colLength = 6;

    	SortMerge2 tempInstance2 = null;
    	for (int index = currentInstanceIndex; index < sortedRules.size(); index++) {
//    		sortMerge2Instance = tempInstance;
//    		sm2 = sortMergeInstanceList.get(index);
            
            Stypes2 = new AttrType[4];
            Stypes2[0] = new AttrType (AttrType.attrInterval);
            Stypes2[1] = new AttrType (AttrType.attrString);
            Stypes2[2] = new AttrType (AttrType.attrInterval);
            Stypes2[3] = new AttrType (AttrType.attrString);
            
            AttrType[] Stypes1 = new AttrType[tagIndex.size()*2];
            for (int currentColumn = 0; currentColumn < tagIndex.size()*2; currentColumn++) {
            	if (currentColumn%2 == 0) {
            		Stypes1[currentColumn] = new AttrType(AttrType.attrInterval);
            	} else {
            		Stypes1[currentColumn] = new AttrType(AttrType.attrString);
            	}
            }
            
            short[] Ssizes1 = new short[tagIndex.size()];
            for (int i=0; i<tagIndex.size(); i++) {
            	Ssizes1[i] = 10;
            }
            
            short [] Ssizes2 = new short[2];
            Ssizes2[0] = 10;
            Ssizes2[1] = 10;
            
            
            joinColumnIndex = tagIndex.get(sortedRules.get(index).get(0));
            
            if (!tagIndex.containsKey(sortedRules.get(index).get(1))) {
            	tagIndex.put(sortedRules.get(index).get(1), currentTagIndex);
            	currentTagIndex+= 2;
            	currentProjCount+= 2;
            	flag = true;
            }
            
            tempVal = flag ? currentProjCount : currentProjCount-2;
            FldSpec [] projectionList2 = new FldSpec[tempVal];
            
            colLength = tempVal;
            for (int i=0; i<tempVal; i++) {
            	projectionList2[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
            }
            
            if (flag) {
                projectionList2[tempVal-2] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
                projectionList2[tempVal-1] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
            }           
            
            
            tempInstance2 = tempInstance;
            
            it = this.queryRuleIteratorScan(sortedRules.get(index).get(0), sortedRules.get(index).get(1), sortedRules.get(index).get(2), index+1, fileNamePrefix);
            
            System.out.println("Column length: " + projectionList2.length);
            
            try {
                tempInstance = new SortMerge2(Stypes1, Stypes1.length, Ssizes1, Stypes2, Stypes2.length, Ssizes2, joinColumnIndex, 12, 1, 12, 10, tempInstance2, it, false, false, ascending, expr, projectionList2, tempVal);
            }
            catch (Exception e) {
                System.err.println("*** join error in SortMerge constructor ***");
                status = FAIL;
                System.err.println (""+e);
                e.printStackTrace();
            }
    		
        	flag = false;
    	}
    	
    	return tempInstance;
	}

	public static void main(String[] args) throws FileNotFoundException, ParserConfigurationException, SAXException, IOException, HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, HFException, HFBufMgrException, HFDiskMgrException, FileScanException, TupleUtilsException, InvalidRelation, IndexException {


		System.out.println("XMLQP2");
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		dbf.setValidating(false);
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(new FileInputStream(new File("/home/ronak/DBMSi Project/Phase3/dbmsiPhase2/javaminibase/src/xmldbTestXML/sample_data.xml")));

		Node root = doc.getDocumentElement();

		xmlParser.build(root);
		xmlParser.preOrder(xmlParser.tree.root);
		xmlParser.BFSSetLevel();

		qp2 = new XMLQP2();
		qp2.xmlDataInsert();

		qp2.QP2Wrapper();
	}
}
