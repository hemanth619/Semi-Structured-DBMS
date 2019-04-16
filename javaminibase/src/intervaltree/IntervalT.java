package intervaltree;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import btree.BTIndexPage;
import btree.BTLeafPage;
import btree.BTSortedPage;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DataClass;
import btree.IndexData;
import btree.IntegerKey;
import btree.IntervalKey;
import btree.IteratorException;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.LeafData;
import btree.NodeNotMatchException;
import btree.NodeType;
import btree.StringKey;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;
import global.AttrType;
import global.Convert;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;

public class IntervalT implements GlobalConst {
	IntervalT() {

	}

	public final static int keyCompare(KeyClass key1, KeyClass key2) throws KeyNotMatchException {
		if ((key1 instanceof IntervalKey) && (key2 instanceof IntervalKey)) {
			return (((IntervalKey) key1).getKey()).start - (((IntervalKey) key2).getKey()).start;
		} else {
			throw new KeyNotMatchException(null, "key types do not match");
		}
	}
	
	/**
	 * It gets the length of the key
	 * 
	 * @param key specify the key whose length will be calculated. Input parameter.
	 * @return return the length of the key
	 * @exception KeyNotMatchException key is neither StringKey nor IntegerKey
	 * @exception IOException          error from the lower layer
	 */
	protected final static int getKeyLength(KeyClass key) throws KeyNotMatchException, IOException {
		if (key instanceof StringKey) {

			OutputStream out = new ByteArrayOutputStream();
			DataOutputStream outstr = new DataOutputStream(out);
			outstr.writeUTF(((StringKey) key).getKey());
			return outstr.size();
		} else if (key instanceof IntegerKey)
			return 4;
		else
			throw new KeyNotMatchException(null, "key types do not match");
	}

	/**
	 * For debug. Print the B+ tree structure out
	 * 
	 * @param header the head page of the B+ tree file
	 * @exception IOException                 error from the lower layer
	 * @exception ConstructPageException      error from BT page constructor
	 * @exception IteratorException           error from iterator
	 * @exception HashEntryNotFoundException  error from lower layer
	 * @exception InvalidFrameNumberException error from lower layer
	 * @exception PageUnpinnedException       error from lower layer
	 * @exception ReplacerException           error from lower layer
	 */
	public static void printIntervalTree(IntervalTreeHeaderPage header)
			throws IOException, ConstructPageException, IteratorException, HashEntryNotFoundException,
			InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
		if (header.get_rootId().pid == INVALID_PAGE) {
			System.out.println("The Tree is Empty!!!");
			return;
		}

		System.out.println("");
		System.out.println("");
		System.out.println("");
		System.out.println("---------------The B+ Tree Structure---------------");

		System.out.println(1 + "     " + header.get_rootId());

		_printIntervalTree(header.get_rootId(), "     ", 1, header.get_keyType());

		System.out.println("--------------- End ---------------");
		System.out.println("");
		System.out.println("");
	}

	private static void _printIntervalTree(PageId currentPageId, String prefix, int i, int keyType)
			throws IOException, ConstructPageException, IteratorException, HashEntryNotFoundException,
			InvalidFrameNumberException, PageUnpinnedException, ReplacerException {

		BTSortedPage sortedPage = new BTSortedPage(currentPageId, keyType);
		prefix = prefix + "       ";
		i++;
		if (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage((Page) sortedPage, keyType);

			System.out.println(i + prefix + indexPage.getPrevPage());
			_printIntervalTree(indexPage.getPrevPage(), prefix, i, keyType);

			RID rid = new RID();
			for (KeyDataEntry entry = indexPage.getFirst(rid); entry != null; entry = indexPage.getNext(rid)) {
				System.out.println(i + prefix + (IndexData) entry.data);
				_printIntervalTree(((IndexData) entry.data).getData(), prefix, i, keyType);
			}
		}
		SystemDefs.JavabaseBM.unpinPage(currentPageId, true/* dirty */);
	}
	
	/**
	 * used for debug: to print a page out. The page is either BTIndexPage, or
	 * BTLeafPage.
	 * 
	 * @param pageno  the number of page. Input parameter.
	 * @param keyType It specifies the type of key. It can be AttrType.attrString or
	 *                AttrType.attrInteger. Input parameter.
	 * @exception IOException                 error from the lower layer
	 * @exception IteratorException           error for iterator
	 * @exception ConstructPageException      error for BT page constructor
	 * @exception HashEntryNotFoundException  error from the lower layer
	 * @exception ReplacerException           error from the lower layer
	 * @exception PageUnpinnedException       error from the lower layer
	 * @exception InvalidFrameNumberException error from the lower layer
	 */
	
	// TODO: Modify implementation based on use case
	public static void printPage(PageId pageno, int keyType)
			throws IOException, IteratorException, ConstructPageException, HashEntryNotFoundException,
			ReplacerException, PageUnpinnedException, InvalidFrameNumberException {
		BTSortedPage sortedPage = new BTSortedPage(pageno, keyType);
		int i;
		i = 0;
		if (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage((Page) sortedPage, keyType);
			System.out.println("");
			System.out.println("**************To Print an Index Page ********");
			System.out.println("Current Page ID: " + indexPage.getCurPage().pid);
			System.out.println("Left Link      : " + indexPage.getLeftLink().pid);

			RID rid = new RID();

			for (KeyDataEntry entry = indexPage.getFirst(rid); entry != null; entry = indexPage.getNext(rid)) {
				if (keyType == AttrType.attrInteger)
					System.out.println(
							i + " (key, pageId):   (" + (IntegerKey) entry.key + ",  " + (IndexData) entry.data + " )");
				if (keyType == AttrType.attrString)
					System.out.println(
							i + " (key, pageId):   (" + (StringKey) entry.key + ",  " + (IndexData) entry.data + " )");

				i++;
			}

			System.out.println("************** END ********");
			System.out.println("");
		} else if (sortedPage.getType() == NodeType.LEAF) {
			BTLeafPage leafPage = new BTLeafPage((Page) sortedPage, keyType);
			System.out.println("");
			System.out.println("**************To Print an Leaf Page ********");
			System.out.println("Current Page ID: " + leafPage.getCurPage().pid);
			System.out.println("Left Link      : " + leafPage.getPrevPage().pid);
			System.out.println("Right Link     : " + leafPage.getNextPage().pid);

			RID rid = new RID();

			for (KeyDataEntry entry = leafPage.getFirst(rid); entry != null; entry = leafPage.getNext(rid)) {
				if (keyType == AttrType.attrInteger)
					System.out.println(i + " (key, [pageNo, slotNo]):   (" + (IntegerKey) entry.key + ",  "
							+ (LeafData) entry.data + " )");
				if (keyType == AttrType.attrString)
					System.out.println(i + " (key, [pageNo, slotNo]):   (" + (StringKey) entry.key + ",  "
							+ (LeafData) entry.data);

				i++;
			}

			System.out.println("************** END ********");
			System.out.println("");
		} else {
			System.out.println("Sorry!!! This page is neither Index nor Leaf page.");
		}

		SystemDefs.JavabaseBM.unpinPage(pageno, true/* dirty */);
	}
	
	/**
	 * It gets an keyDataEntry from bytes array and position
	 * 
	 * @param from     It's a bytes array where KeyDataEntry will come from. Input
	 *                 parameter.
	 * @param offset   the offset in the bytes. Input parameter.
	 * @param keyType  It specifies the type of key. It can be AttrType.attrString
	 *                 or AttrType.attrInteger. Input parameter.
	 * @param nodeType It specifes NodeType.LEAF or NodeType.INDEX. Input parameter.
	 * @param length   The length of (key, data) in byte array "from". Input
	 *                 parameter.
	 * @return return a KeyDataEntry object
	 * @exception KeyNotMatchException  key is neither StringKey nor IntegerKey
	 * @exception NodeNotMatchException nodeType is neither NodeType.LEAF nor
	 *                                  NodeType.INDEX.
	 * @exception ConvertException      error from the lower layer
	 */
	protected final static KeyDataEntry getEntryFromBytes(byte[] from, int offset, int length, int keyType,
			short nodeType) throws KeyNotMatchException, NodeNotMatchException, ConvertException {
		KeyClass key;
		DataClass data;
		int n;
		try {

			if (nodeType == NodeType.INDEX) {
				n = 4;
				data = new IndexData(Convert.getIntValue(offset + length - 4, from));
			} else if (nodeType == NodeType.LEAF) {
				n = 8;
				RID rid = new RID();
				rid.slotNo = Convert.getIntValue(offset + length - 8, from);
				rid.pageNo = new PageId();
				rid.pageNo.pid = Convert.getIntValue(offset + length - 4, from);
				data = new LeafData(rid);
			} else
				throw new NodeNotMatchException(null, "node types do not match");

			if (keyType == AttrType.attrInteger) {
				key = new IntegerKey(new Integer(Convert.getIntValue(offset, from)));
			} else if (keyType == AttrType.attrString) {
				// System.out.println(" offset "+ offset + " " + length + " "+n);
				key = new StringKey(Convert.getStrValue(offset, from, length - n));
			} else
				throw new KeyNotMatchException(null, "key types do not match");

			return new KeyDataEntry(key, data);

		} catch (IOException e) {
			throw new ConvertException(e, "convert faile");
		}
	}

	// TODO: Modify implementation based on use case
	public static void printTreeUtilization(IntervalTreeHeaderPage header) {
		
	}
	
	// TODO: Modify implementation based on use case
	public static void printNonLeafTreeUtilization(IntervalTreeHeaderPage header) {
		
	}
}
