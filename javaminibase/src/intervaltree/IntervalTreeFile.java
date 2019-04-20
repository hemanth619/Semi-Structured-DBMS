package intervaltree;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import intervaltree.IntervalT;
import btree.BT;
import btree.BTIndexPage;
import btree.BTLeafPage;
import btree.BTSortedPage;
import btree.BTreeHeaderPage;
import intervaltree.IntervalTreeHeaderPage;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteFashionException;
import btree.DeleteFileEntryException;
import btree.DeleteRecException;
import btree.FreePageException;
import btree.IndexData;
import btree.IndexFile;
import btree.IndexFullDeleteException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.InsertRecException;
import btree.IntegerKey;
import btree.IntervalKey;
import btree.IteratorException;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafData;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.LeafRedistributeException;
import btree.NodeNotMatchException;
import btree.NodeType;
import btree.PinPageException;
import btree.RecordNotFoundException;
import btree.RedistributeException;
import btree.StringKey;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;

public class IntervalTreeFile extends IndexFile implements GlobalConst {

	private final static int MAGIC0 = 1989;

	private final static String lineSep = System.getProperty("line.separator");

	private static FileOutputStream fos;
	private static DataOutputStream trace;
	
	private IntervalTreeHeaderPage headerPage;
	private PageId headerPageId;
	private String dbname;
	
	public IntervalTreeFile(String filename) {
		
	}
	
	public IntervalTreeFile(String filename, int delete_fashion) {
		
	}
	
	private void freePage(PageId pageno) throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}

	}
	
	private Page pinPage(PageId pageno) throws PinPageException {
		try {
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
			return page;
		} catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e, "");
		}
	}
	
	private void unpinPage(PageId pageno) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}
	
	private void unpinPage(PageId pageno, boolean dirty) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}
	
	private void delete_file_entry(String filename) throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}
	
	/**
	 * Destroy entire B+ tree file.
	 * 
	 * @exception IOException              error from the lower layer
	 * @exception IteratorException        iterator error
	 * @exception UnpinPageException       error when unpin a page
	 * @exception FreePageException        error when free a page
	 * @exception DeleteFileEntryException failed when delete a file from DM
	 * @exception ConstructPageException   error in BT page constructor
	 * @exception PinPageException         failed when pin a page
	 */
	public void destroyFile() throws IOException, IteratorException, UnpinPageException, FreePageException,
			DeleteFileEntryException, ConstructPageException, PinPageException {
		if (headerPage != null) {
			PageId pgId = headerPage.get_rootId();
			if (pgId.pid != INVALID_PAGE)
				_destroyFile(pgId);
			unpinPage(headerPageId);
			freePage(headerPageId);
			delete_file_entry(dbname);
			headerPage = null;
		}
	}

	private void _destroyFile(PageId pageno) throws IOException, IteratorException, PinPageException,
			ConstructPageException, UnpinPageException, FreePageException {

		IntervalTSortedPage sortedPage;
		Page page = pinPage(pageno);
		sortedPage = new IntervalTSortedPage(page, headerPage.get_keyType());

		if (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage(page, headerPage.get_keyType());
			RID rid = new RID();
			PageId childId;
			KeyDataEntry entry;
			for (entry = indexPage.getFirst(rid); entry != null; entry = indexPage.getNext(rid)) {
				childId = ((IndexData) (entry.data)).getData();
				_destroyFile(childId);
			}
		} else { // IntervalTLeafPage
			unpinPage(pageno);
			freePage(pageno);
		}
	}
	
	/**
	 * Stop tracing. And close trace file.
	 * 
	 * @exception IOException error from the lower layer
	 */
	public static void destroyTrace() throws IOException {
		if (trace != null)
			trace.close();
		if (fos != null)
			fos.close();
		fos = null;
		trace = null;
	}
	
//	/**
//	 * insert record with the given key and rid
//	 * 
//	 * @param key the key of the record. Input parameter.
//	 * @param rid the rid of the record. Input parameter.
//	 * @exception KeyTooLongException     key size exceeds the max keysize.
//	 * @exception KeyNotMatchException    key is not integer key nor string key
//	 * @exception IOException             error from the lower layer
//	 * @exception LeafInsertRecException  insert error in leaf page
//	 * @exception IndexInsertRecException insert error in index page
//	 * @exception ConstructPageException  error in BT page constructor
//	 * @exception UnpinPageException      error when unpin a page
//	 * @exception PinPageException        error when pin a page
//	 * @exception NodeNotMatchException   node not match index page nor leaf page
//	 * @exception ConvertException        error when convert between revord and byte
//	 *                                    array
//	 * @exception DeleteRecException      error when delete in index page
//	 * @exception IndexSearchException    error when search
//	 * @exception IteratorException       iterator error
//	 * @exception LeafDeleteException     error when delete in leaf page
//	 * @exception InsertException         error when insert in index page
//	 */
//	public void insert(KeyClass key, RID rid) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
//			IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
//			NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
//			LeafDeleteException, InsertException, IOException {
//		
//		KeyDataEntry newRootEntry;
//
////		if (IntervalT.getKeyLength(key) > headerPage.get_maxKeySize())
////			throw new KeyTooLongException(null, "");
////
////		if (key instanceof StringKey) {
////			if (headerPage.get_keyType() != AttrType.attrString) {
////				throw new KeyNotMatchException(null, "");
////			}
////		} else if (key instanceof IntegerKey) {
////			if (headerPage.get_keyType() != AttrType.attrInteger) {
////				throw new KeyNotMatchException(null, "");
////			}
////		}
//		if (key instanceof IntervalKey) {
//			if (headerPage.get_keyType() != AttrType.attrInterval) {
//				throw new KeyNotMatchException(null, "");
//			}
//		} else
//			throw new KeyNotMatchException(null, "");
//
//		// TWO CASES:
//		
//		// 1. headerPage.root == INVALID_PAGE:
//		// - the tree is empty and we have to create a new first page;
//		// this page will be a leaf page
//		
//		// 2. headerPage.root != INVALID_PAGE:
//		// - we call _insert() to insert the pair (key, rid)
//
//		if (trace != null) {
//			trace.writeBytes("INSERT " + rid.pageNo + " " + rid.slotNo + " " + key + lineSep);
//			trace.writeBytes("DO" + lineSep);
//			trace.flush();
//		}
//
//		// case 1
//		if (headerPage.get_rootId().pid == INVALID_PAGE) {
//			
//			PageId newRootPageId;
//			BTLeafPage newRootPage;
//			RID dummyrid;
//
//			newRootPage = new BTLeafPage(headerPage.get_keyType()); // create the root page as a leaf page
//			newRootPageId = newRootPage.getCurPage(); // page number of the current page
//
//			if (trace != null) {
//				trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
//				trace.flush();
//			}
//
//			// root is the leaf page & the only page. so previous page is invalid, next page is invalid
//			newRootPage.setNextPage(new PageId(INVALID_PAGE));
//			newRootPage.setPrevPage(new PageId(INVALID_PAGE));
//
//			// ASSERTIONS:
//			// - newRootPage, newRootPageId valid and pinned
//
//			newRootPage.insertRecord(key, rid);
//
//			if (trace != null) {
//				trace.writeBytes("PUTIN node " + newRootPageId + lineSep);
//				trace.flush();
//			}
//
//			unpinPage(newRootPageId, true); /* = DIRTY */
//			updateHeader(newRootPageId);
//
//			if (trace != null) {
//				trace.writeBytes("DONE" + lineSep);
//				trace.flush();
//			}
//
//			return;
//		}
//
//		// ASSERTIONS:
//		// - headerPageId, headerPage valid and pinned
//		// - headerPage.root holds the pageId of the root of the B-tree
//		// - none of the pages of the tree is pinned yet
//
//		if (trace != null) {
//			trace.writeBytes("SEARCH" + lineSep);
//			trace.flush();
//		}
//// TODO: To be continued on 8th April 2019
//		newRootEntry = _insert(key, rid, headerPage.get_rootId());
//
//		// TWO CASES:
//		// - newRootEntry != null: a leaf split propagated up to the root
//		// and the root split: the new pageNo is in
//		// newChildEntry.data.pageNo
//		// - newRootEntry == null: no new root was created;
//		// information on headerpage is still valid
//
//		// ASSERTIONS:
//		// - no page pinned
//
//		if (newRootEntry != null) {
//			BTIndexPage newRootPage;
//			PageId newRootPageId;
//			Object newEntryKey;
//
//			// the information about the pair <key, PageId> is
//			// packed in newRootEntry: extract it
//
//			newRootPage = new BTIndexPage(headerPage.get_keyType());
//			newRootPageId = newRootPage.getCurPage();
//
//			// ASSERTIONS:
//			// - newRootPage, newRootPageId valid and pinned
//			// - newEntryKey, newEntryPage contain the data for the new entry
//			// which was given up from the level down in the recursion
//
//			if (trace != null) {
//				trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
//				trace.flush();
//			}
//
//			newRootPage.insertKey(newRootEntry.key, ((IndexData) newRootEntry.data).getData());
//
//			// the old root split and is now the left child of the new root
//			newRootPage.setPrevPage(headerPage.get_rootId());
//
//			unpinPage(newRootPageId, true /* = DIRTY */);
//
//			updateHeader(newRootPageId);
//
//		}
//
//		if (trace != null) {
//			trace.writeBytes("DONE" + lineSep);
//			trace.flush();
//		}
//
//		return;
//	}
	
	private void updateHeader(PageId newRoot) throws IOException, PinPageException, UnpinPageException {

		IntervalTreeHeaderPage header;
		PageId old_data;

		header = new IntervalTreeHeaderPage(pinPage(headerPageId));

		old_data = headerPage.get_rootId();
		header.set_rootId(newRoot);

		// clock in dirty bit to bm so our dtor needn't have to worry about it
		unpinPage(headerPageId, true /* = DIRTY */ );

		// ASSERTIONS:
		// - headerPage, headerPageId valid, pinned and marked as dirty
	}
	
	/**
	   * Insert entry into the index file.
	   * @param data the key for the entry
	   * @param rid the rid of the tuple with the key
	   * @exception IOException from lower layers
	   * @exception KeyTooLongException the key is too long
	   * @exception KeyNotMatchException the keys do not match
	   * @exception LeafInsertRecException  insert record to leaf page failed 
	   * @exception IndexInsertRecException insert record to index page failed
	   * @exception ConstructPageException  fail to construct a header page
	   * @exception UnpinPageException unpin page failed
	   * @exception PinPageException  pin page failed
	   * @exception NodeNotMatchException  nodes do not match
	   * @exception ConvertException conversion failed (from global package)
	   * @exception DeleteRecException delete record failed
	   * @exception IndexSearchException index search failed
	   * @exception IteratorException  error from iterator 
	   * @exception LeafDeleteException delete leaf page failed
	   * @exception InsertException insert record failed
	   */
	public void insert(KeyClass key, final RID rid) throws KeyTooLongException, KeyNotMatchException,
			LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException,
			PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException,
			IteratorException, LeafDeleteException, InsertException, IOException {

		KeyDataEntry newRootEntry;

		/* 1. Basic Check */
		if (IntervalT.getKeyLength(key) > headerPage.get_maxKeySize())
			throw new KeyTooLongException(null, "");
		
		/*2. Basic Check */
		if (key instanceof IntervalKey == false || headerPage.get_keyType() != AttrType.attrInterval) {
			throw new KeyNotMatchException(null, "");
		}

		// TWO CASES:
		
		// 1. headerPage.root == INVALID_PAGE:
		// - the tree is empty and we have to create a new first page;
		// this page will be a leaf page
		
		// 2. headerPage.root != INVALID_PAGE:
		// - we call _insert() to insert the pair (key, rid)
		
		if (trace != null) {
			trace.writeBytes("INSERT " + rid.pageNo + " " + rid.slotNo + " " + key + lineSep);
			trace.writeBytes("DO" + lineSep);
			trace.flush();
		}
		
		/*3. If header page's root ID says INVALID_PAGE */
		// case 1
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			
			PageId newRootPageId;
			IntervalTLeafPage newRootPage;
			RID dummyrid;

			/* 4. Create a leaf page as the root page. */
			newRootPage = new IntervalTLeafPage(headerPage.get_keyType()); // create the root page as a leaf page
			newRootPageId = newRootPage.getCurPage(); // page number of the current page

			if (trace != null) {
				trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
				trace.flush();
			}

			/* 5. Set the previous as invalid and next as invalid as it's the only page. */
			// root is the leaf page & the only page. so previous page is invalid, next page is invalid
			newRootPage.setNextPage(new PageId(INVALID_PAGE));
			newRootPage.setPrevPage(new PageId(INVALID_PAGE));

			// ASSERTIONS:
			// - newRootPage, newRootPageId valid and pinned

			newRootPage.insertRecord(key, rid);

			if (trace != null) {
				trace.writeBytes("PUTIN node " + newRootPageId + lineSep);
				trace.flush();
			}

			unpinPage(newRootPageId, true); /* = DIRTY */
			updateHeader(newRootPageId);

			if (trace != null) {
				trace.writeBytes("DONE" + lineSep);
				trace.flush();
			}

			return;
		}
		

		// ASSERTIONS:
		// - headerPageId, headerPage valid and pinned
		// - headerPage.root holds the pageId of the root of the B-tree
		// - none of the pages of the tree is pinned yet

		if (trace != null) {
			trace.writeBytes("SEARCH" + lineSep);
			trace.flush();
		}
		
		newRootEntry = _insert(key, rid, headerPage.get_rootId());
	}

	private KeyDataEntry _insert(KeyClass key, RID rid, PageId currentPageId)
			throws PinPageException, IOException, ConstructPageException, LeafDeleteException, ConstructPageException,
			DeleteRecException, IndexSearchException, UnpinPageException, LeafInsertRecException, ConvertException,
			IteratorException, IndexInsertRecException, KeyNotMatchException, NodeNotMatchException, InsertException {

		IntervalTSortedPage currentPage;
		Page page;
		KeyDataEntry upEntry;
		
		// pin the current page ID, headerPage.get_rootId() initially -- pin the header page
		page = pinPage(currentPageId);
		currentPage = new IntervalTSortedPage(page, headerPage.get_keyType());

		if (trace != null) {
			trace.writeBytes("VISIT node " + currentPageId + lineSep);
			trace.flush();
		}

		// TWO CASES:
		// - pageType == INDEX:
		// recurse and then split if necessary
		// - pageType == LEAF:
		// try to insert pair (key, rid), maybe split

		if (currentPage.getType() == NodeType.INDEX) {
			IntervalTIndexPage currentIndexPage = new IntervalTIndexPage(page, headerPage.get_keyType());
			PageId currentIndexPageId = currentPageId;
			PageId nextPageId;

			nextPageId = currentIndexPage.getPageNoByKey(key);

			// unpin page, recurse and then pin it again
			unpinPage(currentIndexPageId);

			upEntry = _insert(key, rid, nextPageId);

			// two cases:
			// - upEntry == null: one level lower no split has occurred:
			// we are done.
			
			// - upEntry != null: one of the children has split and
			// upEntry is the new data entry which has
			// to be inserted on this index page

			if (upEntry == null)
				return null;

			currentIndexPage = new IntervalTIndexPage(pinPage(currentPageId), headerPage.get_keyType());

			// ASSERTIONS:
			// - upEntry != null
			// - currentIndexPage, currentIndexPageId valid and pinned

			// the information about the pair <key, PageId> is
			// packed in upEntry

			// check whether there can still be entries inserted on that page
			if (currentIndexPage.available_space() >= IntervalT.getKeyDataLength(upEntry.key, NodeType.INDEX)) {

				// no split has occurred
				currentIndexPage.insertKey(upEntry.key, ((IndexData) upEntry.data).getData());

				unpinPage(currentIndexPageId, true /* DIRTY */);

				return null;
			}

			// ASSERTIONS:
			// - on the current index page is not enough space available .
			// it splits

			// therefore we have to allocate a new index page and we will
			// distribute the entries
			// - currentIndexPage, currentIndexPageId valid and pinned

			IntervalTIndexPage newIndexPage;
			PageId newIndexPageId;

			// we have to allocate a new INDEX page and
			// to redistribute the index entries
			newIndexPage = new IntervalTIndexPage(headerPage.get_keyType()); // allocate a new IntervalTIndexPage of type IntervalKey
			newIndexPageId = newIndexPage.getCurPage();

			if (trace != null) {
				if (headerPage.get_rootId().pid != currentIndexPageId.pid)
					trace.writeBytes("SPLIT node " + currentIndexPageId + " IN nodes " + currentIndexPageId + " "
							+ newIndexPageId + lineSep);
				else
					trace.writeBytes("ROOTSPLIT IN nodes " + currentIndexPageId + " " + newIndexPageId + lineSep);
				trace.flush();
			}

			// ASSERTIONS:
			// - newIndexPage, newIndexPageId valid and pinned
			// - currentIndexPage, currentIndexPageId valid and pinned
			// - upEntry containing (Key, Page) for the new entry which was
			// given up from the level down in the recursion

			KeyDataEntry tmpEntry;
			PageId tmpPageId;
			RID insertRid;
			RID delRid = new RID();

			// The RID passed to getFirst is the input and output parameter
			for (tmpEntry = currentIndexPage.getFirst(delRid); tmpEntry != null; tmpEntry = currentIndexPage
					.getFirst(delRid)) {
				newIndexPage.insertKey(tmpEntry.key, ((IndexData) tmpEntry.data).getData());
				currentIndexPage.deleteSortedRecord(delRid);
			}

			// ASSERTIONS:
			// - currentIndexPage empty
			// - newIndexPage holds all former records from currentIndexPage

			// we will try to make an equal split
			RID firstRid = new RID();
			KeyDataEntry undoEntry = null;
			for (tmpEntry = newIndexPage.getFirst(firstRid); (currentIndexPage.available_space() > newIndexPage
					.available_space()); tmpEntry = newIndexPage.getFirst(firstRid)) {
				// now insert the <key,pageId> pair on the new
				// index page
				undoEntry = tmpEntry;
				currentIndexPage.insertKey(tmpEntry.key, ((IndexData) tmpEntry.data).getData());
				newIndexPage.deleteSortedRecord(firstRid);
			}

			// undo the final record
			if (currentIndexPage.available_space() < newIndexPage.available_space()) {

				newIndexPage.insertKey(undoEntry.key, ((IndexData) undoEntry.data).getData());

				currentIndexPage.deleteSortedRecord(
						new RID(currentIndexPage.getCurPage(), (int) currentIndexPage.getSlotCnt() - 1));
			}

			// check whether <newKey, newIndexPageId>
			// will be inserted
			// on the newly allocated or on the old index page

			tmpEntry = newIndexPage.getFirst(firstRid);

			if (IntervalT.keyCompare(upEntry.key, tmpEntry.key) >= 0) {
				// the new data entry belongs on the new index page
				newIndexPage.insertKey(upEntry.key, ((IndexData) upEntry.data).getData());
			} else {
				currentIndexPage.insertKey(upEntry.key, ((IndexData) upEntry.data).getData());

				int i = (int) currentIndexPage.getSlotCnt() - 1;
				tmpEntry = IntervalT.getEntryFromBytes(currentIndexPage.getpage(), currentIndexPage.getSlotOffset(i),
						currentIndexPage.getSlotLength(i), headerPage.get_keyType(), NodeType.INDEX);

				newIndexPage.insertKey(tmpEntry.key, ((IndexData) tmpEntry.data).getData());

				currentIndexPage.deleteSortedRecord(new RID(currentIndexPage.getCurPage(), i));

			}

			unpinPage(currentIndexPageId, true /* dirty */);

			// fill upEntry
			upEntry = newIndexPage.getFirst(delRid);

			// now set prevPageId of the newIndexPage to the pageId
			// of the deleted entry:
			newIndexPage.setPrevPage(((IndexData) upEntry.data).getData());

			// delete first record on new index page since it is given up
			newIndexPage.deleteSortedRecord(delRid);

			unpinPage(newIndexPageId, true /* dirty */);

			if (trace != null) {
				trace_children(currentIndexPageId);
				trace_children(newIndexPageId);
			}

			((IndexData) upEntry.data).setData(newIndexPageId);

			return upEntry;

			// ASSERTIONS:
			// - no pages pinned
			// - upEntry holds the pointer to the KeyDataEntry which is
			// to be inserted on the index page one level up

		} else if (currentPage.getType() == NodeType.LEAF) {
			IntervalTLeafPage currentLeafPage = new IntervalTLeafPage(page, headerPage.get_keyType());

			PageId currentLeafPageId = currentPageId;

			// ASSERTIONS:
			// - currentLeafPage, currentLeafPageId valid and pinned

			// check whether there can still be entries inserted on that page
			if (currentLeafPage.available_space() >= IntervalT.getKeyDataLength(key, NodeType.LEAF)) {
				// no split has occurred

				currentLeafPage.insertRecord(key, rid);

				unpinPage(currentLeafPageId, true /* DIRTY */);

				if (trace != null) {
					trace.writeBytes("PUTIN node " + currentLeafPageId + lineSep);
					trace.flush();
				}

				return null;
			}

			// ASSERTIONS:
			// - on the current leaf page is not enough space available.
			// It splits.
			// - therefore we have to allocate a new leaf page and we will
			// - distribute the entries

			IntervalTLeafPage newLeafPage;
			PageId newLeafPageId;
			
			// we have to allocate a new LEAF page and
			// to redistribute the data entries
			newLeafPage = new IntervalTLeafPage(headerPage.get_keyType());
			newLeafPageId = newLeafPage.getCurPage();

			newLeafPage.setNextPage(currentLeafPage.getNextPage());
			newLeafPage.setPrevPage(currentLeafPageId); // for dbl-linked list
			currentLeafPage.setNextPage(newLeafPageId); // don't know why this is needed

			// change the prevPage pointer on the next page:

			PageId rightPageId;
			rightPageId = newLeafPage.getNextPage();
			if (rightPageId.pid != INVALID_PAGE) {
				BTLeafPage rightPage;
				rightPage = new BTLeafPage(rightPageId, headerPage.get_keyType());

				rightPage.setPrevPage(newLeafPageId);
				unpinPage(rightPageId, true /* = DIRTY */);

				// ASSERTIONS:
				// - newLeafPage, newLeafPageId valid and pinned
				// - currentLeafPage, currentLeafPageId valid and pinned
			}

			if (trace != null) {
				if (headerPage.get_rootId().pid != currentLeafPageId.pid)
					trace.writeBytes("SPLIT node " + currentLeafPageId + " IN nodes " + currentLeafPageId + " "
							+ newLeafPageId + lineSep);
				else
					trace.writeBytes("ROOTSPLIT IN nodes " + currentLeafPageId + " " + newLeafPageId + lineSep);
				trace.flush();
			}

			KeyDataEntry tmpEntry;
			RID firstRid = new RID();

			for (tmpEntry = currentLeafPage.getFirst(firstRid); tmpEntry != null; tmpEntry = currentLeafPage
					.getFirst(firstRid)) {

				newLeafPage.insertRecord(tmpEntry.key, ((LeafData) (tmpEntry.data)).getData());
				currentLeafPage.deleteSortedRecord(firstRid);

			}

			// ASSERTIONS:
			// - currentLeafPage empty
			// - newLeafPage holds all former records from currentLeafPage

			KeyDataEntry undoEntry = null;
			for (tmpEntry = newLeafPage.getFirst(firstRid); newLeafPage.available_space() < currentLeafPage
					.available_space(); tmpEntry = newLeafPage.getFirst(firstRid)) {
				undoEntry = tmpEntry;
				currentLeafPage.insertRecord(tmpEntry.key, ((LeafData) tmpEntry.data).getData());
				newLeafPage.deleteSortedRecord(firstRid);
			}

			if (BT.keyCompare(key, undoEntry.key) < 0) {
				// undo the final record
				if (currentLeafPage.available_space() < newLeafPage.available_space()) {
					newLeafPage.insertRecord(undoEntry.key, ((LeafData) undoEntry.data).getData());

					currentLeafPage.deleteSortedRecord(
							new RID(currentLeafPage.getCurPage(), (int) currentLeafPage.getSlotCnt() - 1));
				}
			}

			// check whether <key, rid>
			// will be inserted
			// on the newly allocated or on the old leaf page

			if (BT.keyCompare(key, undoEntry.key) >= 0) {
				// the new data entry belongs on the new Leaf page
				newLeafPage.insertRecord(key, rid);

				if (trace != null) {
					trace.writeBytes("PUTIN node " + newLeafPageId + lineSep);
					trace.flush();
				}

			} else {
				currentLeafPage.insertRecord(key, rid);
			}

			unpinPage(currentLeafPageId, true /* dirty */);

			if (trace != null) {
				trace_children(currentLeafPageId);
				trace_children(newLeafPageId);
			}

			// fill upEntry
			tmpEntry = newLeafPage.getFirst(firstRid);
			upEntry = new KeyDataEntry(tmpEntry.key, newLeafPageId);

			unpinPage(newLeafPageId, true /* dirty */);

			// ASSERTIONS:
			// - no pages pinned
			// - upEntry holds the valid KeyDataEntry which is to be inserted
			// on the index page one level up
			return upEntry;
		} else {
			throw new InsertException(null, "");
		}
	}
	
	void trace_children(PageId id)
			throws IOException, IteratorException, ConstructPageException, PinPageException, UnpinPageException {

		if (trace != null) {

			IntervalTSortedPage sortedPage;
			RID metaRid = new RID();
			PageId childPageId;
			KeyClass key;
			KeyDataEntry entry;
			sortedPage = new IntervalTSortedPage(pinPage(id), headerPage.get_keyType());

			// Now print all the child nodes of the page.
			if (sortedPage.getType() == NodeType.INDEX) {
				IntervalTIndexPage indexPage = new IntervalTIndexPage(sortedPage, headerPage.get_keyType());
				trace.writeBytes("INDEX CHILDREN " + id + " nodes" + lineSep);
				trace.writeBytes(" " + indexPage.getPrevPage());
				for (entry = indexPage.getFirst(metaRid); entry != null; entry = indexPage.getNext(metaRid)) {
					trace.writeBytes("   " + ((IndexData) entry.data).getData());
				}
			} else if (sortedPage.getType() == NodeType.LEAF) {
				IntervalTLeafPage leafPage = new IntervalTLeafPage(sortedPage, headerPage.get_keyType());
				trace.writeBytes("LEAF CHILDREN " + id + " nodes" + lineSep);
				for (entry = leafPage.getFirst(metaRid); entry != null; entry = leafPage.getNext(metaRid)) {
					trace.writeBytes("   " + entry.key + " " + entry.data);
				}
			}
			unpinPage(id);
			trace.writeBytes(lineSep);
			trace.flush();
		}

	}
	
	/**
	 * Close the B+ tree file. Unpin header page.
	 * 
	 * @exception PageUnpinnedException       error from the lower layer
	 * @exception InvalidFrameNumberException error from the lower layer
	 * @exception HashEntryNotFoundException  error from the lower layer
	 * @exception ReplacerException           error from the lower layer
	 */
	public void close()
			throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	@Override
	public boolean Delete(KeyClass data, RID rid)
			throws DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
			KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException,
			RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException,
			ConstructPageException, DeleteRecException, IndexSearchException, IOException {
		// TODO Auto-generated method stub
		return false;
	}
}
