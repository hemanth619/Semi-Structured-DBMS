package intervaltree;

import java.io.IOException;

import btree.BT;
import btree.ConstructPageException;
import btree.IndexData;
import btree.IndexFullDeleteException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.IteratorException;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.NodeType;
import diskmgr.Page;
import global.PageId;
import global.RID;

public class IntervalTIndexPage extends IntervalTSortedPage {

	/**
	 * pin the page with pageno, and get the corresponding BTIndexPage, also it sets
	 * the type of node to be NodeType.INDEX.
	 * 
	 * @param pageno  Input parameter. To specify which page number the BTIndexPage
	 *                will correspond to.
	 * @param keyType either AttrType.attrInteger or AttrType.attrString. Input
	 *                parameter.
	 * @exception IOException            error from the lower layer
	 * @exception ConstructPageException error when BTIndexpage constructor
	 */
	public IntervalTIndexPage(PageId pageno, int keyType) throws IOException, ConstructPageException {
		super(pageno, keyType);
		setType(NodeType.INDEX);
	}

	/**
	 * associate the BTIndexPage instance with the Page instance, also it sets the
	 * type of node to be NodeType.INDEX.
	 * 
	 * @param page    input parameter. To specify which page the BTIndexPage will
	 *                correspond to.
	 * @param keyType either AttrType.attrInteger or AttrType.attrString. Input
	 *                parameter.
	 * @exception IOException            error from the lower layer
	 * @exception ConstructPageException error when BTIndexpage constructor
	 */
	public IntervalTIndexPage(Page page, int keyType) throws IOException, ConstructPageException {
		super(page, keyType);
		setType(NodeType.INDEX);
	}

	/*
	 * new a page, associate the BTIndexPage instance with the Page instance, also
	 * it sets the type of node to be NodeType.INDEX.
	 * 
	 * @param keyType either AttrType.attrInteger or AttrType.attrString. Input
	 * parameter.
	 * 
	 * @exception IOException error from the lower layer
	 * 
	 * @exception ConstructPageException error when BTIndexpage constructor
	 */
	public IntervalTIndexPage(int keyType) throws IOException, ConstructPageException {
		super(keyType);
		setType(NodeType.INDEX);
	}

	/**
	 * Iterators. One of the two functions: getFirst and getNext which provide an
	 * iterator interface to the records on a IntervalTIndexPage.
	 * 
	 * @param rid It will be modified and the first rid in the index page will be
	 *            passed out by itself. Input and Output parameter.
	 * @return return the first KeyDataEntry in the index page. null if NO MORE
	 *         RECORD
	 * @exception IteratorException iterator error
	 */
	public KeyDataEntry getFirst(RID rid) throws IteratorException {

		KeyDataEntry entry;

		try {
			rid.pageNo = getCurPage();
			rid.slotNo = 0; // begin with first slot

			if (getSlotCnt() == 0) {
				return null;
			}

			entry = IntervalT.getEntryFromBytes(getpage(), getSlotOffset(0), getSlotLength(0), keyType, NodeType.INDEX);

			return entry;
		} catch (Exception e) {
			throw new IteratorException(e, "Get first entry failed");
		}

	} // end of getFirst
	
	/**
	 * Iterators. One of the two functions: get_first and get_next which provide an
	 * iterator interface to the records on a BTIndexPage.
	 * 
	 * @param rid It will be modified and next rid will be passed out by itself.
	 *            Input and Output parameter.
	 * @return return the next KeyDataEntry in the index page. null if no more
	 *         record
	 * @exception IteratorException iterator error
	 */
	public KeyDataEntry getNext(RID rid) throws IteratorException {
		KeyDataEntry entry;
		int i;
		try {
			rid.slotNo++; // must before any return;
			i = rid.slotNo;

			if (rid.slotNo >= getSlotCnt()) {
				return null;
			}

			entry = IntervalT.getEntryFromBytes(getpage(), getSlotOffset(i), getSlotLength(i), keyType, NodeType.INDEX);

			return entry;
		} catch (Exception e) {
			throw new IteratorException(e, "Get next entry failed");
		}
	} // end of getNext
	
	/*
	 * This function encapsulates the search routine to search a BTIndexPage by B++
	 * search algorithm
	 * 
	 * @param key the key value used in search algorithm. Input parameter.
	 * 
	 * @return It returns the page_no of the child to be searched next.
	 * 
	 * @exception IndexSearchException Index search failed;
	 */
	PageId getPageNoByKey(KeyClass key) throws IndexSearchException {
		KeyDataEntry entry;
		int i;

		try {

			for (i = getSlotCnt() - 1; i >= 0; i--) {
				entry = IntervalT.getEntryFromBytes(getpage(), getSlotOffset(i), getSlotLength(i), keyType, NodeType.INDEX);

				if (IntervalT.keyCompare(key, entry.key) >= 0) {
					return ((IndexData) entry.data).getData();
				}
			}

			return getPrevPage();
		} catch (Exception e) {
			throw new IndexSearchException(e, "Get entry failed");
		}

	} // getPageNoByKey
	
	/*
	 * find entry for key by B+ tree algorithm, but entry.key may not equal
	 * KeyDataEntry.key returned.
	 * 
	 * @param key input parameter.
	 * 
	 * @return return that entry if found; otherwise return null;
	 * 
	 * @exception IndexSearchException index search failed
	 * 
	 */
	KeyDataEntry findKeyData(KeyClass key) throws IndexSearchException {
		KeyDataEntry entry;

		try {

			for (int i = getSlotCnt() - 1; i >= 0; i--) {
				entry = IntervalT.getEntryFromBytes(getpage(), getSlotOffset(i), getSlotLength(i), keyType, NodeType.INDEX);

				if (IntervalT.keyCompare(key, entry.key) >= 0) {
					return entry;
				}
			}
			return null;
		} catch (Exception e) {
			throw new IndexSearchException(e, "finger key data failed");
		}
	} // end of findKeyData
	
	/*
	 * OPTIONAL: fullDeletekey This is optional, and is only needed if you want to
	 * do full deletion. Return its RID. delete key may != key. But delete key <=
	 * key, and the delete key is the first biggest key such that delete key <= key
	 * 
	 * @param key the key used to search. Input parameter.
	 * 
	 * @exception IndexFullDeleteException if no record deleted or failed by any
	 * reason
	 * 
	 * @return RID of the record deleted. Can not return null.
	 */
	RID deleteKey(KeyClass key) throws IndexFullDeleteException {
		KeyDataEntry entry;
		RID rid = new RID();

		try {

			entry = getFirst(rid);

			if (entry == null)
				// it is supposed there is at least a record
				throw new IndexFullDeleteException(null, "No records found");

			if (IntervalT.keyCompare(key, entry.key) < 0)
				// it is supposed to not smaller than first key
				throw new IndexFullDeleteException(null, "First key is bigger");

			while (IntervalT.keyCompare(key, entry.key) > 0) {
				entry = getNext(rid);
				if (entry == null)
					break;
			}

			if (entry == null)
				rid.slotNo--;
			else if (IntervalT.keyCompare(key, entry.key) != 0)
				rid.slotNo--; // we want to delete the previous key

			deleteSortedRecord(rid);
			return rid;
		} catch (Exception e) {
			throw new IndexFullDeleteException(e, "Full delelte failed");
		}
	} // end of deleteKey
	
	/**
	 * It inserts a <key, pageNo> value into the index page,
	 * 
	 * @key the key value in <key, pageNO>. Input parameter.
	 * @pageNo the pageNo in <key, pageNO>. Input parameter.
	 * @return It returns the rid where the record is inserted; null if no space
	 *         left.
	 * @exception IndexInsertRecException error when insert
	 */
	public RID insertKey(KeyClass key, PageId pageNo) throws IndexInsertRecException {
		RID rid;
		KeyDataEntry entry;
		try {
			entry = new KeyDataEntry(key, pageNo);
			rid = super.insertRecord(entry);
			return rid;
		} catch (Exception e) {
			throw new IndexInsertRecException(e, "Insert failed");

		}
	}

	/*
	 * find the position for old key by findKeyData, where the newKey will be
	 * returned .
	 * 
	 * @newKey It will replace certain key in index page. Input parameter.
	 * 
	 * @oldKey It helps us to find which key will be replaced by the newKey. Input
	 * parameter.
	 * 
	 * @return false if no key was found; true if success.
	 * 
	 * @exception IndexFullDeleteException delete failed
	 */

	boolean adjustKey(KeyClass newKey, KeyClass oldKey) throws IndexFullDeleteException {

		try {

			KeyDataEntry entry;
			entry = findKeyData(oldKey);
			if (entry == null)
				return false;

			RID rid = deleteKey(entry.key);
			if (rid == null)
				throw new IndexFullDeleteException(null, "Rid is null");

			rid = insertKey(newKey, ((IndexData) entry.data).getData());
			if (rid == null)
				throw new IndexFullDeleteException(null, "Rid is null");

			return true;
		} catch (Exception e) {
			throw new IndexFullDeleteException(e, "Adjust key failed");
		}
	} // end of adjustKey
}
