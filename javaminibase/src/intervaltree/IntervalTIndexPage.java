package intervaltree;

import java.io.IOException;

import btree.ConstructPageException;
import btree.IndexData;
import btree.IndexFullDeleteException;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.NodeType;
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
