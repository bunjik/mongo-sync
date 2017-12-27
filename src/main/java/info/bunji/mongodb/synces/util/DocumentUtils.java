/**
 *
 */
package info.bunji.mongodb.synces.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBObject;

/**
 ************************************************
 * bson document utility.
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class DocumentUtils {

	@SuppressWarnings("unused")
	private static Logger logger = LoggerFactory.getLogger(DocumentUtils.class);

	private DocumentUtils() {
		// do nothing.
		throw new IllegalAccessError();
	}

	/**
	 **********************************
	 * convert json to Bson document.
	 * @param json json document
	 * @return bson document
	 **********************************
	 */
	public static Document fromJson(String json) {
		return Document.parse(json);
	}

//	/**
//	 ********************************************
//	 * 
//	 * @param status
//	 * @param indexCnt
//	 * @param ts
//	 * @return
//	 ********************************************
//	 */
//	public static Document makeStatusDocument(Status status, Long indexCnt, BsonTimestamp ts) {
//		return fromJson(EsUtils.makeStatusJson(status, indexCnt, ts));
//	}

//	/**
//	 ********************************************
//	 * 
//	 * @param status
//	 * @param id
//	 * @param ts
//	 * @return
//	 ********************************************
//	 */
//	public static Document makeStatusDocument(Status status, String id, BsonTimestamp ts) {
//		Document doc = new Document();
//		doc.append("_id", id);
//		doc.append("status", status.name());
//		if (ts != null) {
//			doc.append("lastOpTime", ts);
//			doc.append("lastSyncTime", ts);
//		}
//		return doc;
//	}

//	// oplog相当のOperationを生成する
//	public static SyncOperation makeStatusOperation(SyncConfig config) {
//		return makeStatusOperation(config.getStatus(), config, config.getLastOpTime(), config.getLastSyncTime());
//	}
//
//	// oplog相当のOperationを生成する
//	public static SyncOperation makeStatusOperation(SyncConfig config, BsonTimestamp lastSyncTime) {
//		return makeStatusOperation(config.getStatus(), config, config.getLastOpTime(), lastSyncTime);
//	}
//
//	// oplog相当のOperationを生成する
//	public static SyncOperation makeStatusOperation(Status status, SyncConfig config, BsonTimestamp ts) {
//		return makeStatusOperation(status, config, ts, ts);
//	}
//	
//	// oplog相当のOperationを生成する
//	public static SyncOperation makeStatusOperation(Status status, SyncConfig config, BsonTimestamp lastOp, BsonTimestamp lastSync) {
//		Document o1Doc = new Document("status", status.name());
//		if (lastOp != null) {
//			o1Doc.append("lastOpTime", new Document("seconds", lastOp.getTime()).append("inc", lastOp.getInc()));
//		}
//		if (lastSync != null) {
//			o1Doc.append("lastSyncTime", new Document("seconds", lastSync.getTime()).append("inc", lastSync.getInc()));
//		}
//
//		Document opDoc = new Document()
//							.append("op", Operation.UPDATE.getValue())
//							.append("ns", config.getMongoDbName() + ".status")
//							.append("o", o1Doc)
//							.append("o2", new Document("_id", config.getSyncName()))
//							.append("ts", lastOp);
////		return new SyncOperation(opDoc, config.getConfigDbName());
//		SyncOperation op = new SyncOperation(opDoc, config.getConfigDbName());
////		logger.debug(op.toString());
//		return op;
//	}

	/**
	 ********************************************
	 * apply field filter.
	 * @param orgDoc document to filter
	 * @param includeFields include fields
	 * @param excludeFields exclude fields
	 * @return filtered document
	 ********************************************
	 */
	public static Document applyFieldFilter(Document orgDoc, final Set<String> includeFields, final Set<String> excludeFields) {
		Document filterdDoc = applyExcludeFields(orgDoc, excludeFields);
		filterdDoc = applyIncludeFields(orgDoc, includeFields);
		return filterdDoc;
    }

	/**
	 ********************************************
	 * convert BsonTimestamp to Date string.
	 * @param ts BsonTimestamp
	 * @return converted Date
	 ********************************************
	 */
	public static String toDateStr(BsonTimestamp ts) {
		if (ts != null) {
			DateTimeFormatter formatter = DateTimeFormat.longDateTime();
			return formatter.print((long)ts.getTime() * 1000);
		}
		return "";
	}
	
	/**
	 ********************************************
	 * apply exclude field filter.
	 * @param bsonObject
	 * @param excludeFields exclude fields
	 * @return filtered document
	 ********************************************
	 */
	private static Document applyExcludeFields(Document bsonObject, Set<String> excludeFields) {
		if (excludeFields == null || excludeFields.isEmpty()) {
            return bsonObject;
		}

		Document filteredObject = bsonObject;
		for (String field : excludeFields) {
			if (field.contains(".")) {
				String rootObject = field.substring(0, field.indexOf("."));
				String childObject = field.substring(field.indexOf(".") + 1);
				if (filteredObject.containsKey(rootObject)) {
					Object object = filteredObject.get(rootObject);
					if (object instanceof DBObject) {
						Document object2 = (Document) object;
						object2 = applyExcludeFields(object2, new HashSet<String>(Arrays.asList(childObject)));
					}
				}
			} else {
				if (filteredObject.containsKey(field)) {
					filteredObject.remove(field);
				}
			}
		}
		return filteredObject;
	}

	/**
	 ********************************************
	 * apply include field filter.
	 * @param bsonObject
	 * @param includeFields include fields
	 * @return filtered document
	 ********************************************
	 */
	private static Document applyIncludeFields(Document bsonObject, final Set<String> includeFields) {
		if (includeFields == null || includeFields.isEmpty()) {
			return bsonObject;
		}

		Document filteredObject = new Document();
		for (String field : includeFields) {
			if (field.contains(".")) {
				String rootObject = field.substring(0, field.indexOf("."));
				Object object = bsonObject.get(rootObject);
				if (object instanceof Document) {
					Document object2 = (Document) object;
					object2 = applyIncludeFields(object2, getChildItems(rootObject, includeFields));
				}
			} else if (includeFields.contains(field)) {
				filteredObject.put(field, bsonObject.get(field));
			}
		}
		return filteredObject;
	}

	/**
	 ********************************************
	 *
	 * @param parent
	 * @param fields
	 * @return
	 ********************************************
	 */
	private static Set<String> getChildItems(String parent, final Set<String> fields) {
		Set<String> children = new HashSet<>();
		for (String field : fields) {
			if (field.startsWith(parent + ".")) {
				children.add(field.substring((parent + ".").length()));
			} else if (field.startsWith(parent)) {
				children.add(field);
			}
		}
		return children;
	}
}
