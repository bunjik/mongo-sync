/**
 *
 */
package info.bunji.mongodb.synces.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBObject;

import info.bunji.mongodb.synces.Status;

/**
 ************************************************
 *
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class DocumentUtils {

	private Logger logger = LoggerFactory.getLogger(DocumentUtils.class);

	private DocumentUtils() {
		// do nothing.
	}

	public static Document fromJson(String json) {
		return Document.parse(json);
	}

	public static Document makeStatusDocument(Status status, Long indexCnt, BsonTimestamp ts) {
		return fromJson(EsUtils.makeStatusJson(status, indexCnt, ts));
	}

	/**
	 ********************************************
	 *
	 * @param orgDoc
	 * @param includeFields
	 * @param excludeFields
	 * @return
	 ********************************************
	 */
	public static Document applyFieldFilter(Document orgDoc, final Set<String> includeFields, final Set<String> excludeFields) {
		Document filterdDoc = applyExcludeFields(orgDoc, excludeFields);
		filterdDoc = applyIncludeFields(orgDoc, includeFields);
		return filterdDoc;
    }

	/**
	 ********************************************
	 *
	 * @param bsonObject
	 * @param excludeFields
	 * @return
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
	 *
	 * @param bsonObject
	 * @param includeFields
	 * @return
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
