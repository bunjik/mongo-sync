/*
 * Copyright 2016 Fumiharu Kinoshita
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.bunji.mongodb.synces;

import java.lang.reflect.Type;
import java.util.Date;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 ************************************************
 * sync operation info.
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class SyncOperation {

	private static final Logger logger = LoggerFactory.getLogger(SyncOperation.class);
	
	private String destDbName;

	private Operation op;
	private String srcDbName;
	private String collection;
	private Document doc = null;
	private String id = null;
	private BsonTimestamp ts;
	private boolean isPartialUpdate = false;

	private static final Gson gson;

	static {
		GsonBuilder builder = new GsonBuilder();

		// convert BsonTimestamp to JSON string
		builder.registerTypeAdapter(BsonTimestamp.class, new JsonSerializer<BsonTimestamp>() {
			@Override
			public JsonElement serialize(BsonTimestamp src, Type typeOfSrc, JsonSerializationContext context) {
				// convert from epoctime
				DateTime dt = new DateTime((long) src.getTime() * 1000L);
				return new JsonPrimitive(dt.toString());
			}
		});
		// convert Date to JSON String
		builder.registerTypeAdapter(Date.class, new JsonSerializer<Date>() {
			@Override
			public JsonElement serialize(Date src, Type typeOfSrc, JsonSerializationContext context) {
				DateTime dt = new DateTime(src);
				return new JsonPrimitive(dt.toString());
			}
		});
		// convert ObjectId to String
		builder.registerTypeAdapter(ObjectId.class, new JsonSerializer<ObjectId>() {
			@Override
			public JsonElement serialize(ObjectId src, Type typeOfSrc, JsonSerializationContext context) {
				return new JsonPrimitive(src.toString());
			}
		});
		gson = builder.create();
	}

	/**
	 **********************************
	 * @param op operartion type
	 * @param destDbName
	 * @param collection target collection
	 * @param doc sync document
	 * @param ts oplog timestamp
	 **********************************
	 */
	public SyncOperation(Operation op, String destDbName, String collection, Document doc, Object ts) {
		this.op = op;
		this.destDbName = destDbName;
		this.collection = collection;
		this.doc = doc;
		//this.id = doc != null ? doc.remove("_id").toString() : null;
		this.id = Objects.toString(doc.remove("_id"), null);
		if (this.id == null) {
			op = Operation.UNKNOWN;
		}
		this.ts = (BsonTimestamp) ts;
	}

	/**
	 **********************************
	 * @param op operartion type
	 * @param destDbName config dbName
	 * @param collection target collection
	 * @param doc sync document
	 * @param id document id
	 **********************************
	 */
	public SyncOperation(Operation op, String collection, Document doc, String id) {
		this.op = op;
		this.destDbName = null;
		this.collection = collection;
		this.doc = doc;
		this.id = id;
		this.ts = null;
	}

	/**
	 **********************************
	 * 
	 * @param oplogDoc
	 * @param destDbName
	 **********************************
	 */
	public SyncOperation(Document oplogDoc, String destDbName) {

		//logger.trace("oplog = \n{}", oplogDoc.toJson());

		this.op = Operation.valueOf(oplogDoc.get("op"));
		this.destDbName = destDbName;

		String ns = oplogDoc.getString("ns");

		String[] nsVals = ns.split("\\.", 2);
		this.srcDbName = nsVals[0];
		this.collection = nsVals[1];

		this.ts = oplogDoc.get("ts", BsonTimestamp.class);

		Document o1Doc = oplogDoc.get("o", Document.class);
		Document o2Doc = oplogDoc.get("o2", Document.class);

		switch (op) {
		case INSERT :
			if (o1Doc != null && o1Doc.containsKey(SyncConfig.ID_FIELD)) {
				this.id = o1Doc.remove(SyncConfig.ID_FIELD).toString();
				this.doc = o1Doc;
			}
			break;
		case DELETE :
			if (o1Doc != null && o1Doc.containsKey(SyncConfig.ID_FIELD)) {
				this.id = o1Doc.remove(SyncConfig.ID_FIELD).toString();
			}
			break;
		case UPDATE :
			if (o1Doc.containsKey("$unset") || o1Doc.containsKey("$set")) {
				this.isPartialUpdate = true;
			} else {
				this.doc = o1Doc;
			}
			//this.id = o2Doc.get("_id").toString();
			this.id = Objects.toString(o2Doc.get("_id"), null);
			if (this.id == null) this.op = Operation.UNKNOWN;
			break;
		case COMMAND :
			if (o1Doc.containsKey("drop")) {
				// drop collection
				this.op = Operation.DROP_COLLECTION;
				this.collection = o1Doc.getString("drop");
			} else if (o1Doc.containsKey("create")) {
				// create collection
				this.collection = o1Doc.getString("create");
				this.op = Operation.CREATE_COLLECTION;
			} else {
				this.op = Operation.UNKNOWN;
			}
			break;
		default :
			break;
		}
	}

	/**
	 **********************************
	 * 
	 * @return 
	 **********************************
	 */
	public boolean isPartialUpdate() {
		return isPartialUpdate;
	}
	
	/**
	 **********************************
	 * get oplog timestamp.
	 * @return oplog timestamp
	 **********************************
	 */
	public BsonTimestamp getTimestamp() {
		return ts;
	}

	/**
	 **********************************
	 * get operation.
	 * @return operartion type
	 **********************************
	 */
	public Operation getOp() {
		return op;
	}

	/**
	 **********************************
	 * get document id.
	 * @return document id
	 **********************************
	 */
	public String getId() {
		return id;
	}

	public String getSrcDbName() {
		return srcDbName;
	}
	
	/**
	 **********************************
	 * 
	 * @return
	 **********************************
	 */
	public String getDestDbName() {
		return destDbName;
	}

	/**
	 **********************************
	 * 
	 * @param destDbName
	 **********************************
	 */
	public void setDestDbName(String destDbName) {
		this.destDbName = destDbName;
	}

	/**
	 **********************************
	 * get target collection.
	 * @return target collection
	 **********************************
	 */
	public String getCollection() {
		return collection;
	}

	/**
	 **********************************
	 * get document.
	 * @return document
	 **********************************
	 */
	public Document getDoc() {
		return doc;
	}

	/**
	 **********************************
	 * get document.
	 * @return document
	 **********************************
	 */
	public void setDoc(Document doc) {
		doc.remove("_id");
		this.doc = doc;
	}


	/**
	 **********************************
	 * get json document .
	 * @return json converted document
	 **********************************
	 */
	public String getJson() {
		return gson.toJson(doc);
	}

	/*
	 **********************************
	 * (非 Javadoc)
	 * @see java.lang.Object#toString()
	 **********************************
	 */
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
	}
}
