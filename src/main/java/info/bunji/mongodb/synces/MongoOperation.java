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

import org.bson.BsonTimestamp;
import org.bson.Document;
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
 *
 * @author Fumiharu Kinoshita
 */
public class MongoOperation {

	private static final Logger logger = LoggerFactory.getLogger(MongoOperation.class);

	private final Operation op;
	private final String index;
	private final String collection;
	private final Document  doc;
	private final String id;
	private final BsonTimestamp ts;

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
		// convert Date to JSON string
		builder.registerTypeAdapter(Date.class, new JsonSerializer<Date>() {
			@Override
			public JsonElement serialize(Date src, Type typeOfSrc, JsonSerializationContext context) {
				DateTime dt = new DateTime(src);
				return new JsonPrimitive(dt.toString());
			}
		});

		gson = builder.create();
	}

	public MongoOperation(Operation op, String index, String collection, Document doc, Object ts) {
		this.op = op;
		this.index = index;
		this.collection = collection;
		this.doc = doc;
		this.id = doc != null ? doc.remove("_id").toString() : null;
		this.ts = (BsonTimestamp) ts;
	}

	/**
	 * ステータス更新用
	 * @param op
	 * @param collection
	 * @param doc
	 * @param id
	 */
	public MongoOperation(Operation op, String collection, Document doc, String id) {
		this.op = op;
		this.index = SyncConfig.STATUS_INDEX;
		this.collection = collection;
		this.doc = doc;
		this.id = id;
		this.ts = null;
	}

	public BsonTimestamp getTimestamp() {
		return ts;
	}

	public Operation getOp() {
		return op;
	}

	public String getId() {
		return id;
	}

	public String getIndex() {
		return index;
	}

	public String getCollection() {
		return collection;
	}

	public Document getDoc() {
		return doc;
	}

	public String getJson() {
		return gson.toJson(doc);
	}
}
