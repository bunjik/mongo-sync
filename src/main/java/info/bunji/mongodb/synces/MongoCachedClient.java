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

import java.util.EventListener;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import info.bunji.mongodb.synces.MongoClientService.ClientCacheKey;

/**
 *
 * @author Fumiharu Kinoshita
 */
class MongoCachedClient extends MongoClient {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private final AtomicInteger refCount = new AtomicInteger(0);

	private final ClientCacheKey cacheKey;

	private final Set<Listener> listeners = new HashSet<>();

	/**
	 **********************************
	 * @param cacheKey
	 * @param seeds
	 * @param credentialsList
	 * @param options
	 **********************************
	 */
	public MongoCachedClient(ClientCacheKey cacheKey,
							List<ServerAddress> seeds,
							List<MongoCredential> credentialsList,
							MongoClientOptions options) {
		super(seeds, credentialsList, options);
		this.cacheKey = cacheKey;
	}

	/**
	 **********************************
	 * @param cacheKey
	 * @param server
	 * @param credentialsList
	 * @param options
	 **********************************
	 */
	public MongoCachedClient(ClientCacheKey cacheKey,
							ServerAddress server,
							List<MongoCredential> credentialsList,
							MongoClientOptions options) {
		super(server, credentialsList, options);
		this.cacheKey = cacheKey;
	}

	/**
	 **********************************
	 * 参照数に1加算する.
	 * @return 加算後の参照数
	 **********************************
	 */
	int addRefCount() {
		return refCount.incrementAndGet();
	}

	/*
	 ******************************
	 * (非 Javadoc)
	 * @see com.mongodb.Mongo#close()
	 ******************************
	 */
	@Override
	public void close() {
		if (refCount.decrementAndGet() <= 0) {
			// 参照数が0になったらcloseする
			logger.trace("closing real mongoClient");
			forceClose();
			for (Listener listener : listeners) {
				listener.onCloseClient(cacheKey);
			}
		}
	}

	/**
	 ******************************
	 * 参照数に関わらずクローズする.
	 * <br>
	 * このメソッドによりクローズした場合は、イベントによる通知は行われません。
	 ******************************
	 */
	void forceClose() {
		super.close();
	}

	/**
	 ******************************
	 *
	 * @param listener
	 ******************************
	 */
	void addListener(Listener listener) {
		listeners.add(listener);
	}

	/**
	 ******************************
	 *
	 * @param listener
	 ******************************
	 */
	void removeListener(Listener listener) {
		listeners.remove(listener);
	}


	/**
	 ********************************************
	 *
	 ********************************************
	 */
	public static interface Listener extends EventListener {
		/**
		 ******************************
		 * クライアントのclose時に呼び出されるイベント.
		 * @param cacheKey 接続を一意に特定するキー
		 ******************************
		 */
		void onCloseClient(ClientCacheKey cacheKey);
	}
}
