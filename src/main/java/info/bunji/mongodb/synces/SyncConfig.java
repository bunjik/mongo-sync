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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.bson.BsonTimestamp;

import com.mongodb.ServerAddress;

/**
 ************************************************
 * 同期設定保持クラス
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class SyncConfig {

	public static final String ID_FIELD = "_id";

//	public static final String STATUS_INDEX = ".mongosync";

	private String configDbName;

	/** sync name */
	private String syncName;

	/** target mongo database name */
	private String mongoDbName;

	/** インポート対象のコレクション名 */
	private Set<String> importCollections = new TreeSet<>();

	/** 同期先DB名 */
	private String destDbName;

	/** include field names */
	private Set<String> includeFields = new TreeSet<>();

	/** exclude field names */
	private Set<String> excludeFields = new TreeSet<>();

	/** target mongo connection setting */
	private MongoConnection mongoConnection;

	/**  */
	private BsonTimestamp lastOpTime = null;

	/** sync count */
	private AtomicLong syncCount = new AtomicLong(0);

	/** sync status */
	private Status status;

	/** extend information */
	private Map<String, Object> extendInfo = new HashMap<>();

	/**
	 * 
	 */
	public String getConfigDbName() {
		return configDbName;
	}

	public void setConfigDbName(String configDbName) {
		this.configDbName = configDbName;
	}

	/**
	 **********************************
	 * get sync name.
	 * @return sync name
	 **********************************
	 */
	public String getSyncName() {
		return syncName;
	}

	/**
	 **********************************
	 * set sync name.
	 * @param syncName sync name
	 **********************************
	 */
	public void setSyncName(String syncName) {
		this.syncName = syncName;
	}

	/**
	 **********************************
	 * get sync destination name.
	 * @return dest dbname
	 **********************************
	 */
	public String getDestDbName() {
		return destDbName;
	}

	/**
	 **********************************
	 * set sync destination name.
	 * @param destDbName
	 **********************************
	 */
	public void setDestDbName(String destDbName) {
		this.destDbName = destDbName;
	}

	public Set<String> getIncludeFields() {
		return includeFields;
	}

	public void setIncludeFields(Set<String> includeFields) {
		this.includeFields = includeFields;
	}

	public Set<String> getExcludeFields() {
		return excludeFields;
	}

	public void setExcludeFields(Set<String> excludeFields) {
		this.excludeFields = excludeFields;
	}

	public MongoConnection getMongoConnection() {
		return mongoConnection;
	}

	public void setMongoConnection(MongoConnection mongoConnection) {
		this.mongoConnection = mongoConnection;
	}

	/**
	 ********************************************
	 * get current sync count.
	 * @return sync count
	 ********************************************
	 */
	public long getSyncCount() {
		return syncCount.get();
	}

	/**
	 ********************************************
	 * increment sync count.
	 * @return incremented sync count
	 ********************************************
	 */
	public long addSyncCount() {
		long count = 0L;
		while (true) {
			count = syncCount.get();
			long next = count + 1;
			if (count == Long.MAX_VALUE) {
				next = 0L;	// 最大値に到達したら初期化する
			}
			if (syncCount.compareAndSet(count, next)) {
				count = next;
				break;
			}
		}
		return count;
	}

	/**
	 ********************************************
	 * increment sync count.
	 * @param delta increment count
	 * @return incremented sync count
	 ********************************************
	 */
	public long addSyncCount(long delta) {
		return syncCount.addAndGet(delta);
	}

	/**
	 ********************************************
	 * get sync mongo database name.
	 * @return mongo database name
	 ********************************************
	 */
	public String getMongoDbName() {
		return mongoDbName;
	}

	/**
	 ********************************************
	 * set sync mongo database name.
	 * @param mongoDbName mongo database name
	 ********************************************
	 */
	public void setMongoDbName(String mongoDbName) {
		this.mongoDbName = mongoDbName;
	}

	/**
	 ********************************************
	 * get sync status.
	 * @return sync status
	 ********************************************
	 */
	public Status getStatus() {
		return status;
	}

	/**
	 ********************************************
	 * set sync status.
	 * @param status sync status
	 ********************************************
	 */
	public void setStatus(Status status) {
		this.status = status;
	}

	public BsonTimestamp getLastOpTime() {
		return lastOpTime;
	}

	public void setLastOpTime(BsonTimestamp lastOpTime) {
		this.lastOpTime = lastOpTime;
	}

	/**
	 ********************************************
	 * get sync collection names.
	 * @return collection names
	 ********************************************
	 */
	public Set<String> getImportCollections() {
		return importCollections;
	}

	/**
	 ********************************************
	 * set sync collection names.
	 * @param importCollections collection names
	 ********************************************
	 */
	public void setImportCollections(Set<String> importCollections) {
		this.importCollections = importCollections;
	}

	public Map<String, Object> getExtendInfo() {
		return extendInfo;
	}

	/*
	 * (non Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

	/**
	 ********************************************
	 * check sync collection.
	 * @param name check collection name
	 * @return true if sync collection, otherwise false
	 ********************************************
	 */
	public boolean isTargetCollection(String name) {
		if (name == null) return false;
		if (importCollections.isEmpty()) return true;
		return importCollections.contains(name);
	}

	/*
	 **********************************
	 * (non Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 **********************************
	 */
	@Override
	public boolean equals(Object other) {
		boolean ret = false;
		if (other != null && other.getClass() == getClass()) {
			SyncConfig config = (SyncConfig) other;

			if (Objects.deepEquals(importCollections, config.getImportCollections())
				&& Objects.equals(mongoDbName, config.getMongoDbName())
				&& Objects.equals(destDbName, config.getDestDbName())
				&& Objects.deepEquals(includeFields, config.getIncludeFields())
				&& Objects.deepEquals(excludeFields, config.getExcludeFields())
				&& mongoConnection.equals(config.getMongoConnection())) {
				ret = true;
			}
		}
		return ret;
	}

	/**
	 ********************************************
	 * mongodbの接続情報を保持する内部クラス
	 ********************************************
	 */
	public static class MongoConnection {
		private List<ServerInfo> serverList;
		private String authDbName;
		private String username;
		private String password;

		public List<ServerInfo> getServerList() {
			return serverList;
		}

		public void setServerList(List<ServerInfo> serverList) {
			this.serverList = serverList;
		}

		public String getAuthDbName() {
			return authDbName;
		}

		public void setAuthDbName(String authDbName) {
			this.authDbName = authDbName;
		}

		public String getUsername() {
			return username;
		}

		public void setUsername(String username) {
			this.username = username;
		}

		public String getPassword() {
			return password;
		}

		public void setPassword(String password) {
			this.password = password;
		}

		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}

		@Override
		public boolean equals(Object other) {
			boolean ret = false;
			if (other != null && other.getClass().equals(getClass())) {
				MongoConnection conn = (MongoConnection) other;
				if (Objects.equals(serverList, conn.getServerList())
					&& Objects.equals(authDbName, conn.getAuthDbName())
					&& Objects.equals(username, conn.getUsername())
					&& Objects.equals(password, conn.getPassword())) {
					ret = true;
				}
			}
			return ret;
		}

		@Override
		public int hashCode() {
			return Objects.hash(serverList, authDbName, username, password);
		}
	}

	static class ServerInfo {
		private String host;
		private int    port;

		public String getHost() {
			return host;
		}

		public void setHost(String host) {
			this.host = host;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public ServerAddress getServerAddress() throws UnknownHostException {
			return new ServerAddress(InetAddress.getByName(host), port);
		}

		@Override
		public boolean equals(Object other) {
			boolean ret = false;
			if (other != null && other.getClass().equals(getClass())) {
				ServerInfo info = (ServerInfo) other;
				if (Objects.equals(host, info.getHost())
						&& Objects.equals(port, info.getPort())) {
					ret = true;
				}
			}
			return ret;
		}

		@Override
		public int hashCode() {
			return Objects.hash(host, port);
		}

		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}
	}
}
