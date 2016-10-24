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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.bson.BsonTimestamp;

import com.mongodb.ServerAddress;

import net.arnx.jsonic.JSONHint;

/**
 ************************************************
 * 同期設定保持クラス
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class SyncConfig {

	public static final String ID_FIELD = "_id";

	public static final String STATUS_INDEX = ".mongosync";

	/** 同期設定名 */
	private String syncName;

	/** 同期対象のmongodbのデータベース名 */
	private String mongoDbName;

	/** インポート対象のコレクション名 */
	private Set<String> importCollections = new TreeSet<>();

	/** 同期先インデックス名 */
	private String indexName;

	/** 同期対象のフィールド名 */
	private Set<String> includeFields = new TreeSet<>();

	/** 同期対象外のフィールド名 */
	private Set<String> excludeFields = new TreeSet<>();

	/** 同期対象のmongodbの接続情報 */
	private MongoConnection mongoConnection;

	/**  */
	@JSONHint(ignore=true)
	private BsonTimestamp lastOpTime = null;

	/** 同期件数 */
	@JSONHint(ignore=true)
	private AtomicLong syncCount = new AtomicLong(0);

	/** ステータス */
	@JSONHint(ignore=true)
	private Status status;

	public String getSyncName() {
		return syncName;
	}

	public void setSyncName(String syncName) {
		this.syncName = syncName;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
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
	 * 現在の同期件数を返す.
	 * @return 現在の同期件数
	 ********************************************
	 */
	public long getSyncCount() {
		return syncCount.get();
	}

	/**
	 ********************************************
	 * 同期件数1を加算する.
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
	 * 指定数を加算し、加算後の同期件数を返す.
	 * @param delta 加算数
	 * @return 加算後の同期件数
	 ********************************************
	 */
	public long addSyncCount(long delta) {
		return syncCount.addAndGet(delta);
	}

	/**
	 ********************************************
	 * 同期対象のdb名を取得する.
	 * @return 同期対象のdb名
	 ********************************************
	 */
	public String getMongoDbName() {
		return mongoDbName;
	}

	/**
	 ********************************************
	 * 同期対象のdb名を設定する.
	 * @param 同期対象のdb名
	 ********************************************
	 */
	public void setMongoDbName(String mongoDbName) {
		this.mongoDbName = mongoDbName;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public BsonTimestamp getLastOpTime() {
		return lastOpTime;
	}

	public void setLastOpTime(BsonTimestamp lastOpTime) {
		this.lastOpTime = lastOpTime;
	}

	/*
	 * (非 Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
	}

	/**
	 ********************************************
	 * インポート対象のコレクション一覧を返す.
	 * @return コレクション一覧
	 ********************************************
	 */
	public Set<String> getImportCollections() {
		return importCollections;
	}

	/**
	 ********************************************
	 * インポート対象のコレクション一覧を返す.
	 * @return コレクション一覧
	 ********************************************
	 */
	public void setImportCollections(Set<String> importCollections) {
		this.importCollections = importCollections;
	}

	/**
	 ********************************************
	 * 同期対象のコレクションかを返す.
	 * <br>
	 * @param name チェック対象のコレクション名
	 * @return 同期対象の場合はtrue、そうでない場合はfalseを返す。
	 ********************************************
	 */
	public boolean isTargetCollection(String name) {
		if (name == null) return false;
		if (importCollections.isEmpty()) return true;
		return importCollections.contains(name);
	}

	/*
	 * (非 Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object other) {
		boolean ret = false;
		if (other != null && other.getClass().equals(getClass())) {
			SyncConfig config = (SyncConfig) other;

			if (Objects.deepEquals(importCollections, config.getImportCollections())
				&& Objects.equals(mongoDbName, config.getMongoDbName())
				&& Objects.equals(indexName, config.getIndexName())
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
	 * @author Fumiharu Kinoshita
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

		@JSONHint(ignore=true)
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
