/**
 *
 */
package info.bunji.mongodb.synces;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

import info.bunji.mongodb.synces.SyncConfig.MongoConnection;
import info.bunji.mongodb.synces.SyncConfig.ServerInfo;

/**
 *
 * @author Fumiharu Kinoshita
 */
public class MongoClientService implements MongoCachedClient.Listener {

	//private static final Logger logger = LoggerFactory.getLogger(MongoClientService.class);

	private static final Map<ClientCacheKey, MongoCachedClient> mongoClients = new HashMap<>();

	private static MongoClientService _instance = new MongoClientService();

	private MongoClientService() {
		// do nothing.
	}

	/**
	 **********************************
	 * MongoClientを取得する.
	 * <pre>
	 * 同一接続先のクライアントが既に存在する場合は、新規の接続ではなく、既存の接続が再利用される。
	 * </pre>
	 * @param config 接続情報
	 * @return MongoClient
	 * @throws UnknownHostException
	 **********************************
	 */
	static MongoClient getClient(SyncConfig config) throws UnknownHostException {
		MongoCachedClient client;
		synchronized (mongoClients) {
			ClientCacheKey cacheKey = new ClientCacheKey(config);
			boolean isValidConnection = false;
			client = mongoClients.get(cacheKey);
			if (client != null) {
				try {
					// validate client
					client.listDatabaseNames();
					isValidConnection = true;
				} catch (Exception me) {
					// needs reconnect
					mongoClients.remove(cacheKey);
					client.forceClose();
				}
			}

			if (!isValidConnection) {
				MongoConnection connInfo = config.getMongoConnection();
				List<ServerAddress> seeds = new ArrayList<>();
				for (ServerInfo info : connInfo.getServerList()) {
					seeds.add(info.getServerAddress());
				}
				List<MongoCredential> credentialList = new ArrayList<>();
				if (!StringUtils.isEmpty(connInfo.getUsername())) {
					String user = connInfo.getUsername();
					char[] pass = Objects.toString(connInfo.getPassword(), "").toCharArray();
					String authDb = connInfo.getAuthDbName();
					credentialList.add(MongoCredential.createCredential(user, authDb, pass));
				}

				// 接続オプション
				MongoClientOptions option = MongoClientOptions.builder()
											.socketKeepAlive(true)
											.readPreference(ReadPreference.primaryPreferred())
											.connectionsPerHost(100).build();

				// TODO sharding未対応
				if (seeds.size() == 1) {
					client = new MongoCachedClient(cacheKey, seeds.get(0), credentialList, option);
				} else {
					client = new MongoCachedClient(cacheKey, seeds, credentialList, option);
				}
				// check conection;
				client.listDatabaseNames();

				// イベントリスナの追加
				client.addListener(_instance);

				mongoClients.put(cacheKey, client);
			}

			client.addRefCount();
		}
		return client;
	}

	/**
	 **********************************
	 * 現在接続中のクライアントをすべてクローズする.
	 **********************************
	 */
	static void closeAllClient() {
		synchronized (mongoClients) {
			for (MongoCachedClient client : mongoClients.values()) {
				client.forceClose();
			}
		}
	}

	/*
	 * (非 Javadoc)
	 * @see info.bunji.mongodb.synces.MongoCachedClient.Listener#onCloseClient(info.bunji.mongodb.synces.MongoClientService.ClientCacheKey)
	 */
	@Override
	public void onCloseClient(ClientCacheKey cacheKey) {
		mongoClients.remove(cacheKey);
	}

	/**
	 *
	 */
	static class ClientCacheKey {
		private MongoConnection connInfo;

		ClientCacheKey(SyncConfig config) {
			this.connInfo = config.getMongoConnection();
		}

		@Override
		public int hashCode() {
			return Objects.hash(
					connInfo.getUsername(),
					connInfo.getPassword(),
					connInfo.getAuthDbName(),
					connInfo.getServerList());
		}

		@Override
		public boolean equals(Object obj) {
			boolean ret = false;
			if (obj != null && obj instanceof ClientCacheKey) {
				ret = connInfo.equals(((ClientCacheKey) obj).connInfo);
			}
			return ret;
		}

		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}
	}
}
