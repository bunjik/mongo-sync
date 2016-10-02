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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Properties;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.bunji.asyncutil.AsyncExecutor;
import info.bunji.asyncutil.AsyncResult;
import info.bunji.mongodb.synces.rest.SyncConfigServlet;

/**
 *
 *
 * @author Fumiharu Kinoshita
 */
public class MongoEsSync {

	private static Logger logger = LoggerFactory.getLogger(MongoEsSync.class);

	private static Properties defaultProps;

	public static final long CHECK_INTERVAL = 2000;

	static {
		// デフォルト値の設定
		// default
		//   es.host: localhost
		//   es.port: 9300
		//   es.clustername: elasticsearch
		defaultProps = new Properties();
		defaultProps.put("es.host", "localhost");
		defaultProps.put("es.prot", "9300");
		defaultProps.put("es.clustername", "elasticsearch");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		String propFileName = "settings.properties";
		if (args.length > 0) {
			propFileName = args[0];
		}

		// 設定ファイルの読み込み(es接続設定)
		Properties prop = new Properties(defaultProps);
		try (InputStream is = new FileInputStream(propFileName)) {
			prop.load(is);
		} catch (Exception e) {
			logger.error("propertyFile not found. [" + propFileName + "]");
			System.exit(1);
		}

		Settings settings = Settings.settingsBuilder()
		        .put("cluster.name", prop.getProperty("es.clustername"))
		        .build();
		final Client esClient = TransportClient.builder()
				.settings(settings)
				.build()
				.addTransportAddress(new InetSocketTransportAddress(
									InetAddress.getByName(prop.getProperty("es.host")),
									Integer.valueOf(prop.getProperty("es.port"))));

		// 監視スレッドの起動
		StatusCheckProcess process = new StatusCheckProcess(esClient, CHECK_INTERVAL);
		final AsyncResult<Boolean> checker = AsyncExecutor.execute(process);

        // shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					checker.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				MongoClientService.closeAllClient();
				esClient.close();
				logger.debug("stop es sync.");
			}
		});

		// ポート番号は後で外部化
		Server server = new Server(1234);

		// static contents
		ResourceHandler rh = new ResourceHandler();
		rh.setBaseResource(Resource.newClassPathResource("contents"));
		ContextHandler staticContext = new ContextHandler();
		staticContext.setContextPath("/");
		staticContext.setHandler(rh);

		// api
		ServletContextHandler apiContext = new ServletContextHandler(server, "/api");
		apiContext.addServlet(new ServletHolder(new SyncConfigServlet(process)), "/configs/*");

		ContextHandlerCollection handlers = new ContextHandlerCollection();
		handlers.addHandler(staticContext);
		handlers.addHandler(apiContext);
		server.setHandler(handlers);

		server.start();
		server.join();
	}
}