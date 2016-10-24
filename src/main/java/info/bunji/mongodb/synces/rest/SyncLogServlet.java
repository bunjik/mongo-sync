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
package info.bunji.mongodb.synces.rest;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.filter.ThresholdFilter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.CyclicBufferAppender;
import net.arnx.jsonic.JSON;

/**
 * @author Fumiharu Kinoshita
 *
 */
public class SyncLogServlet extends HttpServlet {

	private CyclicBufferAppender<ILoggingEvent> appender = null;

	private static final int LOG_SIZE = 100;

	public SyncLogServlet() {
		// UI出力用のAppender
		Logger rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		if (rootLogger instanceof ch.qos.logback.classic.Logger) {
			appender = new CyclicBufferAppender<>();
			appender.setMaxSize(LOG_SIZE);
			ThresholdFilter filter = new ThresholdFilter();
//			filter.setLevel("INFO");
			filter.setLevel("DEBUG");
			filter.start();
			appender.addFilter(filter);
			((ch.qos.logback.classic.Logger) rootLogger).addAppender(appender);
			appender.start();
		}
	}

	/* (非 Javadoc)
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
		try {
			List<LogMsg> resultList = new ArrayList<>();
			for (int i = 0; i < appender.getLength(); i++) {
				ILoggingEvent event = appender.get(i);
				if (event == null) {
					break;
				}
				// 逆順に挿入
				resultList.add(0, new LogMsg(
										event.getLevel().toString(),
										event.getFormattedMessage(),
										event.getTimeStamp()));
			}

			res.setContentType("application/json; charset=utf-8");
			OutputStream os = res.getOutputStream();
			res.setStatus(HttpServletResponse.SC_OK);

			Map<String, Object> results = new TreeMap<>();
			results.put("results", resultList);
			JSON.encode(results, os);
			os.flush();
		} catch (Exception e) {
			res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}

	static class LogMsg {
		private String message;
		private String level;
		private long timestamp;

		LogMsg(String level, String message, long timestamp) {
			this.message = message;
			this.level = level;
			this.timestamp = timestamp;
		}

		/**
		 * @return message
		 */
		public String getMessage() {
			return message;
		}

		/**
		 * @return level
		 */
		public String getLevel() {
			return level;
		}

		/**
		 * @return timestamp
		 */
		public long getTimestamp() {
			return timestamp;
		}
	}
}
