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
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.bunji.mongodb.synces.StatusChecker;

/**
 ************************************************
 *
 * @author Fumiharu Kinoshita
 ************************************************
 */
public class RestServlet extends AbstractRestServlet {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private StatusChecker<?> process;

	public RestServlet(StatusChecker<?> process) {
		this.process = process;
	}

	/* (非 Javadoc)
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
		//logger.debug("call doGet() " + req.getPathInfo());
		try {
			String[] params = req.getPathInfo().split("/");
			if (params[1].equals("list")) {
				Map<String, Object> results = new TreeMap<>();
				results.put("results", process.getConfigs(true));
				toJsonStream(res, results);
			} else if (params[1].equals("config") && params.length >= 3) {
				toJsonStream(res, process.getConfigs(true).get(params[2]));
//			} else if (params[1].equals("mapping") && params.length >= 3) {
//				Map<String, Object> results = new TreeMap<>();
//				results.put("results", process.getMapping(params[2]));
//				toJsonStream(res, results);
			} else {
				res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			}
		} catch (NoNodeAvailableException nnae) {
			//logger.error(e.getMessage(), e);
			res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}

	/* (非 Javadoc)
	 * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
		try {
			String[] params = req.getPathInfo().split("/");
			if (params.length == 3) {
				if (params[1].equals("start")) {
					// start indexer
					process.startIndexer(params[2]);
					res.setStatus(HttpServletResponse.SC_OK);
				} else if (params[1].equals("stop")) {
					// stop indexer
					process.stopIndexer(params[2]);
					res.setStatus(HttpServletResponse.SC_OK);
				} else {
					res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				}
			} else {
				res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}

	/* (非 Javadoc)
	 * @see javax.servlet.http.HttpServlet#doPut(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doPut(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
		// TODO 暫定
		res.setStatus(HttpServletResponse.SC_BAD_REQUEST);

		try {
			String[] params = req.getPathInfo().split("/");
			if (params.length == 3 && params[1].equals("resync")) {
				String syncName = params[2];

				// resync indexer
				process.resyncIndexer(syncName);

				logger.debug("resync started.");
				res.setStatus(HttpServletResponse.SC_OK);
			} else {
				res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}

	/* (非 Javadoc)
	 * @see javax.servlet.http.HttpServlet#doDelete(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doDelete(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
		try {
			String[] params = req.getPathInfo().split("/");
			if (params.length == 3 && params[1].equals("delete")) {
				process.removeConfig(params[2]);
				res.setStatus(HttpServletResponse.SC_OK);
			} else {
				res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}
	}
}
