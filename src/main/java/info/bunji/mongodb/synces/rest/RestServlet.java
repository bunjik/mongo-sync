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
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.bunji.mongodb.synces.StatusCheckProcess;
import net.arnx.jsonic.JSON;

/**
 * @author Fumiharu Kinoshita
 *
 */
public class RestServlet extends HttpServlet {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private StatusCheckProcess process;

	public RestServlet(StatusCheckProcess process) {
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
			res.setContentType("application/json; charset=utf-8");
			OutputStream os = res.getOutputStream();
			if (params[1].equals("list")) {
				res.setStatus(HttpServletResponse.SC_OK);
				Map<String, Object> results = new TreeMap<>();
				results.put("results", process.getConfigList());
				JSON.encode(results, os);
			} else if (params[1].equals("config") && params.length >= 3) {
//				results.put("results", process.getConfigList().get(params[2]));
				JSON.encode(process.getConfigList().get(params[2]), os);
			} else if (params[1].equals("mapping") && params.length >= 3) {
				res.setStatus(HttpServletResponse.SC_OK);
				Map<String, Object> results = new TreeMap<>();
				results.put("results", process.getMapping(params[2]));
				JSON.encode(results, os);
			} else {
				res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			}
			os.flush();
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

				// stop indexer
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
				process.deleteIndexer(params[2]);
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
