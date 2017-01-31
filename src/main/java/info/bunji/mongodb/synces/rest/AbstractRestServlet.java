/*
 * Copyright 2017 Fumiharu Kinoshita
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
import java.io.OutputStreamWriter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;

/**
 ************************************************
 * 
 * 
 * @author Fumiharu Kinoshita
 ************************************************
 */
public abstract class AbstractRestServlet extends HttpServlet {

	/**
	 **********************************
	 * output json response
	 * @param res response
	 * @param src 
	 * @throws IOException
	 **********************************
	 */
	protected void toJsonStream(HttpServletResponse res, Object src) throws IOException {
		res.setContentType("application/json; charset=utf-8");
		res.setStatus(HttpServletResponse.SC_OK);
		OutputStream os = res.getOutputStream();

		// use JSONIC
		//JSON.encode(src, os);
		//os.flush();

		// use Gson
		JsonWriter writer = new JsonWriter(new OutputStreamWriter(os, "UTF8"));
		new Gson().toJson(src, src.getClass(), writer);
		writer.flush();
	}
}
