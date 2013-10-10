/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.examples.wikisearch.query;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.examples.wikisearch.logic.ContentLogic;
import org.apache.accumulo.examples.wikisearch.logic.QueryLogic;
import org.apache.accumulo.examples.wikisearch.sample.Results;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;

import java.util.Arrays;

@Controller
public class QueryController {
	private static final Logger log = Logger.getLogger(QueryController.class);

	@Resource
	private Connector connector;

	@Value("${query.tableName}")
	private String tableName;

	@Value("${query.numThreads}")
	private int numQueryThreads;

	/**
	 * calls the query logic with the parameters, returns results
	 * 
	 * @param query
	 * @param auths auth strings
	 * @return query results
	 */
	@RequestMapping("/query")
	@ResponseBody
	public Results query(@RequestParam String query,
			@RequestParam String[] auths) {
		log.info("Query: " + query + "; auths: " + StringUtils.join(auths, ','));

		QueryLogic table = new QueryLogic();
		table.setTableName(tableName);
		table.setMetadataTableName(tableName + "Metadata");
		table.setIndexTableName(tableName + "Index");
		table.setReverseIndexTableName(tableName + "ReverseIndex");
		table.setQueryThreads(numQueryThreads);
		table.setUnevaluatedFields("TEXT");
		table.setUseReadAheadIterator(false);

		return table.runQuery(connector, Arrays.asList(auths), query, null, null, null);
	}

	/**
	 * Executes a content query with the parameters, returns results.
	 * 
	 * @param content query (see ContentLogic for format)
	 * @param auths auth strings
	 * @return query results
	 */
	@RequestMapping(value="/content")
	@ResponseBody
	public Results content(@RequestParam String query,
			@RequestParam String[] auths) {
		log.info("Content query: " + query + "; auths: " + StringUtils.join(auths, ','));

		ContentLogic table = new ContentLogic();
		table.setTableName(tableName);
		return table.runQuery(connector, query, Arrays.asList(auths));
	}
}
