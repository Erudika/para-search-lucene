/*
 * Copyright 2013-2022 Erudika. http://erudika.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For issues and patches go to: https://github.com/erudika
 */

module com.erudika.para.server.search.lucene {
	requires com.erudika.para.core;
	requires org.apache.commons.lang3;
	requires com.fasterxml.jackson.core;
	requires com.fasterxml.jackson.databind;
	requires org.slf4j;
	requires java.logging;
	requires jakarta.inject;
	requires org.apache.lucene.analysis.common;
	requires org.apache.lucene.core;
	requires org.apache.lucene.queries;
//	requires org.apache.lucene.spatial3d;
	requires org.apache.lucene.queryparser;
//	requires org.apache.lucene.spatial_extras;
	exports com.erudika.para.server.search;
	provides com.erudika.para.core.search.Search with com.erudika.para.server.search.LuceneSearch;
}