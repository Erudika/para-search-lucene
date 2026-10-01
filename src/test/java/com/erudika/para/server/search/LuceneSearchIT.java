/*
 * Copyright 2013-2026 Erudika. http://erudika.com
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
package com.erudika.para.server.search;

import com.erudika.para.core.ParaObject;
import com.erudika.para.core.Sysprop;
import com.erudika.para.core.persistence.DAO;
import com.erudika.para.core.utils.Config;
import com.erudika.para.core.utils.Pager;
import static com.erudika.para.server.search.SearchTest.u;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.*;

/**
 *
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public class LuceneSearchIT extends SearchTest {

	@BeforeAll
	public static void setUpClass() {
		System.setProperty("para.env", "embedded");
		System.setProperty("para.app_name", "para-test");
		System.setProperty("para.cluster_name", "para-test");
		System.setProperty("para.read_from_index", "true");
		System.setProperty("para.es.shards", "2");
		s = new LuceneSearch(mock(DAO.class));
		SearchTest.init();
	}

	@AfterAll
	public static void tearDownClass() {
		SearchTest.cleanup();
	}

	@Test
	public void testRangeQuery() {
		// many terms
		Map<String, Object> terms1 = new HashMap<>();
		terms1.put(Config._TIMESTAMP + " <", 1111111111L);

		Map<String, Object> terms2 = new HashMap<>();
		terms2.put(Config._TIMESTAMP + "<=", u.getTimestamp());

		List<ParaObject> res1 = s.findTerms(u.getType(), terms1, true);
		List<ParaObject> res2 = s.findTerms(u.getType(), terms2, true);

		assertEquals(1, res1.size());
		assertEquals(1, res2.size());

		assertEquals(u.getId(), res1.get(0).getId());
		assertEquals(u.getId(), res2.get(0).getId());
	}

	@Test
	public void testSortBy() {
		// many terms
		Pager p = new Pager(2);
		p.setSortby("number");
		List<ParaObject> res1 = s.findQuery(s1.getType(), "sentence", p);
		assertEquals(2, res1.size());

		p = new Pager(2);
		p.setSortby("properties.number");
		List<ParaObject> res2 = s.findQuery(s1.getType(), "sentence", p);
		assertEquals(2, res2.size());
		assertEquals(s2.getId(), res2.get(0).getId());
		assertEquals(s1.getId(), res2.get(1).getId());
	}

	@Test
	public void testSortByRelevanceOnUnsetSortby() {
		// index two docs, one with a term appearing more often than the other.
		// With an unset sortby the backend must fall back to relevance (score) ordering.
		Sysprop high = new Sysprop("test-score-high");
		high.addProperty("text", "buffalo buffalo buffalo buffalo buffalo");
		Sysprop low = new Sysprop("test-score-low");
		low.addProperty("text", "buffalo");
		s.indexAll(Arrays.asList(high, low));
		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) {
		}

		Pager p = new Pager(10);
		p.setSortby(null);
		List<ParaObject> res = s.findQuery(high.getType(), "properties.text:buffalo", p);
		assertEquals(2, res.size());
		assertEquals(high.getId(), res.get(0).getId());
		assertEquals(low.getId(), res.get(1).getId());

		s.unindexAll(Arrays.asList(high, low));
	}

	@Test
	public void testSortByRelevanceOnExplicitScore() {
		// "sortby=_score" is accepted as an explicit flag for relevance/score ordering.
		Sysprop high = new Sysprop("test-score-high2");
		high.addProperty("text", "sparrow sparrow sparrow sparrow sparrow");
		Sysprop low = new Sysprop("test-score-low2");
		low.addProperty("text", "sparrow");
		s.indexAll(Arrays.asList(high, low));
		try {
			Thread.sleep(1000);
		} catch (InterruptedException ex) {
		}

		Pager p = new Pager(10);
		p.setSortby("_score");
		List<ParaObject> res = s.findQuery(high.getType(), "properties.text:sparrow", p);
		assertEquals(2, res.size());
		assertEquals(high.getId(), res.get(0).getId());
		assertEquals(low.getId(), res.get(1).getId());

		s.unindexAll(Arrays.asList(high, low));
	}

	@Test
	public void testQueryFieldLimit() {
		// 2187 fields - the largest index schema observed in production
		ArrayList<String> fields = new ArrayList<>();
		for (int i = 0; i < 2184; i++) {
			fields.add("field" + i);
		}
		fields.add("name");
		fields.add("tags");
		fields.add("properties.title");

		assertNotNull(LuceneUtils.qs("name:needle", fields));
		// queries with unfielded terms must parse even when the number of indexed fields
		// is above Lucene's max boolean clause count (default is 1024)
		assertNotNull(LuceneUtils.qs("needle", fields));
		assertNotNull(LuceneUtils.qs("properties.space:\"scooldspace:default\" AND (needle)", fields));
		assertNotNull(LuceneUtils.qs("needle one two three", fields)); // multiple unfielded terms
		// invalid query syntax must not fall back to a match-all query
		assertNull(LuceneUtils.qs("AND ? OR", fields));

		List<String> bounded = LuceneUtils.getBoundedQueryFields(fields, 512);
		assertEquals(512, bounded.size());
		assertTrue(bounded.contains("name"));
		assertTrue(bounded.contains("tags"));
//		assertTrue(bounded.contains("properties.title"));
		// the result must be deterministic and must keep all fields when below the limit
		assertEquals(bounded, LuceneUtils.getBoundedQueryFields(fields, 512));
		assertEquals(fields.size(), LuceneUtils.getBoundedQueryFields(fields, fields.size() + 1).size());
	}

	@Test
	public void testUnfieldedQueryTermsWithLargeIndexSchema() {
		// apps with a large index schema (more than 1024 distinct field names) must not fail
		// to parse and execute queries with unfielded terms because of Lucene's max clause count
		ArrayList<Sysprop> props = new ArrayList<>(1101);
		for (int i = 0; i < 1100; i++) {
			Sysprop sp = new Sysprop("large-schema-" + i);
			sp.addProperty("uniqueField" + i, "uniqueValue" + i);
			props.add(sp);
		}
		Sysprop target = new Sysprop("largeSchemaNeedle");
		target.addProperty("space", "scooldspace:default");
		target.addProperty("title", "largeschemaneedle");
		props.add(target);
		s.indexAll(appid1, props);

		// a single unfielded term, as sent by clients when the query is simple
		List<ParaObject> res1 = s.findQuery(appid1, null, "largeschemaneedle");
		assertEquals(1, res1.size());
		assertEquals(target.getId(), res1.get(0).getId());

		// the exact query shape which failed to parse in production
		List<ParaObject> res2 = s.findQuery(appid1, null,
				"properties.space:\"scooldspace:default\" AND (largeschemaneedle)");
		assertEquals(1, res2.size());
		assertEquals(target.getId(), res2.get(0).getId());

		// multiple unfielded terms must also be within the clause count limit
		List<ParaObject> res3 = s.findQuery(appid1, null, "largeschemaneedle largeschemaneedle");
		assertEquals(1, res3.size());

		s.unindexAll(appid1, props);
	}
}
