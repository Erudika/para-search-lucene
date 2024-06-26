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
package com.erudika.para.server.search;

import com.erudika.para.core.ParaObject;
import com.erudika.para.core.persistence.DAO;
import com.erudika.para.core.utils.Config;
import com.erudika.para.core.utils.Pager;
import static com.erudika.para.server.search.SearchTest.u;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
