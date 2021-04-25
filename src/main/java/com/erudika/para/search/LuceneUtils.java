/*
 * Copyright 2013-2021 Erudika. http://erudika.com
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
package com.erudika.para.search;

import com.erudika.para.DestroyListener;
import com.erudika.para.Para;
import com.erudika.para.core.Address;
import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.utils.ParaObjectUtils;
import com.erudika.para.persistence.DAO;
import com.erudika.para.utils.Config;
import static com.erudika.para.utils.Config.DEFAULT_LIMIT;
import com.erudika.para.utils.Pager;
import com.erudika.para.utils.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import static org.apache.lucene.search.SortField.Type.DOUBLE;
import static org.apache.lucene.search.SortField.Type.LONG;
import static org.apache.lucene.search.SortField.Type.STRING;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lucene related utility methods.
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public final class LuceneUtils {

	private static final Logger logger = LoggerFactory.getLogger(LuceneUtils.class);
	protected static final String NESTED_FIELD_NAME = "nstd";
	private static final String SOURCE_FIELD_NAME = "_source";
	private static final String DOC_ID_FIELD_NAME = "_docid";
	private static final FieldType ID_FIELD;
	private static final FieldType DOC_ID_FIELD;
	private static final FieldType SOURCE_FIELD;
	private static final FieldType DEFAULT_FIELD;
	private static final FieldType DEFAULT_NOT_ANALYZED_FIELD;
	private static final Set<String> NOT_ANALYZED_FIELDS;
	private static final Analyzer QUERY_STRING_ANALYZER;
	private static final String[] IGNORED_FIELDS;
	private static final int FIELD_LIMIT = 16383; // 32766 / 2; = 2 bytes per char, max field length must be <= 32766

	private static final Map<String, IndexWriter> WRITERS = new ConcurrentHashMap<String, IndexWriter>();
//	private static S3Directory s3Directory;

	/**
	 * Default analyzer.
	 */
	protected static final Analyzer ANALYZER;

	static {
		SOURCE_FIELD = new FieldType();
		SOURCE_FIELD.setIndexOptions(IndexOptions.NONE);
		SOURCE_FIELD.setStored(true);
		SOURCE_FIELD.setTokenized(false);

		ID_FIELD = new FieldType();
		ID_FIELD.setStored(true);
		ID_FIELD.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
		ID_FIELD.setTokenized(false);

		DOC_ID_FIELD = new FieldType();
		DOC_ID_FIELD.setStored(true);
		DOC_ID_FIELD.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
		DOC_ID_FIELD.setTokenized(false);

		DEFAULT_FIELD = new FieldType();
		DEFAULT_FIELD.setStored(false);
		DEFAULT_FIELD.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
		DEFAULT_FIELD.setTokenized(true);
		DEFAULT_FIELD.setStoreTermVectorPayloads(true);
		DEFAULT_FIELD.setStoreTermVectorPositions(true);
		DEFAULT_FIELD.setStoreTermVectorOffsets(true);
		DEFAULT_FIELD.setStoreTermVectors(true);
		DEFAULT_FIELD.setOmitNorms(false);
		DEFAULT_FIELD.setDocValuesType(DocValuesType.NONE);

		DEFAULT_NOT_ANALYZED_FIELD = new FieldType();
		DEFAULT_NOT_ANALYZED_FIELD.setStored(false);
		DEFAULT_NOT_ANALYZED_FIELD.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
		DEFAULT_NOT_ANALYZED_FIELD.setTokenized(false);
		DEFAULT_NOT_ANALYZED_FIELD.setStoreTermVectorPayloads(true);
		DEFAULT_NOT_ANALYZED_FIELD.setStoreTermVectorPositions(true);
		DEFAULT_NOT_ANALYZED_FIELD.setStoreTermVectorOffsets(true);
		DEFAULT_NOT_ANALYZED_FIELD.setStoreTermVectors(true);
		DEFAULT_NOT_ANALYZED_FIELD.setOmitNorms(false);
		DEFAULT_NOT_ANALYZED_FIELD.setDocValuesType(DocValuesType.NONE);

		ANALYZER = new StandardAnalyzer();

		NOT_ANALYZED_FIELDS = new HashSet<>();
		NOT_ANALYZED_FIELDS.add("nstd");
		NOT_ANALYZED_FIELDS.add("latlng");
		NOT_ANALYZED_FIELDS.add("tag");
		NOT_ANALYZED_FIELDS.add("id");
		NOT_ANALYZED_FIELDS.add("key");
		NOT_ANALYZED_FIELDS.add("name");
		NOT_ANALYZED_FIELDS.add("type");
		NOT_ANALYZED_FIELDS.add("tags");
		NOT_ANALYZED_FIELDS.add("email");
		NOT_ANALYZED_FIELDS.add("appid");
		NOT_ANALYZED_FIELDS.add("votes");
		NOT_ANALYZED_FIELDS.add("groups");
		NOT_ANALYZED_FIELDS.add("updated");
		NOT_ANALYZED_FIELDS.add("password");
		NOT_ANALYZED_FIELDS.add("parentid");
		NOT_ANALYZED_FIELDS.add("creatorid");
		NOT_ANALYZED_FIELDS.add("timestamp");
		NOT_ANALYZED_FIELDS.add("identifier");
		NOT_ANALYZED_FIELDS.add("nstdparentid");
		NOT_ANALYZED_FIELDS.add("token");

		KeywordAnalyzer ka = new KeywordAnalyzer();
		QUERY_STRING_ANALYZER = new PerFieldAnalyzerWrapper(ANALYZER,
				NOT_ANALYZED_FIELDS.stream().collect(Collectors.toMap(field -> field, f -> ka)));

		// these fields are not indexed
		IGNORED_FIELDS = new String[] {
			"settings", // App
			"datatypes", // App
			"deviceState", // Thing
			"deviceMetadata", // Thing
			"resourcePermissions", // App
			"validationConstraints" // App
		};
	}

	private LuceneUtils() { }

	/**
	 * Indexes documents.
	 * @param appid appid
	 * @param docs a list of documents
	 */
	public static void indexDocuments(String appid, List<Document> docs) {
		if (docs.isEmpty()) {
			return;
		}
		IndexWriter iwriter = null;
		try {
			iwriter = getIndexWriter(appid);
			if (iwriter != null) {
				for (Document doc : docs) {
					if (doc.get(Config._ID) != null) {
						try {
							iwriter.updateDocument(new Term(Config._ID, doc.get(Config._ID)), doc);
						} catch (Exception e) {
							logger.error("Failed to index document " + doc.get(Config._TYPE) + ":" + doc.get(Config._ID)
									+ " from batch of " + docs.size() + " for app '" + appid + "'", e);
						}
					}
				}
				iwriter.commit();
			}
		} catch (Exception ex) {
			logger.error(null, ex);
		}
	}

	/**
	 * Removes documents from Lucene index.
	 * @param appid appid
	 * @param ids a list of ids to remove
	 */
	public static void unindexDocuments(String appid, List<String> ids) {
		if (ids.isEmpty()) {
			return;
		}
		IndexWriter iwriter = null;
		try {
			iwriter = getIndexWriter(appid);
			if (iwriter != null) {
				ArrayList<Term> keys = new ArrayList<>();
				for (String id : ids) {
					if (id != null) {
						keys.add(new Term(Config._ID, id));
					}
				}
				iwriter.deleteDocuments(keys.toArray(new Term[0]));
				iwriter.commit();
			}
		} catch (Exception ex) {
			logger.error("Failed to unindex " + ids.size() + " documents for app '" + appid + "'", ex);
		}
	}

	/**
	 * Removes documents from Lucene index, matching a query.
	 * @param appid appid
	 * @param query queries which documents to remove
	 */
	public static void unindexDocuments(String appid, Query query) {
		if (query == null) {
			return;
		}
		IndexWriter iwriter = null;
		try {
			iwriter = getIndexWriter(appid);
			if (iwriter != null) {
				iwriter.deleteDocuments(query);
				iwriter.commit();
			}
		} catch (Exception ex) {
			logger.error("Failed to unindex documents matching '" + query.toString() + "' for app '" + appid + "'", ex);
		}
	}

	/**
	 * Deletes the entire index.
	 * @param appid appid
	 */
	public static void deleteIndex(String appid) {
		IndexWriter iwriter = null;
		try {
			iwriter = getIndexWriter(appid);
			if (iwriter != null) {
				iwriter.deleteAll();
				iwriter.commit();
			}
		} catch (Exception ex) {
			logger.error("Failed to delete index for app '" + appid + "'", ex);
		}
	}

	/**
	 * Rebuilds an index. Reads objects from the data store and indexes them. Works on one DB table and index only.
	 *
	 * @param dao DAO for connecting to the DB - the primary data source
	 * @param app an app
	 * @param pager a Pager instance
	 * @return true if successful, false if index doesn't exist or failed.
	 */
	public static boolean rebuildIndex(DAO dao, App app, Pager... pager) {
		if (app == null || StringUtils.isBlank(app.getAppIdentifier()) || dao == null) {
			return false;
		}
		try {
			String indexName = app.getAppIdentifier().trim();
			logger.info("Deleting '{}' index before rebuilding it...", indexName);
			deleteIndex(indexName);
			logger.debug("rebuildIndex(): {}", indexName);
			Pager p = getPager(pager);
			int batchSize = Config.getConfigInt("reindex_batch_size", p.getLimit());
			long reindexedCount = 0;

			List<ParaObject> list;
			List<Document> docs = new LinkedList<Document>();
			do {
				list = dao.readPage(app.getAppIdentifier(), p); // use appid!
				logger.debug("rebuildIndex(): Read {} objects from table {}.", list.size(), indexName);
				for (ParaObject obj : list) {
					if (obj != null) {
						// put objects from DB into the newly created index
						Map<String, Object> data = ParaObjectUtils.getAnnotatedFields(obj, null, false);
						docs.add(paraObjectToDocument(indexName, data));
						reindexedCount++;
						if (docs.size() >= batchSize) {
							indexDocuments(indexName, docs);
							docs.clear();
						}
					}
				}
			} while (!list.isEmpty());

			// anything left after loop? index that too
			if (!docs.isEmpty()) {
				indexDocuments(indexName, docs);
			}
			logger.info("rebuildIndex(): Done. {} objects reindexed.", reindexedCount);
		} catch (Exception e) {
			logger.warn(null, e);
			return false;
		}
		return true;
	}

	private static void addDocumentFields(JsonNode object, Document doc) {
		try {
			LinkedList<Map<String, JsonNode>> stack = new LinkedList<>();
			stack.add(Collections.singletonMap("", object));
			while (!stack.isEmpty()) {
				Map<String, JsonNode> singletonMap = stack.pop();
				String prefix = singletonMap.keySet().iterator().next();
				JsonNode value = singletonMap.get(prefix);
				if (!StringUtils.equalsAny(prefix, IGNORED_FIELDS) && value != null) {
					switch (value.getNodeType()) {
						case OBJECT:
							String pre = (StringUtils.isBlank(prefix) ? "" : prefix + ".");
							for (Iterator<Entry<String, JsonNode>> iterator1 = value.fields(); iterator1.hasNext();) {
								Entry<String, JsonNode> entry = iterator1.next();
								addFieldToStack(pre + entry.getKey(), entry.getValue(), stack, doc);
							}
							break;
						default:
							addFieldToStack(prefix, value, stack, doc);
							break;
					}
				}
			}
		} catch (Exception e) {
			logger.error(null, e);
		}
	}

	private static void addFieldToStack(String prefix, JsonNode val, LinkedList<Map<String, JsonNode>> stack, Document doc)
			throws JsonProcessingException {
		switch (val.getNodeType()) {
			case OBJECT:
				stack.push(Collections.singletonMap(prefix, val));
				break;
			case ARRAY:
				StringBuilder sb = new StringBuilder();
				for (Iterator<JsonNode> iterator1 = val.elements(); iterator1.hasNext();) {
					JsonNode next = iterator1.next();
					String textValue;
					if (next.isObject() || next.isArray()) {
						textValue = ParaObjectUtils.getJsonWriterNoIdent().writeValueAsString(next);
					} else {
						textValue = next.asText();
					}
					if (!StringUtils.isBlank(textValue)) {
						sb.append(textValue).append(",");
						doc.add(getField(prefix, textValue));
					}
				}
				if (sb.length() > 0) {
					addSortedDocValuesField(doc, prefix, sb.toString());
				}
				break;
			case NULL:
				break;
			default:
				String txt = val.asText("null");
				Field f = getField(prefix, txt);
				if (!(f instanceof LatLonPoint)) {
					if (val.isNumber()) {
						switch (val.numberType()) {
							case FLOAT:
							case DOUBLE:
								doc.add(new SortedNumericDocValuesField(prefix,
										NumericUtils.doubleToSortableLong(val.asDouble())));
								break;
							default:
								doc.add(new SortedNumericDocValuesField(prefix, val.asLong()));
						}
					} else {
						addSortedDocValuesField(doc, prefix, txt);
					}
				}
				doc.add(f);
				break;
		}
	}

	private static void addSortedDocValuesField(Document doc, String prefix, String txt) {
		if (txt.length() > FIELD_LIMIT) {
			int i = 0;
			for (int start = 0; start < txt.length(); start += FIELD_LIMIT) {
				String s = txt.substring(start, Math.min(txt.length(), start + FIELD_LIMIT));
				doc.add(new SortedDocValuesField(prefix + (start > 0 ? String.valueOf(++i) : ""),
						new BytesRef(s.getBytes())));
			}
		} else {
			doc.add(new SortedDocValuesField(prefix, new BytesRef(txt.getBytes())));
		}
	}

	private static Field getField(String field, String value) {
		if ("latlng".equals(field) && StringUtils.contains(value, ",")) {
			String[] latlng = value.split(",", 2);
			return new LatLonPoint(field, NumberUtils.toDouble(latlng[0]), NumberUtils.toDouble(latlng[1]));
		} else if (Config._ID.equals(field)) {
			return new Field(field, value, ID_FIELD);
		} else {
			if (NOT_ANALYZED_FIELDS.contains(field)) {
				return new Field(field, value, DEFAULT_NOT_ANALYZED_FIELD);
			} else {
				return new Field(field, value, DEFAULT_FIELD);
			}
		}
	}

	/**
	 * Converts a ParaObject to Lucene Document. Takes care of nested objects and indexes them separately.
	 * Stores the original object data as JSON text inside the "_source" field.
	 * @param appid an appid
	 * @param data object data - keys and values
	 * @return a {@link Document} object
	 */
	@SuppressWarnings("unchecked")
	public static Document paraObjectToDocument(String appid, Map<String, Object> data) {
		if (data == null) {
			throw new IllegalArgumentException("Null data");
		}
		Document doc = new Document();
		JsonNode jsonDoc = null;
		try {
			// Process nested fields first
			// Nested objects are stored as independent documents in Lucene.
			// They are not shown in search results.
			if (data.containsKey(NESTED_FIELD_NAME)) {
				Object nstd = data.get(NESTED_FIELD_NAME);
				if (nstd instanceof List) {
					LinkedList<Document> docs = new LinkedList<>();
					Map<String, Object> dataWithNestedField = new HashMap<>(data);
					//dataWithNestedField.remove(NESTED_FIELD_NAME);
					dataWithNestedField.put("nstdobject", "true");
					jsonDoc = ParaObjectUtils.getJsonMapper().valueToTree(dataWithNestedField);
					for (Map<String, Object> obj : ((List<Map<String, Object>>) nstd)) {
						Map<String, Object> object = new HashMap<>(obj);
						object.put(Config._ID, Utils.getNewId());
						// the nested object's type is forced to be equal to its parent, otherwise breaks queries
						object.put(Config._TYPE, data.get(Config._TYPE));
						object.put(NESTED_FIELD_NAME + Config._PARENTID, data.get(Config._ID));
						JsonNode nestedJsonDoc = ParaObjectUtils.getJsonMapper().valueToTree(object);
						Document nestedDoc = new Document();
						addDocumentFields(nestedJsonDoc, nestedDoc);
						addSource(jsonDoc, nestedDoc); // nested field has the source of its parent
						docs.add(nestedDoc);
					}
					indexDocuments(appid, docs);
				}
			} else {
				jsonDoc = ParaObjectUtils.getJsonMapper().valueToTree(data);
			}
			addDocumentFields(jsonDoc, doc);
			addSource(jsonDoc, doc);
		} catch (Exception e) {
			logger.error(null, e);
		}
		return doc;
	}

	private static void addSource(JsonNode jsonDoc, Document doc) {
		try {
			doc.add(new Field(LuceneUtils.SOURCE_FIELD_NAME,
					ParaObjectUtils.getJsonWriterNoIdent().writeValueAsString(jsonDoc), SOURCE_FIELD));
			// add special DOC ID field, used in "search after
			String docId = Utils.getNewId();
			doc.add(new SortedNumericDocValuesField(DOC_ID_FIELD_NAME, NumberUtils.toLong(docId)));
			doc.add(new Field(DOC_ID_FIELD_NAME, docId, DOC_ID_FIELD));
		} catch (JsonProcessingException ex) {
			logger.error(null, ex);
		}
	}

	/**
	 * Reads the JSON from "_source" field of a document and turns it into a ParaObject.
	 * @param <P> type
	 * @param doc Lucene document
	 * @return a ParaObject or null if source is missing
	 */
	public static <P extends ParaObject> P documentToParaObject(Document doc) {
		if (doc == null || StringUtils.isBlank(doc.get(SOURCE_FIELD_NAME))) {
			return null;
		}
		return ParaObjectUtils.fromJSON(doc.get(SOURCE_FIELD_NAME));
	}

	private static <P extends ParaObject> P readObjectFromIndex(Document hit) {
		P result = documentToParaObject(hit);
		if (result != null) {
			logger.debug("Search result from index: appid={}, id={}", result.getAppid(), result.getId());
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private static <P extends ParaObject> List<P> readResultsFromDatabase(DAO dao, String appid,
			Map<String, P> keysAndSources) {
		if (keysAndSources == null || keysAndSources.isEmpty()) {
			return Collections.emptyList();
		}
		ArrayList<P> results = new ArrayList<>(keysAndSources.size());
		ArrayList<String> nullz = new ArrayList<>(results.size());
		Map<String, P> fromDB = dao.readAll(appid, new ArrayList<>(keysAndSources.keySet()), true);
		for (Map.Entry<String, P> entry : keysAndSources.entrySet()) {
			String key = entry.getKey();
			P pobj = fromDB.get(key);
			if (pobj == null) {
				pobj = entry.getValue();
				// object is still in index but not in DB
				if (pobj != null && appid.equals(pobj.getAppid()) && pobj.getStored()) {
					nullz.add(key);
				}
			}
			if (pobj != null) {
				results.add(pobj);
			}
		}

		if (!nullz.isEmpty()) {
			logger.warn("Found {} objects that are still indexed but deleted from the database: {}. "
					+ "Sometimes this happens if you do a search right after a delete operation.",
					nullz.size(), nullz);
		}
		return results;
	}

	/**
	 * Geopoint distance query. Finds objects located near a center point.
	 * @param <P> object type
	 * @param dao {@link DAO}
	 * @param appid appid
	 * @param type object type to search for
	 * @param query a geopoint query
	 * @param queryString query string for filtering results
	 * @param pager a {@link Pager}
	 * @return a list of ParaObjects
	 */
	public static <P extends ParaObject> List<P> searchGeoQuery(DAO dao, String appid, String type,
			Query query, String queryString, Pager... pager) {
		if (StringUtils.isBlank(type) || StringUtils.isBlank(appid)) {
			return Collections.emptyList();
		}
		if (StringUtils.isBlank(queryString)) {
			queryString = "*";
		}
		DirectoryReader ireader = null;
		try {
			Pager page = getPager(pager);
			ireader = getIndexReader(appid);
			if (ireader != null) {
				Document[] hits1 = searchQueryRaw(ireader, appid, Utils.type(Address.class), query, page);
				page.setLastKey(null); // will cause problems if not cleared

				if (hits1.length == 0) {
					return Collections.emptyList();
				}

				if (type.equals(Utils.type(Address.class))) {
					return searchQuery(dao, appid, hits1, page);
				}

				// then searchQuery their parent objects
				ArrayList<String> parentids = new ArrayList<>(hits1.length);
				for (Document doc : hits1) {
					Address address = documentToParaObject(doc);
					if (address != null && !StringUtils.isBlank(address.getParentid())) {
						parentids.add(address.getParentid());
					}
				}

				Builder qsPart = new BooleanQuery.Builder();
				qsPart.add(qs(queryString, FieldInfos.getIndexedFields(ireader)), BooleanClause.Occur.MUST);
				Builder filterIdsPart = new BooleanQuery.Builder();
				for (String id : parentids) {
					filterIdsPart.add(new TermQuery(new Term(Config._ID, id)), BooleanClause.Occur.SHOULD);
				}
				qsPart.add(filterIdsPart.build(), BooleanClause.Occur.FILTER);
				Document[] hits2 = searchQueryRaw(ireader, appid, type, qsPart.build(), page);
				return searchQuery(dao, appid, hits2, page);
			}
		} catch (Exception e) {
			logger.error(null, e);
		} finally {
			closeIndexReader(ireader);
		}
		return Collections.emptyList();
	}

	/**
	 * Searches the Lucene index of a particular appid.
	 * @param <P> type
	 * @param dao {@link DAO}
	 * @param appid appid
	 * @param type type
	 * @param query a query
	 * @param pager a {@link Pager}
	 * @return a list of ParaObjects
	 */
	public static <P extends ParaObject> List<P> searchQuery(DAO dao, String appid, String type, String query, Pager... pager) {
		if (StringUtils.isBlank(appid)) {
			return Collections.emptyList();
		}
		DirectoryReader ireader = null;
		try {
			ireader = getIndexReader(appid);
			if (ireader != null) {
				Pager page = getPager(pager);
				List<P> docs = searchQuery(dao, appid, searchQueryRaw(ireader, appid, type,
						qs(query, FieldInfos.getIndexedFields(ireader)), page), page);
				return docs;
			}
		} catch (Exception e) {
			logger.error(null, e);
		} finally {
			closeIndexReader(ireader);
		}
		return Collections.emptyList();
	}

	/**
	 * Searches the Lucene index of a particular appid.
	 * @param <P> type
	 * @param dao {@link DAO}
	 * @param appid appid
	 * @param type type
	 * @param query a query
	 * @param pager a {@link Pager}
	 * @return a list of ParaObjects
	 */
	public static <P extends ParaObject> List<P> searchQuery(DAO dao, String appid, String type, Query query, Pager... pager) {
		if (StringUtils.isBlank(appid)) {
			return Collections.emptyList();
		}
		DirectoryReader ireader = null;
		try {
			ireader = getIndexReader(appid);
			if (ireader != null) {
				Pager page = getPager(pager);
				List<P> docs = searchQuery(dao, appid, searchQueryRaw(ireader, appid, type, query, page), page);
				return docs;
			}
		} catch (Exception e) {
			logger.error(null, e);
		} finally {
			closeIndexReader(ireader);
		}
		return Collections.emptyList();
	}

	@SuppressWarnings("unchecked")
	private static <P extends ParaObject> List<P> searchQuery(DAO dao, String appid, Document[] hits, Pager pager) {
		if (hits == null || hits.length == 0) {
			return Collections.emptyList();
		}
		ArrayList<P> results = new ArrayList<>(hits.length);
		LinkedHashMap<String, P> keysAndSources = new LinkedHashMap<>(hits.length);
		try {
			boolean readFromIndex = Config.getConfigBoolean("read_from_index", false);
			for (Document hit : hits) {
				P result = readObjectFromIndex(hit);
				if (result != null) {
					if (keysAndSources.containsKey(result.getId())) {
						pager.setCount(pager.getCount() - 1); // substract duplicates due to nested objects (nstd)
					} else {
						results.add(result);
						keysAndSources.put(result.getId(), result);
					}
				}
			}
			if (!readFromIndex) {
				return readResultsFromDatabase(dao, appid, keysAndSources);
			}
		} catch (Exception e) {
			logger.error(null, e);
		}
		return results;
	}

	private static Document[] searchQueryRaw(DirectoryReader ireader, String appid, String type, Query query, Pager pager) {
		if (StringUtils.isBlank(appid) || ireader == null) {
			return new Document[0];
		}
		if (query == null) {
			query = new MatchAllDocsQuery();
		}
		if (pager == null) {
			pager = new Pager();
		}
		try {
			IndexSearcher isearcher = new IndexSearcher(ireader);
			if (!StringUtils.isBlank(type)) {
				query = new BooleanQuery.Builder().
						add(query, BooleanClause.Occur.MUST).
						add(new TermQuery(new Term(Config._TYPE, type)), BooleanClause.Occur.FILTER).
						build();
			}
			int maxPerPage = pager.getLimit();
			int pageNum = (int) pager.getPage();
			TopDocs topDocs;

			if (pageNum <= 1 && !StringUtils.isBlank(pager.getLastKey())) {
				// Read the last Document from index to get its docId which is required by "searchAfter".
				// We can't get it from lastKey beacuse it contains the id of the last ParaObject on the page.
				Integer lastDocId = getLastDocId(isearcher, pager.getLastKey());
				if (lastDocId != null) {
					topDocs = isearcher.searchAfter(new FieldDoc(lastDocId, 1,
							new Object[]{NumberUtils.toLong(pager.getLastKey())}), query, maxPerPage,
							new Sort(new SortedNumericSortField(DOC_ID_FIELD_NAME, LONG, pager.isDesc())));
				} else {
					topDocs = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
				}
			} else {
				int start = (pageNum < 1 || pageNum > Config.MAX_PAGES) ? 0 : (pageNum - 1) * maxPerPage;
				Sort sort = new Sort(getSortFieldForQuery(ireader, type, pager));
				TopFieldCollector collector = TopFieldCollector.create(sort, DEFAULT_LIMIT, DEFAULT_LIMIT);
				isearcher.search(query, collector);
				topDocs = collector.topDocs(start, maxPerPage);
			}

			ScoreDoc[] hits = topDocs.scoreDocs;
			pager.setCount(topDocs.totalHits.value);

			Document[] docs = new Document[hits.length];
			for (int i = 0; i < hits.length; i++) {
				docs[i] = isearcher.doc(hits[i].doc);
			}
			if (hits.length > 0) {
				pager.setLastKey(docs[hits.length - 1].get(DOC_ID_FIELD_NAME));
			}
			logger.debug("Lucene query: {} Hits: {}, Total: {}", query, hits.length, topDocs.totalHits);
			return docs;
		} catch (Exception e) {
			Throwable cause = e.getCause();
			String msg = cause != null ? cause.getMessage() : e.getMessage();
			logger.warn("No search results for type '{}' in app '{}': {}.", type, appid, msg);
		}
		return new Document[0];
	}

	private static Integer getLastDocId(IndexSearcher isearcher, String lastKey) throws IOException {
		Query lastDoc = new TermQuery(new Term(DOC_ID_FIELD_NAME, lastKey));
		TopDocs docs = isearcher.search(lastDoc, 1);
		ScoreDoc[] hits = docs.scoreDocs;
		if (hits.length > 0) {
			return hits[0].doc;
		}
		return null;
	}

	private static SortField getSortFieldForQuery(DirectoryReader ireader, String type, Pager pager) {
		if (DOC_ID_FIELD_NAME.equals(pager.getSortby())) {
			return new SortedNumericSortField(DOC_ID_FIELD_NAME, LONG, pager.isDesc());
		} else {
			FieldInfo finfo = FieldInfos.getMergedFieldInfos(ireader).fieldInfo(pager.getSortby());
			if (finfo != null) {
				switch (finfo.getDocValuesType()) {
					case SORTED_NUMERIC:
						return new SortedNumericSortField(pager.getSortby(), DOUBLE, pager.isDesc());
					default:
						return new SortField(pager.getSortby(), STRING, pager.isDesc());
				}
			} else {
				return new SortField(pager.getSortby(), STRING, pager.isDesc());
			}
		}
	}

	/**
	 * Counts the total number of documents for a given query.
	 * @param appid appid
	 * @param query a query
	 * @return total docs found in index
	 */
	public static int count(String appid, Query query) {
		if (StringUtils.isBlank(appid) || query == null) {
			return 0;
		}
		DirectoryReader ireader = null;
		try {
			ireader = getIndexReader(appid);
			if (ireader != null) {
				IndexSearcher isearcher = new IndexSearcher(ireader);
				return isearcher.count(query);
			}
		} catch (Exception e) {
			logger.error(null, e);
		} finally {
			closeIndexReader(ireader);
		}
		return 0;
	}

	private static DirectoryReader getIndexReader(String appid) {
		try {
			Directory indexDir = getDirectory(appid);
			if (DirectoryReader.indexExists(indexDir)) {
				return DirectoryReader.open(indexDir);
			}
		} catch (IOException ex) {
			logger.warn("Couldn't get IndexReader - '{}' does not exist: {}", getIndexName(appid), ex.getMessage());
		}
		return null;
	}

	private static IndexWriter getIndexWriter(String appid) {
		synchronized (WRITERS) {
			if (!WRITERS.containsKey(appid)) {
				try {
					Analyzer analyzer = new StandardAnalyzer();
					IndexWriterConfig config = new IndexWriterConfig(analyzer);
					config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
					WRITERS.put(appid, new IndexWriter(getDirectory(appid), config));
				} catch (IOException ex) {
					logger.warn("Couldn't get IndexWriter - '{}' does not exist: {}", getIndexName(appid), ex.getMessage());
				}
				Para.addDestroyListener(new DestroyListener() {
					public void onDestroy() {
						closeIndexWriters();
					}
				});
			}
		}
		return WRITERS.get(appid);
	}

	private static Directory getDirectory(String appid) throws IOException {
//		if (Config.getConfigParam("lucene.storage", "").equalsIgnoreCase("s3")) {
//			if (s3Directory == null) {
//				s3Directory = new S3Directory(Config.getConfigParam("lucene.s3_prefix", "") + getIndexName(appid));
//				s3Directory.create();
//			}
//			return s3Directory;
//		} else {
			return FSDirectory.open(FileSystems.getDefault().
					getPath(Config.getConfigParam("lucene.dir", Paths.get(".").toAbsolutePath().normalize().toString()),
							"data", getIndexName(appid)));
//		}
	}

	private static void closeIndexReader(DirectoryReader ireader) {
		try {
			if (ireader != null) {
				ireader.close();
				if (ireader.directory() != null) {
					ireader.directory().close();
				}
			}
		} catch (Exception e) {
			logger.error(null, e);
		}
	}

	private static void closeIndexWriters() {
		try {
			for (IndexWriter indexWriter : WRITERS.values()) {
				if (indexWriter != null) {
					if (indexWriter.hasUncommittedChanges()) {
						indexWriter.commit();
					}
					if (indexWriter.isOpen()) {
						indexWriter.close();
					}
				}
			}
			WRITERS.clear();
		} catch (Exception e) {
			logger.error(null, e);
		}
	}

	/**
	 * Creates a term filter for a set of terms.
	 * @param terms some terms
	 * @param mustMatchAll if true all terms must match ('AND' operation)
	 * @return the filter
	 */
	static Query getTermsQuery(Map<String, ?> terms, boolean mustMatchAll) {
		BooleanQuery.Builder fb = new BooleanQuery.Builder();
		int addedTerms = 0;
		boolean noop = true;
		Query bfb = null;

		for (Map.Entry<String, ?> term : terms.entrySet()) {
			Object val = term.getValue();
			if (!StringUtils.isBlank(term.getKey()) && val != null) {
				Matcher matcher = Pattern.compile(".*(<|>|<=|>=)$").matcher(term.getKey().trim());
				bfb = new TermQuery(new Term(term.getKey(), val.toString()));
				if (matcher.matches() && val instanceof Number) {
					String key = term.getKey().replaceAll("[<>=\\s]+$", "");

					if (">".equals(matcher.group(1))) {
						bfb = TermRangeQuery.newStringRange(key, val.toString(), null, false, false);
					} else if ("<".equals(matcher.group(1))) {
						bfb = TermRangeQuery.newStringRange(key, null, val.toString(), false, false);
					} else if (">=".equals(matcher.group(1))) {
						bfb = TermRangeQuery.newStringRange(key, val.toString(), null, true, false);
					} else if ("<=".equals(matcher.group(1))) {
						bfb = TermRangeQuery.newStringRange(key, null, val.toString(), false, true);
					}
				}
				if (mustMatchAll) {
					fb.add(bfb, BooleanClause.Occur.MUST);
				} else {
					fb.add(bfb, BooleanClause.Occur.SHOULD);
				}
				addedTerms++;
				noop = false;
			}
		}
		if (addedTerms == 1 && bfb != null) {
			return bfb;
		}
		return noop ? null : fb.build();
	}

	/**
	 * Tries to parse a query string in order to check if it is valid.
	 * @param query a Lucene query string
	 * @return the query if valid, or '*' if invalid
	 */
	static Query qs(String query, Collection<String> fields) {
		if (fields == null || fields.isEmpty()) {
			fields = Collections.singletonList(Config._NAME);
		}
		if (StringUtils.isBlank(query)) {
			query = "*";
		}
		MultiFieldQueryParser parser = new MultiFieldQueryParser(fields.toArray(new String[0]), QUERY_STRING_ANALYZER);
		parser.setAllowLeadingWildcard(false);
		//parser.setLowercaseExpandedTerms(false); // DEPRECATED in Lucene 7.x
		query = query.trim();
		if (query.length() > 1 && query.startsWith("*")) {
			query = query.substring(1);
		}
		if (!StringUtils.isBlank(query) && !"*".equals(query)) {
			try {
				Query q = parser.parse(query);
				return q;
			} catch (Exception ex) {
				logger.warn("Failed to parse query string '{}'.", query);
			}
		}
		return new MatchAllDocsQuery();
	}

	static boolean isValidQueryString(String query) {
		if (StringUtils.isBlank(query)) {
			return false;
		}
		if ("*".equals(query.trim())) {
			return true;
		}
		try {
			StandardQueryParser parser = new StandardQueryParser();
			parser.setAllowLeadingWildcard(false);
			parser.parse(query, "");
			return true;
		} catch (QueryNodeException ex) {
			return false;
		}
	}

	static Pager getPager(Pager[] pager) {
		return (pager != null && pager.length > 0) ? pager[0] : new Pager();
	}

	/**
	 * A method reserved for future use. It allows to have indexes with different names than the appid.
	 *
	 * @param appid an app identifer
	 * @return the correct index name
	 */
	static String getIndexName(String appid) {
		return appid + "-lucene";
	}

}
