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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks one indexing call and warns if it runs for more than 30 seconds.
 */
final class SlowIndexOperation implements AutoCloseable {

	private static final Logger logger = LoggerFactory.getLogger(SlowIndexOperation.class);
	private static final long WARNING_AFTER_MILLIS = 30_000L;
	private static final int MAX_LOGGED_OBJECT_IDS = 10;
	private static final int MAX_DOCUMENTS_FOR_FIELD_STATS = 10;
	private static final int MAX_UNIQUE_FIELDS_PER_DOCUMENT = 10_000;
	private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
	private static final ThreadLocal<SlowIndexOperation> ACTIVE = new ThreadLocal<>();
	private static final ScheduledThreadPoolExecutor WATCHDOG = createWatchdog();

	private final String appid;
	private final String operation;
	private final List<String> objectIds;
	private final int objectCount;
	private final long startedNanos;
	private final long startedCpuNanos;
	private final long threadId;
	private final long warningAfterNanos;
	private final Consumer<String> warningSink;
	private final SlowIndexOperation previous;
	private final AtomicBoolean closed = new AtomicBoolean();
	private final AtomicBoolean warningLogged = new AtomicBoolean();
	private final Map<String, LongAdder> stageNanos = new ConcurrentHashMap<>();
	private final List<Document> documents = Collections.synchronizedList(new ArrayList<>());
	private int documentCount;
	private long fieldInstances;
	private boolean fieldStatsSampled;
	private final ScheduledFuture<?> warningTask;
	private volatile String currentStage = "starting";
	private volatile long currentStageStartedNanos;

	private SlowIndexOperation(String appid, String operation, List<String> objectIds, int objectCount,
			long warningAfterMillis, Consumer<String> warningSink) {
		if (warningAfterMillis <= 0) {
			throw new IllegalArgumentException("warningAfterMillis must be positive.");
		}
		this.appid = sanitize(appid);
		this.operation = operation;
		this.objectIds = limitObjectIds(objectIds);
		this.objectCount = objectCount;
		this.startedNanos = System.nanoTime();
		this.threadId = Thread.currentThread().threadId();
		this.startedCpuNanos = threadCpuTime(threadId);
		this.warningAfterNanos = TimeUnit.MILLISECONDS.toNanos(warningAfterMillis);
		this.warningSink = warningSink;
		this.previous = ACTIVE.get();
		this.currentStageStartedNanos = startedNanos;
		ACTIVE.set(this);
		this.warningTask = WATCHDOG.schedule(this::warnWhileRunning, warningAfterMillis, TimeUnit.MILLISECONDS);
	}

	static SlowIndexOperation start(String appid, String operation, List<String> objectIds, int objectCount) {
		return start(appid, operation, objectIds, objectCount, WARNING_AFTER_MILLIS,
				message -> logger.warn("{}", message));
	}

	static SlowIndexOperation start(String appid, String operation, List<String> objectIds, int objectCount,
			long warningAfterMillis, Consumer<String> warningSink) {
		return new SlowIndexOperation(appid, operation, objectIds, objectCount, warningAfterMillis, warningSink);
	}

	static SlowIndexOperation current() {
		return ACTIVE.get();
	}

	<T> T measure(String stage, Supplier<T> action) {
		String previousStage = currentStage;
		long start = System.nanoTime();
		currentStage = stage;
		currentStageStartedNanos = start;
		try {
			return action.get();
		} finally {
			stageNanos.computeIfAbsent(stage, ignored -> new LongAdder()).add(System.nanoTime() - start);
			currentStage = previousStage;
			currentStageStartedNanos = System.nanoTime();
		}
	}

	<T> T measureIO(String stage, IOSupplier<T> action) throws IOException {
		String previousStage = currentStage;
		long start = System.nanoTime();
		currentStage = stage;
		currentStageStartedNanos = start;
		try {
			return action.get();
		} finally {
			stageNanos.computeIfAbsent(stage, ignored -> new LongAdder()).add(System.nanoTime() - start);
			currentStage = previousStage;
			currentStageStartedNanos = System.nanoTime();
		}
	}

	void recordDocuments(List<Document> docs) {
		synchronized (documents) {
			documentCount += docs.size();
			for (Document doc : docs) {
				fieldInstances += doc.getFields().size();
				if (documents.size() < MAX_DOCUMENTS_FOR_FIELD_STATS) {
					documents.add(doc);
				} else {
					fieldStatsSampled = true;
				}
			}
		}
	}

	@Override
	public void close() {
		if (!closed.compareAndSet(false, true)) {
			return;
		}
		warningTask.cancel(false);
		if (ACTIVE.get() == this) {
			if (previous == null) {
				ACTIVE.remove();
			} else {
				ACTIVE.set(previous);
			}
		}
		if (System.nanoTime() - startedNanos >= warningAfterNanos) {
			warnOnce("completed");
		}
	}

	private void warnWhileRunning() {
		if (!closed.get()) {
			warnOnce("still-running");
		}
	}

	private void warnOnce(String status) {
		if (warningLogged.compareAndSet(false, true)) {
			warningSink.accept(formatWarning(status));
		}
	}

	private String formatWarning(String status) {
		long now = System.nanoTime();
		long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(now - startedNanos);
		long cpuNanos = threadCpuTime(threadId);
		long cpuMillis = startedCpuNanos < 0 || cpuNanos < 0 ? -1 :
				TimeUnit.NANOSECONDS.toMillis(cpuNanos - startedCpuNanos);
		long currentStageMillis = TimeUnit.NANOSECONDS.toMillis(now - currentStageStartedNanos);
		Map<String, Long> stages = new TreeMap<>();
		stageNanos.forEach((name, duration) -> stages.put(name,
				TimeUnit.NANOSECONDS.toMillis(duration.sum())));
		FieldStats fieldStats = getFieldStats();
		return "Slow Lucene indexing operation: appid='" + appid + "' objectIds=" + objectIds +
				" operation=" + operation + " status=" + status + " elapsedMs=" + elapsedMillis +
				" cpuMs=" + cpuMillis + " currentStage=" + currentStage +
				" currentStageMs=" + currentStageMillis + " stageTimesMs=" + stages +
				" objectCount=" + objectCount + " documentCount=" + fieldStats.documentCount +
				" maxFieldCount=" + fieldStats.maxFieldCount + " fieldCountTruncated=" +
				fieldStats.fieldCountTruncated + " fieldStatsSampled=" + fieldStats.fieldStatsSampled +
				" fieldInstances=" + fieldStats.fieldInstances;
	}

	private FieldStats getFieldStats() {
		List<Document> snapshot;
		int totalDocuments;
		long totalFieldInstances;
		boolean sampled;
		synchronized (documents) {
			snapshot = new ArrayList<>(documents);
			totalDocuments = documentCount;
			totalFieldInstances = fieldInstances;
			sampled = fieldStatsSampled;
		}
		if (snapshot.isEmpty()) {
			return new FieldStats(totalDocuments, -1, totalFieldInstances, false, sampled);
		}
		int maxFieldCount = 0;
		boolean truncated = false;
		for (Document doc : snapshot) {
			HashSet<String> names = new HashSet<>();
			List<IndexableField> fields = doc.getFields();
			for (IndexableField field : fields) {
				if (names.size() < MAX_UNIQUE_FIELDS_PER_DOCUMENT) {
					names.add(field.name());
				} else if (!names.contains(field.name())) {
					truncated = true;
					break;
				}
			}
			maxFieldCount = Math.max(maxFieldCount, names.size());
		}
		return new FieldStats(totalDocuments, maxFieldCount, totalFieldInstances, truncated, sampled);
	}

	private static List<String> limitObjectIds(List<String> objectIds) {
		if (objectIds == null || objectIds.isEmpty()) {
			return List.of();
		}
		ArrayList<String> limited = new ArrayList<>(Math.min(objectIds.size(), MAX_LOGGED_OBJECT_IDS));
		for (String id : objectIds) {
			if (id != null && limited.size() < MAX_LOGGED_OBJECT_IDS) {
				limited.add(sanitize(id));
			}
		}
		return limited;
	}

	private static String sanitize(String value) {
		return value == null ? "null" : value.replace("\r", "\\r").replace("\n", "\\n").replace("\t", "\\t");
	}

	private static long threadCpuTime(long id) {
		if (THREAD_MX_BEAN.isThreadCpuTimeSupported() && THREAD_MX_BEAN.isThreadCpuTimeEnabled()) {
			return THREAD_MX_BEAN.getThreadCpuTime(id);
		}
		return -1;
	}

	private static ScheduledThreadPoolExecutor createWatchdog() {
		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, task -> {
			Thread thread = new Thread(task, "para-lucene-slow-index-watchdog");
			thread.setDaemon(true);
			return thread;
		});
		executor.setRemoveOnCancelPolicy(true);
		return executor;
	}

	@FunctionalInterface
	interface IOSupplier<T> {
		T get() throws IOException;
	}

	private record FieldStats(int documentCount, int maxFieldCount, long fieldInstances,
			boolean fieldCountTruncated, boolean fieldStatsSampled) { }
}
