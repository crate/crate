/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.Task;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public final class SearchSlowLog implements SearchOperationListener {
    private long queryWarnThreshold;
    private long queryInfoThreshold;
    private long queryDebugThreshold;
    private long queryTraceThreshold;

    private long fetchWarnThreshold;
    private long fetchInfoThreshold;
    private long fetchDebugThreshold;
    private long fetchTraceThreshold;

    private SlowLogLevel level;

    private final Logger queryLogger;
    private final Logger fetchLogger;

    private static final String INDEX_SEARCH_SLOWLOG_PREFIX = "index.search.slowlog";
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING =
        Setting.timeSetting(INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.query.warn", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING =
        Setting.timeSetting(INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.query.info", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING =
        Setting.timeSetting(INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.query.debug", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING =
        Setting.timeSetting(INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.query.trace", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING =
        Setting.timeSetting(INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.warn", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING =
        Setting.timeSetting(INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.info", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING =
        Setting.timeSetting(INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.debug", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING =
        Setting.timeSetting(INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.trace", TimeValue.timeValueNanos(-1),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<SlowLogLevel> INDEX_SEARCH_SLOWLOG_LEVEL =
        new Setting<>(INDEX_SEARCH_SLOWLOG_PREFIX + ".level", SlowLogLevel.TRACE.name(), SlowLogLevel::parse, Property.Dynamic,
            Property.IndexScope);

    private static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));

    public SearchSlowLog(IndexSettings indexSettings) {

        this.queryLogger = LogManager.getLogger(INDEX_SEARCH_SLOWLOG_PREFIX + ".query");
        this.fetchLogger = LogManager.getLogger(INDEX_SEARCH_SLOWLOG_PREFIX + ".fetch");

        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING, this::setQueryWarnThreshold);
        this.queryWarnThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING).nanos();
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING, this::setQueryInfoThreshold);
        this.queryInfoThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING).nanos();
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING, this::setQueryDebugThreshold);
        this.queryDebugThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING).nanos();
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING, this::setQueryTraceThreshold);
        this.queryTraceThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING).nanos();

        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING, this::setFetchWarnThreshold);
        this.fetchWarnThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING).nanos();
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING, this::setFetchInfoThreshold);
        this.fetchInfoThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING).nanos();
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING, this::setFetchDebugThreshold);
        this.fetchDebugThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING).nanos();
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING, this::setFetchTraceThreshold);
        this.fetchTraceThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING).nanos();

        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_LEVEL, this::setLevel);
        setLevel(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_LEVEL));
    }

    private void setLevel(SlowLogLevel level) {
        this.level = level;
        Loggers.setLevel(queryLogger, level.name());
        Loggers.setLevel(fetchLogger, level.name());
    }
    @Override
    public void onQueryPhase(SearchContext context, long tookInNanos) {
        if (queryWarnThreshold >= 0 && tookInNanos > queryWarnThreshold) {
            queryLogger.warn("{}", new SlowLogSearchContextPrinter(context, tookInNanos));
        } else if (queryInfoThreshold >= 0 && tookInNanos > queryInfoThreshold) {
            queryLogger.info("{}", new SlowLogSearchContextPrinter(context, tookInNanos));
        } else if (queryDebugThreshold >= 0 && tookInNanos > queryDebugThreshold) {
            queryLogger.debug("{}", new SlowLogSearchContextPrinter(context, tookInNanos));
        } else if (queryTraceThreshold >= 0 && tookInNanos > queryTraceThreshold) {
            queryLogger.trace("{}", new SlowLogSearchContextPrinter(context, tookInNanos));
        }
    }

    @Override
    public void onFetchPhase(SearchContext context, long tookInNanos) {
        if (fetchWarnThreshold >= 0 && tookInNanos > fetchWarnThreshold) {
            fetchLogger.warn("{}", new SlowLogSearchContextPrinter(context, tookInNanos));
        } else if (fetchInfoThreshold >= 0 && tookInNanos > fetchInfoThreshold) {
            fetchLogger.info("{}", new SlowLogSearchContextPrinter(context, tookInNanos));
        } else if (fetchDebugThreshold >= 0 && tookInNanos > fetchDebugThreshold) {
            fetchLogger.debug("{}", new SlowLogSearchContextPrinter(context, tookInNanos));
        } else if (fetchTraceThreshold >= 0 && tookInNanos > fetchTraceThreshold) {
            fetchLogger.trace("{}", new SlowLogSearchContextPrinter(context, tookInNanos));
        }
    }

    static final class SlowLogSearchContextPrinter {
        private final SearchContext context;
        private final long tookInNanos;

        SlowLogSearchContextPrinter(SearchContext context, long tookInNanos) {
            this.context = context;
            this.tookInNanos = tookInNanos;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(context.indexShard().shardId())
                    .append(" ")
                    .append("took[").append(TimeValue.timeValueNanos(tookInNanos)).append("], ")
                    .append("took_millis[").append(TimeUnit.NANOSECONDS.toMillis(tookInNanos)).append("], ")
                    .append("total_hits[").append(context.queryResult().getTotalHits()).append("], ");
            if (context.getQueryShardContext().getTypes() == null) {
                sb.append("types[], ");
            } else {
                sb.append("types[");
                Strings.arrayToDelimitedString(context.getQueryShardContext().getTypes(), ",", sb);
                sb.append("], ");
            }
            if (context.groupStats() == null) {
                sb.append("stats[], ");
            } else {
                sb.append("stats[");
                Strings.collectionToDelimitedString(context.groupStats(), ",", "", "", sb);
                sb.append("], ");
            }
            sb.append("search_type[").append(context.searchType()).append("], total_shards[").append(context.numberOfShards()).append("], ");
            if (context.request().source() != null) {
                sb.append("source[").append(context.request().source().toString(FORMAT_PARAMS)).append("], ");
            } else {
                sb.append("source[], ");
            }
            if (context.getTask().getHeader(Task.X_OPAQUE_ID) != null) {
                sb.append("id[").append(context.getTask().getHeader(Task.X_OPAQUE_ID)).append("], ");
            } else {
                sb.append("id[], ");
            }
            return sb.toString();
        }
    }

    private void setQueryWarnThreshold(TimeValue warnThreshold) {
        this.queryWarnThreshold = warnThreshold.nanos();
    }

    private void setQueryInfoThreshold(TimeValue infoThreshold) {
        this.queryInfoThreshold = infoThreshold.nanos();
    }

    private void setQueryDebugThreshold(TimeValue debugThreshold) {
        this.queryDebugThreshold = debugThreshold.nanos();
    }

    private void setQueryTraceThreshold(TimeValue traceThreshold) {
        this.queryTraceThreshold = traceThreshold.nanos();
    }

    private void setFetchWarnThreshold(TimeValue warnThreshold) {
        this.fetchWarnThreshold = warnThreshold.nanos();
    }

    private void setFetchInfoThreshold(TimeValue infoThreshold) {
        this.fetchInfoThreshold = infoThreshold.nanos();
    }

    private void setFetchDebugThreshold(TimeValue debugThreshold) {
        this.fetchDebugThreshold = debugThreshold.nanos();
    }

    private void setFetchTraceThreshold(TimeValue traceThreshold) {
        this.fetchTraceThreshold = traceThreshold.nanos();
    }

    long getQueryWarnThreshold() {
        return queryWarnThreshold;
    }

    long getQueryInfoThreshold() {
        return queryInfoThreshold;
    }

    long getQueryDebugThreshold() {
        return queryDebugThreshold;
    }

    long getQueryTraceThreshold() {
        return queryTraceThreshold;
    }

    long getFetchWarnThreshold() {
        return fetchWarnThreshold;
    }

    long getFetchInfoThreshold() {
        return fetchInfoThreshold;
    }

    long getFetchDebugThreshold() {
        return fetchDebugThreshold;
    }

    long getFetchTraceThreshold() {
        return fetchTraceThreshold;
    }

    SlowLogLevel getLevel() {
        return level;
    }
}
