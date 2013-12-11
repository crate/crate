package org.cratedb.stats;

import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.index.memory.ReusableMemoryIndex;
import org.apache.lucene.search.Collector;
import org.apache.lucene.util.BytesRef;
import org.cratedb.action.collect.CollectorContext;
import org.cratedb.action.groupby.GlobalSQLGroupingCollector;
import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.SQLGroupingCollector;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.key.Rows;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLFetchCollector;
import org.cratedb.lucene.LuceneFieldMapper;
import org.cratedb.lucene.fields.BooleanLuceneField;
import org.cratedb.lucene.fields.IntegerLuceneField;
import org.cratedb.lucene.fields.LongLuceneField;
import org.cratedb.lucene.fields.StringLuceneField;
import org.cratedb.lucene.index.memory.MemoryIndexPool;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.shard.service.InternalIndexShard;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardStatsTable implements StatsTable {

    private final CacheRecycler cacheRecycler;

    public class Columns {
        public static final String NODE_ID = "node_id";
        public static final String NODE_NAME = "node_name";
        public static final String NUM_DOCS = "num_docs";
        public static final String PRIMARY = "primary";
        public static final String RELOCATING_NODE = "relocating_node";
        public static final String SHARD_ID = "shard_id";
        public static final String SIZE = "size";
        public static final String STATE = "state";
        public static final String TABLE_NAME = "table_name";
    }

    private static LuceneFieldMapper fieldMapper = new LuceneFieldMapper(){{
        put(Columns.NODE_ID, new StringLuceneField(Columns.NODE_ID));
        put(Columns.NODE_NAME, new StringLuceneField(Columns.NODE_NAME));
        put(Columns.NUM_DOCS, new LongLuceneField(Columns.NUM_DOCS));
        put(Columns.PRIMARY, new BooleanLuceneField(Columns.PRIMARY));
        put(Columns.RELOCATING_NODE, new StringLuceneField(Columns.RELOCATING_NODE));
        put(Columns.SHARD_ID, new IntegerLuceneField(Columns.SHARD_ID));
        put(Columns.SIZE, new LongLuceneField(Columns.SIZE));
        put(Columns.STATE, new StringLuceneField(Columns.STATE));
        put(Columns.TABLE_NAME, new StringLuceneField(Columns.TABLE_NAME));
    }};

    private final Map<String, AggFunction> aggFunctionMap;
    private final MemoryIndexPool memoryIndexPool;
    private final ESLogger logger = Loggers.getLogger(getClass());

    static public class ShardInfo implements StatsInfo {
        private final InternalIndexShard shard;
        private final String indexName;
        private final String nodeId;
        private final int shardId;
        private final boolean unassigned;
        private final Map<String, Object> fields;

        public ShardInfo (String indexName,
                          String nodeId,
                          int shardId,
                          InternalIndexShard shard) {
            this.indexName = indexName;
            this.nodeId = nodeId;
            this.shardId = shardId;
            this.shard = shard;
            this.unassigned = shard == null ? true : false;
            this.fields = new HashMap();
        }

        public Map<String, Object> fields() {
            return fields;
        }

        public Object getStat(String columnName) {
            switch (columnName.toLowerCase()) {
                case Columns.TABLE_NAME:
                    return new BytesRef(indexName);
                case Columns.NODE_ID:
                    return !unassigned ? new BytesRef(nodeId) : null;
                case Columns.NODE_NAME:
                    return !unassigned ? new BytesRef(shard.nodeName()) : null;
                case Columns.NUM_DOCS:
                    return !unassigned ? shard.docStats().getCount() : 0;
                case Columns.SHARD_ID:
                    return shardId;
                case Columns.SIZE:
                    return !unassigned ? shard.storeStats().getSizeInBytes() : 0L;
                case Columns.STATE:
                    return !unassigned ? new BytesRef(shard.state().toString()) :
                            ShardRoutingState.UNASSIGNED;
                case Columns.PRIMARY:
                    return !unassigned ? new Boolean(shard.routingEntry().primary()) : false;
                case Columns.RELOCATING_NODE:
                    return !unassigned ? new BytesRef(shard.routingEntry().relocatingNodeId()) :
                            null;
                default:
                    return null;
            }
        }
    }


    @Inject
    public ShardStatsTable(Map<String, AggFunction> aggFunctionMap,
            CacheRecycler cacheRecycler) throws Exception {
        this.aggFunctionMap = aggFunctionMap;
        memoryIndexPool = new MemoryIndexPool();
        this.cacheRecycler = cacheRecycler;
    }

    private void index(ParsedStatement stmt,
                       MemoryIndex memoryIndex,
                       StatsInfo shardInfo) throws IOException {

        // build map of result columns
        for (Tuple<String, String> columnNames : stmt.outputFields()) {
            if (fieldMapper().containsKey(columnNames.v2())) {
                shardInfo.fields().put(columnNames.v2(), shardInfo.getStat(columnNames.v2()));
            }
        }
        // add all columns used for aggregates
        for (AggExpr aggExpr : stmt.aggregateExpressions()) {
            if (aggExpr.parameterInfo != null) {
                String columnName = aggExpr.parameterInfo.columnName;
                if (fieldMapper().containsKey(columnName)) {
                    shardInfo.fields().put(columnName, shardInfo.getStat(columnName));
                }
            }
        }

        // build index with filtered fields
        for (String columnName : stmt.columnsWithFilter) {
            if (logger.isTraceEnabled()) {
                logger.trace("Indexing field {} with value: {}",
                        columnName, shardInfo.getStat(columnName));
            }
            memoryIndex.addField(columnName,
                    fieldMapper.get(columnName).tokenStream(shardInfo.getStat(columnName)));
        }

    }

    public Rows queryGroupBy(int numReducers,
            ParsedStatement stmt,
            StatsInfo shardInfo)
            throws Exception
    {
        CollectorContext cc = new CollectorContext().fieldLookup(
                new StatsTableFieldLookup(shardInfo.fields()));
        cc.cacheRecycler(cacheRecycler);
        SQLGroupingCollector collector = new SQLGroupingCollector(
                stmt,
                cc,
                aggFunctionMap,
                numReducers
        );
        doQuery(stmt, shardInfo, collector);

        return collector.rows();
    }

    public List<List<Object>> query(ParsedStatement stmt, StatsInfo shardInfo) throws Exception
    {
        SQLFetchCollector collector = new SQLFetchCollector(
                stmt,
                new StatsTableFieldLookup(shardInfo.fields())
        );
        doQuery(stmt, shardInfo, collector);

        return collector.results;
    }

    private void doQuery(ParsedStatement stmt,
                         StatsInfo shardInfo,
                         Collector collector) throws Exception
    {
        final ReusableMemoryIndex memoryIndex = memoryIndexPool.acquire();

        index(stmt, memoryIndex, shardInfo);
        memoryIndex.createSearcher().search(stmt.query, collector);

        memoryIndexPool.release(memoryIndex);
    }


    public static Iterable<String> cols() {
        return fieldMapper.keySet();
    }

    public static LuceneFieldMapper fieldMapper() {
        return fieldMapper;
    }

}
