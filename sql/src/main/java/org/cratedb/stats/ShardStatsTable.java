package org.cratedb.stats;

import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.SQLGroupingCollector;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.SQLFetchCollector;
import org.cratedb.lucene.LuceneFieldMapper;
import org.cratedb.lucene.fields.BooleanLuceneField;
import org.cratedb.lucene.fields.IntegerLuceneField;
import org.cratedb.lucene.fields.LongLuceneField;
import org.cratedb.lucene.fields.StringLuceneField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.shard.service.InternalIndexShard;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardStatsTable implements StatsTable {

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
    private final InternalIndexShard shard;
    private final String indexName;
    private final String nodeId;

    private final MemoryIndex index;

    private Map<String, Object> fields;

    private final ESLogger logger = Loggers.getLogger(getClass());

    public ShardStatsTable(Map<String, AggFunction> aggFunctionMap,
                           String indexName,
                           InternalIndexShard shard,
                           String nodeId) throws Exception {
        this.aggFunctionMap = aggFunctionMap;
        this.indexName = indexName;
        this.shard = shard;
        this.nodeId = nodeId;
        this.index = new MemoryIndex();
        this.fields = new HashMap<>();
    }

    private void index(ParsedStatement stmt) throws IOException {

        // build map of result columns
        for (Tuple<String, String> columnNames : stmt.outputFields()) {
            if (fieldMapper().containsKey(columnNames.v2())) {
                fields.put(columnNames.v2(), getStat(columnNames.v2()));
            }
        }

        // build index with filtered fields
        for (String columnName : stmt.columnsWithFilter) {
            if (logger.isTraceEnabled()) {
                logger.trace("Indexing field {} with value: {}",
                        columnName, getStat(columnName));
            }
            index.addField(columnName,
                    fieldMapper.get(columnName).tokenStream(getStat(columnName)));
        }

    }

    public Map<String, Map<GroupByKey, GroupByRow>> queryGroupBy(String[] reducers,
                                                          ParsedStatement stmt)
            throws Exception
    {

        SQLGroupingCollector collector = new SQLGroupingCollector(
                stmt,
                new StatsTableFieldLookup(fields),
                aggFunctionMap,
                reducers
        );
        synchronized (index) {
            index(stmt);
            doQuery(stmt.query, collector);
        }

        return collector.partitionedResult;
    }

    public List<List<Object>> query(ParsedStatement stmt) throws Exception
    {
        SQLFetchCollector collector = new SQLFetchCollector(
                stmt,
                new StatsTableFieldLookup(fields)
        );
        synchronized (index) {
            index(stmt);
            doQuery(stmt.query, collector);
        }

        return collector.results;
    }

    public void doQuery(Query query, Collector collector) throws Exception
    {
        index.createSearcher().search(query, collector);
    }


    public static Iterable<String> cols() {
        return fieldMapper.keySet();
    }

    public static LuceneFieldMapper fieldMapper() {
        return fieldMapper;
    }

    private Object getStat(String columnName) {
        switch (columnName.toLowerCase()) {
            case Columns.TABLE_NAME:
                return indexName;
            case Columns.NODE_ID:
                return nodeId;
            case Columns.NODE_NAME:
                return shard.nodeName();
            case Columns.NUM_DOCS:
                return shard.docStats().getCount();
            case Columns.SHARD_ID:
                return shard.shardId().id();
            case Columns.SIZE:
                return shard.storeStats().getSizeInBytes();
            case Columns.STATE:
                return shard.state().toString();
            case Columns.PRIMARY:
                return new Boolean(shard.routingEntry().primary());
            case Columns.RELOCATING_NODE:
                return shard.routingEntry().relocatingNodeId();
            default:
                return null;
        }
    }
}
