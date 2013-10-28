package org.cratedb.action.groupby;

import com.google.common.base.Joiner;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.cratedb.action.GroupByFieldLookup;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.parser.ColumnDescription;
import org.cratedb.action.parser.ColumnReferenceDescription;
import org.cratedb.action.sql.ParsedStatement;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * Collector that can be used to get results from a Lucene query.
 *
 * The result is partitioned by the reducers and grouped by the group key(s)
 * See {@link org.cratedb.action.TransportDistributedSQLAction} for a full overview of the process.
 */
public class SQLGroupingCollector extends Collector {

    private final String[] reducers;
    private final GroupByFieldLookup groupByFieldLookup;
    private final GroupingKeyGenerator keyGenerator;
    private final ParsedStatement parsedStatement;
    private final Map<String, AggFunction> aggFunctionMap;

    /**
     * Partitioned and grouped results.
     *
     * Assuming two reducers and a query as follows:
     *
     *      select avg(age), race, count(*) ... from ... group by race
     *
     * The result is in the following format:
     *
     * partitionedResult = {
     *     "node1": {
     *         "hash Human": (GroupByRow)[AvgAggState, "Human", CountAggState],
     *         "hash Vogon": (GroupByRow)[AvgAggState, "Vogon", CountAggState]
     *     },
     *     "node2": {
     *         "hash Android": (GroupByRow)[AvgAggState, "Android", CountAggState]
     *     }
     * }
     */
    public Map<String, Map<Integer, GroupByRow>> partitionedResult = newHashMap();

    public SQLGroupingCollector(ParsedStatement parsedStatement,
                                GroupByFieldLookup groupByFieldLookup,
                                Map<String, AggFunction> aggFunctionMap,
                                String[] reducers) {
        this.parsedStatement = parsedStatement;
        this.groupByFieldLookup = groupByFieldLookup;
        this.reducers = reducers;
        this.aggFunctionMap = aggFunctionMap;

        assert parsedStatement.groupByColumnNames != null;
        keyGenerator = new GroupingKeyGenerator(parsedStatement.groupByColumnNames.size());

        for (String reducer : reducers) {
            partitionedResult.put(reducer, new HashMap<Integer, GroupByRow>());
        }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public void collect(int doc) throws IOException {
        groupByFieldLookup.setNextDocId(doc);
        keyGenerator.reset();
        for (String groupByColumn : parsedStatement.groupByColumnNames) {
            Object groupByColValue = groupByFieldLookup.lookupField(groupByColumn);
            keyGenerator.add(groupByColumn, groupByColValue);

        }

        int key = keyGenerator.getKey();
        String reducer = partitionByKey(reducers, key);
        Map<Integer, GroupByRow> resultMap = partitionedResult.get(reducer);

        GroupByRow row = resultMap.get(key);
        if (row == null) {
            row = GroupByRow.createEmptyRow(parsedStatement.resultColumnList, aggFunctionMap);
        }

        int columnIdx = -1;
        for (ColumnDescription column : parsedStatement.resultColumnList) {
            columnIdx++;
            switch (column.type) {
                case ColumnDescription.Types.AGGREGATE_COLUMN:
                    Object value = null;
                    AggExpr aggExpr = (AggExpr)column;
                    if (!aggExpr.parameterInfo.isAllColumn) {
                        throw new UnsupportedOperationException("select aggFunc(column) not supported!");
                    }

                    AggFunction function = aggFunctionMap.get(aggExpr.functionName);
                    function.iterate(row.aggregateStates.get(columnIdx), value);
                    break;

                case ColumnDescription.Types.CONSTANT_COLUMN:
                    row.regularColumns.put(
                        columnIdx,
                        keyGenerator.getValue(((ColumnReferenceDescription)column).name)
                    );
                    break;
            }
        }

        resultMap.put(key, row);
    }

    private String partitionByKey(String[] reducers, int key) {
        return reducers[Math.abs(key) % reducers.length];
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        groupByFieldLookup.setNextReader(context);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    private class GroupingKeyGenerator {

        Map<String, Object> keyMap;

        public GroupingKeyGenerator(Integer size) {
            keyMap = new HashMap<>(size);
        }

        public void add(String columnName, Object value) {
            keyMap.put(columnName, value);
        }

        public Object getValue(String columnName) {
            return keyMap.get(columnName);
        }

        public void reset() {
            keyMap.clear();
        }

        public int getKey() {
            int result = 1;
            for (Object o : keyMap.values()) {
                result = result * 31 + (o != null ? o.hashCode() : 0);
            }

            return result;
        }
    }
}
