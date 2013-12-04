package org.cratedb.action.groupby;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.cratedb.action.FieldLookup;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.sql.ParsedStatement;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Maps.newHashMap;

/**
 * Collector that can be used to get results from a Lucene query.
 *
 * The result is partitioned by the reducers and grouped by the group key(s)
 * See {@link org.cratedb.action.TransportDistributedSQLAction} for a full overview of the process.
 */
public class SQLGroupingCollector extends Collector {

    protected final String[] reducers;
    private final FieldLookup fieldLookup;
    private final ParsedStatement parsedStatement;
    private final AggFunction[] aggFunctions;
    private SQLGroupingAggregateHandler aggregateHandler;
    private RowSerializationContext rowSerializationContext;

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
     *         GroupByKey {"Human"}: (GroupByRow)[AvgAggState, CountAggState],
     *         GroupByKey {"Vogon"}: (GroupByRow)[AvgAggState, CountAggState]
     *     },
     *     "node2": {
     *         GroupByKey {"Android"}: (GroupByRow)[AvgAggState, CountAggState]
     *     }
     * }
     */
    public Map<String, Map<GroupByKey, GroupByRow>> partitionedResult = newHashMap();

    public SQLGroupingCollector(ParsedStatement parsedStatement,
                                FieldLookup fieldLookup,
                                Map<String, AggFunction> aggFunctionMap,
                                String[] reducers) {
        this.parsedStatement = parsedStatement;
        this.fieldLookup = fieldLookup;
        this.reducers = reducers;

        for (String reducer : reducers) {
            partitionedResult.put(reducer, new HashMap<GroupByKey, GroupByRow>());
        }

        aggFunctions = new AggFunction[parsedStatement.aggregateExpressions.size()];
        for (int i = 0; i < aggFunctions.length; i++) {
            aggFunctions[i] = aggFunctionMap.get(
                parsedStatement.aggregateExpressions.get(i).functionName);
        }

        if (parsedStatement.hasStoppableAggregate) {
            aggregateHandler = new CheckingSQLGroupingAggregateHandler();
        } else {
            aggregateHandler = new SimpleSQLGroupingAggregateHandler();
        }

        rowSerializationContext = new RowSerializationContext(parsedStatement.aggregateExpressions);
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    protected GroupByKey getGroupByKey() throws IOException {
        Object[] keyValue = new Object[parsedStatement.groupByColumnNames.size()];
        for (int i = 0; i < parsedStatement.groupByColumnNames.size(); i++) {
            keyValue[i] = fieldLookup.lookupField(parsedStatement.groupByColumnNames.get(i));
        }
        return new GroupByKey(keyValue);
    }

    @Override
    public void collect(int doc) throws IOException {
        fieldLookup.setNextDocId(doc);
        GroupByKey key = getGroupByKey();

        String reducer = partitionByKey(reducers, key);
        Map<GroupByKey, GroupByRow> resultMap = partitionedResult.get(reducer);

        GroupByRow row = resultMap.get(key);

        if (row == null) {
            row = GroupByRow.createEmptyRow(key, rowSerializationContext);
            resultMap.put(key, row);
        }
        aggregateHandler.handleAggregates(row, fieldLookup,
                parsedStatement.aggregateExpressions(), aggFunctions);
    }

    protected String partitionByKey(String[] reducers, GroupByKey key) {
        return reducers[Math.abs(key.hashCode()) % reducers.length];
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        fieldLookup.setNextReader(context);
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }
}
