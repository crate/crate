package org.cratedb.action.groupby;

import com.google.common.collect.ImmutableSet;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.any.AnyAggFunction;
import org.cratedb.action.groupby.aggregate.max.MaxAggFunction;
import org.cratedb.action.groupby.aggregate.min.MinAggFunction;
import org.cratedb.action.parser.ColumnDescription;
import org.cratedb.action.parser.ColumnReferenceDescription;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.core.collections.LimitingCollectionIterator;
import org.cratedb.sql.types.SQLFieldMapper;

import java.util.*;

public class GroupByHelper {

    /**
     * Aggregation functions whose values need to be mapped before returning the GroupByResult
     */
    public static final ImmutableSet<String> MAPPED_AGG_FUNCTIONS = ImmutableSet.of(
            MinAggFunction.NAME,
            MaxAggFunction.NAME,
            AnyAggFunction.NAME
    );

    public static List<Integer> getSeenIdxMap(Collection<AggExpr> aggregateExpressions) {
        List<Integer> idxMap = new ArrayList<>();
        Set<String> distinctColumns = new HashSet<>();
        int seenIdx = 0;
        for (AggExpr expr : aggregateExpressions) {
            if (expr.isDistinct) {
                if (!distinctColumns.contains(expr.parameterInfo.columnName)) {
                    distinctColumns.add(expr.parameterInfo.columnName);
                    idxMap.add(seenIdx);
                    seenIdx++;
                }
            }
        }

        return idxMap;
    }

    public static Collection<GroupByRow> trimRows(List<GroupByRow> rows,
                                                  Comparator<GroupByRow> comparator,
                                                  int totalLimit) {
        // sorting/trim is only called if if something CAN be trimmed.
        // Otherwise the sorting would just be overhead because the Handler node will sort everything anyway.
        if (rows.size() > totalLimit) {
            return sortAndTrimRows(rows, comparator, totalLimit);
        }
        return rows;
    }

    public static Collection<GroupByRow> sortAndTrimRows(List<GroupByRow> rows,
                                                         Comparator<GroupByRow> comparator,
                                                         int totalLimit) {
        Collections.sort(rows, comparator);
        return new LimitingCollectionIterator<>(rows, totalLimit);
    }

    /**
     * this method can be used to built a GroupByFieldExtractor array which provides fast value
     * lookup on GroupByRows.
     *
     * The Array is in the same order as the parsedStatements resultColumnList
     *
     * E.g.
     *
     *  GroupByFieldExtractor[1].getValue(groupByRow) will return the value for the second column in
     *  the resultColumnList.
     *
     * see also {@link GroupByFieldExtractor}
     */
    public static GroupByFieldExtractor[] buildFieldExtractor(ParsedStatement parsedStatement,
                                                              final SQLFieldMapper fieldMapper) {
        GroupByFieldExtractor[] extractors = new GroupByFieldExtractor[parsedStatement.resultColumnList.size()];

        int colIdx = 0;
        int aggStateIdx = 0;
        int keyValIdx;

        for (final ColumnDescription columnDescription : parsedStatement.resultColumnList) {
            if (columnDescription instanceof AggExpr) {
                // fieldMapper is null in case of group by on information schema
                if (fieldMapper != null && MAPPED_AGG_FUNCTIONS.contains(((AggExpr) columnDescription).functionName)) {
                    // need to use fieldMapper to convert long to int/short, etc..
                    // groupingCollector/fieldcache doesn't return the correct types.
                    extractors[colIdx] = new GroupByFieldExtractor(aggStateIdx) {
                        @Override
                        public Object getValue(GroupByRow row) {
                            return fieldMapper.convertToXContentValue(
                                ((AggExpr) columnDescription).parameterInfo.columnName,
                                row.aggStates.get(idx).value()
                            );
                        }
                    };
                } else {

                    extractors[colIdx] = new GroupByFieldExtractor(aggStateIdx) {
                        @Override
                        public Object getValue(GroupByRow row) {
                            return row.aggStates.get(idx).value();
                        }
                    };
                }
                aggStateIdx++;
            } else {
                 // currently only AggExpr and ColumnReferenceDescription exists, so this must be true.
                assert columnDescription instanceof ColumnReferenceDescription;
                keyValIdx = parsedStatement.groupByColumnNames.indexOf(((ColumnReferenceDescription) columnDescription).name);
                extractors[colIdx] = new GroupByFieldExtractor(keyValIdx) {
                    @Override
                    public Object getValue(GroupByRow row) {
                        return row.key.get(idx);
                    }
                };
            }
            colIdx++;
        }

        return extractors;
    }

    public static Object[][] sortedRowsToObjectArray(Collection<GroupByRow> rows,
                                                     ParsedStatement parsedStatement,
                                                     GroupByFieldExtractor[] fieldExtractors) {
        int rowCount = Math.max(0, rows.size() - parsedStatement.offset());
        Object[][] result = new Object[parsedStatement.isGlobalAggregate() ? 1 : rowCount][parsedStatement.outputFields().size()];
        int currentRow = -1;
        int remainingOffset = parsedStatement.offset();
        if (parsedStatement.isGlobalAggregate()) {
            if (rowCount == 0) {
                // fill with initial (mostly NULL) values
                for (int i=0;i<result[0].length;i++) {
                    result[0][i] = parsedStatement.aggregateExpressions().get(i).createAggState().value();
                }
                return result;
            } else {
                // final merge
                Iterator<GroupByRow> iter = rows.iterator();
                final GroupByRow resultRow = iter.next();
                while (iter.hasNext()) {
                    resultRow.merge(iter.next());
                }
                rows = new ArrayList<GroupByRow>(1) {{ add(resultRow); }};
            }
        }

        for (GroupByRow row : rows) {
            if (remainingOffset > 0) {
                remainingOffset--;
                continue;
            }
            currentRow++;
            for (int i = 0; i < result[currentRow].length; i++) {
                result[currentRow][i] = fieldExtractors[i].getValue(row);
            }
        }

        return result;
    }
}
