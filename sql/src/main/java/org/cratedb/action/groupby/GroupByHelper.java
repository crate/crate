package org.cratedb.action.groupby;

import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.min.MinAggFunction;
import org.cratedb.action.sql.ITableExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.core.collections.LimitingCollectionIterator;
import org.cratedb.sql.types.SQLFieldMapper;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class GroupByHelper {

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

    public static Object[][] sortedRowsToObjectArray(Collection<GroupByRow> rows,
                                                     ParsedStatement parsedStatement,
                                                     ITableExecutionContext tableExecutionContext) {
        int rowCount = Math.max(0, rows.size() - parsedStatement.offset());
        Object[][] result = new Object[rowCount][parsedStatement.outputFields().size()];
        int currentRow = -1;
        int remainingOffset = parsedStatement.offset();

        for (GroupByRow row : rows) {
            if (remainingOffset > 0) {
                remainingOffset--;
                continue;
            }

            currentRow++;
            if (tableExecutionContext != null) {
                SQLFieldMapper sqlFieldMapper = tableExecutionContext.mapper();
                for (int i = 0; i < result[currentRow].length; i++) {
                    Object value = null;
                    int idx = parsedStatement.idxMap[i];
                    AggExpr aggExpr = row.getAggExpr(idx);
                    if (aggExpr == null || aggExpr.parameterInfo.isAllColumn) {
                        value = row.get(idx);
                    } else if (aggExpr.functionName.equalsIgnoreCase(MinAggFunction.NAME)) {
                        value = sqlFieldMapper.convertToXContentValue(
                                row.getAggExpr(idx).parameterInfo.columnName,
                                row.get(idx)
                        );
                    }
                    result[currentRow][i] = value;
                }
            } else {
                // simple loop, no mapper available
                for (int i = 0; i < result[currentRow].length; i++) {
                    int idx = parsedStatement.idxMap[i];
                    result[currentRow][i] = row.get(idx);
                }
            }

        }

        return result;
    }
}
