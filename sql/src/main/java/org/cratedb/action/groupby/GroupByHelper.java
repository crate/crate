package org.cratedb.action.groupby;

import org.cratedb.action.groupby.aggregate.min.MinAggFunction;
import org.cratedb.action.sql.ITableExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.core.collections.LimitingCollectionIterator;

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
            for (int i = 0; i < result[currentRow].length; i++) {
                result[currentRow][i] = row.get(parsedStatement.idxMap[i]);

                Object value = null;
                int idx = parsedStatement.idxMap[i];

                if (idx < row.key.size() || row.aggExprs.get(idx-row.key.size()).parameterInfo.isAllColumn) {
                    value = row.get(idx);
                } else if (row.aggExprs.get(idx-row.key.size()).functionName.equalsIgnoreCase(MinAggFunction.NAME) &&
                        tableExecutionContext != null) {
                    value = tableExecutionContext.mapper().convertToXContentValue(
                            row.aggExprs.get(idx-row.key.size()).parameterInfo.columnName,
                            row.get(idx)
                    );
                }
                result[currentRow][i] = value;
            }
        }

        return result;
    }
}
