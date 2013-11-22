package org.cratedb.action.groupby;

import com.google.common.collect.MinMaxPriorityQueue;
import org.cratedb.action.sql.ParsedStatement;

import java.util.Collection;
import java.util.Comparator;

public class GroupByHelper {

    public static Collection<GroupByRow> sortRows(Collection<GroupByRow> rows,
                                                  Comparator<GroupByRow> comparator,
                                                  Integer limit,
                                                  Integer offset) {

        MinMaxPriorityQueue.Builder<GroupByRow> rowBuilder = MinMaxPriorityQueue.orderedBy(comparator);

        if (limit != null) {
            int limitPlusOffset = limit;
            if (offset != null) {
                limitPlusOffset += offset;
            }
            rowBuilder.maximumSize(limitPlusOffset);
        }

        MinMaxPriorityQueue<GroupByRow> q = rowBuilder.create();
        q.addAll(rows);

        return q;
    }

    public static Object[][] sortedRowsToObjectArray(Collection<GroupByRow> rows,
                                                     ParsedStatement parsedStatement,
                                                     int offset) {
        Object[][] result = new Object[rows.size() - offset][parsedStatement.outputFields().size()];
        int currentRow = -1;
        int remainingOffset = offset;

        for (GroupByRow row : rows) {
            if (remainingOffset > 0) {
                remainingOffset--;
                continue;
            }

            currentRow++;
            for (int i = 0; i < result[currentRow].length; i++) {
                result[currentRow][i] = row.get(parsedStatement.idxMap[i]);
            }
        }

        return result;
    }
}
