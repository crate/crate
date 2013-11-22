package org.cratedb.action.groupby;

import com.google.common.collect.MinMaxPriorityQueue;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.core.collections.SortedPriorityQueueIterator;
import org.cratedb.service.SQLParseService;

import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;

public class GroupByHelper {

    public static SortedPriorityQueueIterator<GroupByRow> sortRows(Collection<GroupByRow> rows,
                                                                   Comparator<GroupByRow> comparator,
                                                                   int totalLimit) {
        PriorityQueue<GroupByRow> q = new PriorityQueue<>(totalLimit, comparator);
        q.addAll(rows);
        return new SortedPriorityQueueIterator<>(q, totalLimit);
    }

    public static Object[][] sortedRowsToObjectArray(SortedPriorityQueueIterator<GroupByRow> rows,
                                                     ParsedStatement parsedStatement) {
        Object[][] result = new Object[rows.size() - parsedStatement.offset()][parsedStatement.outputFields().size()];
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
            }
        }

        return result;
    }
}
