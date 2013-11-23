package org.cratedb.action.groupby;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.core.collections.LimitingCollectionIterator;

import java.util.*;

public class GroupByHelper {

    public static Collection<GroupByRow> sortRows(List<GroupByRow> rows,
                                                  Comparator<GroupByRow> comparator,
                                                  int totalLimit) {
        Collections.sort(rows, comparator);
        return new LimitingCollectionIterator<GroupByRow>(rows, totalLimit);
    }

    public static Object[][] sortedRowsToObjectArray(Collection<GroupByRow> rows,
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
