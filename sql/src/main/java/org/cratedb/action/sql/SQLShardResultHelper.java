package org.cratedb.action.sql;


import org.apache.lucene.util.BytesRef;

import java.util.Collection;
import java.util.List;

public class SQLShardResultHelper {


    public static Object[][] sortedRowsToObjectArray(Collection<List> rows,
                                                     ParsedStatement parsedStatement) {
        int rowCount = Math.max(0, rows.size() - parsedStatement.offset());
        Object[][] result = new Object[rowCount][parsedStatement.outputFields().size()];
        int currentRow = -1;
        int remainingOffset = parsedStatement.offset();

        for (List row : rows) {
            if (remainingOffset > 0) {
                remainingOffset--;
                continue;
            }
            currentRow++;
            for (int i = 0; i < result[currentRow].length; i++) {
                Object value = row.get(i);
                if (value instanceof BytesRef) {
                    result[currentRow][i] = ((BytesRef)value).utf8ToString();
                } else {
                    result[currentRow][i] = value;
                }
            }
        }

        return result;
    }



}
