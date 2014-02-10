/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

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
