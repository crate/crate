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

package io.crate.executor;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.apache.lucene.util.BytesRef;
import org.cratedb.action.sql.SQLResponse;

public class RowsResponseBuilder implements ResponseBuilder {

    /**
     * TODO: let the client set this flag through SQLRequest and Planner
     */
    private final boolean convertBytesRefs;

    public RowsResponseBuilder(boolean convertBytesRefs) {
        this.convertBytesRefs = convertBytesRefs;
    }

    @Override
    public SQLResponse buildResponse(String[] outputNames, Object[][] rows, long requestStartedTime) {
        if (convertBytesRefs) {
            convertBytesRef(rows);
        }
        return new SQLResponse(outputNames, rows, rows.length, requestStartedTime);
    }

    private void convertBytesRef(Object[][] rows) {
        if (rows.length == 0) {
            return;
        }

        final IntArrayList stringColumns = new IntArrayList();

        for (int c = 0; c < rows[0].length; c++) {
            if (rows[0][c] instanceof BytesRef) {
                stringColumns.add(c);
            }
        }

        for (int r = 0; r < rows.length; r++) {
            for (IntCursor stringColumn : stringColumns) {
                rows[r][stringColumn.value] = ((BytesRef)rows[r][stringColumn.value]).utf8ToString();
            }
        }
    }
}
