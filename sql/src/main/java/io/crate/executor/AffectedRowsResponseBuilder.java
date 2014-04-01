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

import io.crate.Constants;
import io.crate.DataType;
import io.crate.action.sql.SQLResponse;

public class AffectedRowsResponseBuilder implements ResponseBuilder {

    /**
     * expect the first column in the first row of <code>rows</code> to contain the number of affected rows
     */
    @Override
    public SQLResponse buildResponse(DataType[] dataTypes, String[] outputNames, Object[][] rows, long requestStartedTime) {
        long affectedRows = 0;
        if (rows.length >= 1 && rows[0].length >= 1) {
            affectedRows = ((Number)rows[0][0]).longValue();
        }
        return new SQLResponse(outputNames, Constants.EMPTY_RESULT, affectedRows, requestStartedTime);
    }
}
