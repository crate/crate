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

package org.cratedb.stats;

import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.action.FieldLookup;
import org.cratedb.sql.CrateException;

import java.io.IOException;
import java.util.Map;

public class StatsTableFieldLookup implements FieldLookup {

    private final Map<String, Object> fields;

    public StatsTableFieldLookup(Map<String, Object> fields) {
        this.fields = fields;
    }

    @Override
    public void setNextDocId(int doc) {
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
    }

    @Override
    public Object lookupField(String columnName) throws IOException, CrateException {
        return fields.get(columnName);
    }
}
