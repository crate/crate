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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.cratedb.action.FieldLookup;
import org.elasticsearch.common.collect.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SQLFetchCollector extends Collector {

    private final ParsedStatement parsedStatement;
    private final FieldLookup fieldLookup;

    public List<List<Object>> results = new ArrayList();

    public SQLFetchCollector(ParsedStatement parsedStatement, FieldLookup fieldLookup) {
        this.parsedStatement = parsedStatement;
        this.fieldLookup = fieldLookup;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public void collect(int doc) throws IOException {
        List<Object> rowResult = new ArrayList<>(parsedStatement.outputFields.size());
        for (Tuple<String, String> columnNames : parsedStatement.outputFields()) {
            rowResult.add(fieldLookup.lookupField(columnNames.v2()));
        }
        results.add(rowResult);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }
}
