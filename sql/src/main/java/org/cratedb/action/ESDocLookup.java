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

package org.cratedb.action;

import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.sql.GroupByOnArrayUnsupportedException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.DocLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.List;

/**
 * Wrapper around {@link SearchLookup} that implements {@link FieldLookup}
 *
 * This is used in the {@link org.cratedb.action.groupby.SQLGroupingCollector} and done so
 * that the GroupingCollector can be tested without depending on SearchLookup.
 */
public class ESDocLookup implements FieldLookup {

    DocLookup docLookup;

    @Inject
    public ESDocLookup(DocLookup searchLookup) {
        this.docLookup = searchLookup;
    }

    public void setNextDocId(int doc) {
        docLookup.setNextDocId(doc);
    }

    public void setNextReader(AtomicReaderContext context) {
        docLookup.setNextReader(context);
    }

    public Object lookupField(String columnName) throws GroupByOnArrayUnsupportedException {
        ScriptDocValues docValues = (ScriptDocValues)docLookup.get(columnName);
        if (docValues.isEmpty())
            return null;
        List<?> values = docValues.getValues();
        if (values.size() > 1) {
            throw new GroupByOnArrayUnsupportedException(columnName);
        }

        return values.get(0);
    }
}
