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

package org.cratedb.lucene.fields;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.MultiTermQueryWrapperFilter;
import org.apache.lucene.search.SortField;
import org.cratedb.index.ColumnDefinition;

import java.io.IOException;

public abstract class LuceneField<E extends Object> {

    public final String name;
    public final boolean allowMultipleValues;
    public SortField.Type type;
    public Analyzer analyzer = new KeywordAnalyzer();

    public LuceneField(String name, boolean allowMultipleValues) {
        this.name = name;
        this.allowMultipleValues = allowMultipleValues;
    }

    public abstract Object getValue(IndexableField field);
    public abstract MultiTermQuery rangeQuery(Object from, Object to,
                                              boolean includeLower, boolean includeUpper);
    public abstract MultiTermQueryWrapperFilter rangeFilter(
        Object from, Object to, boolean includeLower, boolean includeUpper);

    public abstract ColumnDefinition getColumnDefinition(String tableName, int ordinalPosition);

    public abstract Object mappedValue(Object value);

    public abstract Field field(E value);

    public abstract TokenStream tokenStream(E value) throws IOException;
}
