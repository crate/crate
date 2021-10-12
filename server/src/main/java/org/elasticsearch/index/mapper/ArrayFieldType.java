/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryShardContext;

class ArrayFieldType extends MappedFieldType {

    private final MappedFieldType innerFieldType;

    ArrayFieldType(MappedFieldType innerFieldType) {
        super(innerFieldType.name(), innerFieldType.isSearchable(), innerFieldType.hasDocValues());
        this.innerFieldType = innerFieldType;
    }

    @Override
    public String name() {
        return innerFieldType.name();
    }

    @Override
    public String typeName() {
        return ArrayMapper.CONTENT_TYPE;
    }

    @Override
    public boolean hasDocValues() {
        return innerFieldType.hasDocValues();
    }

    @Override
    public NamedAnalyzer indexAnalyzer() {
        return innerFieldType.indexAnalyzer();
    }

    @Override
    public void setIndexAnalyzer(NamedAnalyzer analyzer) {
        innerFieldType.setIndexAnalyzer(analyzer);
    }

    @Override
    public NamedAnalyzer searchAnalyzer() {
        return innerFieldType.searchAnalyzer();
    }

    @Override
    public void setSearchAnalyzer(NamedAnalyzer analyzer) {
        innerFieldType.setSearchAnalyzer(analyzer);
    }

    @Override
    public NamedAnalyzer searchQuoteAnalyzer() {
        return innerFieldType.searchQuoteAnalyzer();
    }

    @Override
    public void setSearchQuoteAnalyzer(NamedAnalyzer analyzer) {
        innerFieldType.setSearchQuoteAnalyzer(analyzer);
    }

    @Override
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper) {
        return innerFieldType.rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper);
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        return innerFieldType.existsQuery(context);
    }
}
