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

package org.cratedb.information_schema;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexableField;
import org.cratedb.action.FieldLookup;
import org.cratedb.lucene.fields.LuceneField;
import org.cratedb.sql.GroupByOnArrayUnsupportedException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class InformationSchemaFieldLookup implements FieldLookup {

    private final Map<String, LuceneField> fieldMapper;
    int docId;
    private AtomicReader reader;

    public InformationSchemaFieldLookup(Map<String, LuceneField> fieldMapper) {
        this.fieldMapper = fieldMapper;
    }

    @Override
    public void setNextDocId(int doc) {
        this.docId = doc;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        this.reader = context.reader();
    }

    @Override
    public Object lookupField(final String columnName)
            throws IOException, GroupByOnArrayUnsupportedException
    {
        Set<String> fieldsToLoad = new HashSet<>();
        fieldsToLoad.add(columnName);

        IndexableField[] fields = reader.document(docId, fieldsToLoad).getFields(columnName);
        if (fields.length > 1) {
            throw new GroupByOnArrayUnsupportedException(columnName);
        }

        return fieldMapper.get(columnName).getValue(fields[0]);
    }
}

