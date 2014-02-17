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

package org.cratedb.action.collect;

import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.DataType;
import org.cratedb.action.FieldLookup;
import org.cratedb.index.ColumnDefinition;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;

public class FieldLookupExpression<ReturnType> extends
        CollectorExpression<ReturnType> implements ColumnReferenceExpression{

    private ESLogger logger = Loggers.getLogger(getClass());

    private final ColumnDefinition columnDefinition;
    private FieldLookup fieldLookup;

    public FieldLookupExpression(ColumnDefinition columnDefinition) {
        this.columnDefinition = columnDefinition;
    }

    public void startCollect(CollectorContext context) {
        fieldLookup = context.fieldLookup();
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        fieldLookup.setNextReader(context);
    }

    @Override
    public void setNextDocId(int docId) {
        fieldLookup.setNextDocId(docId);
    }

    public ReturnType evaluate() {
        try {
            return (ReturnType) fieldLookup.lookupField(columnDefinition.columnName);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public DataType returnType() {
        return columnDefinition.dataType;
    }


    public static FieldLookupExpression create(ColumnDefinition columnDefinition) {
        switch (columnDefinition.dataType) {
            case STRING:
                return new FieldLookupExpression<String>(columnDefinition);
            case DOUBLE:
                return new FieldLookupExpression<Double>(columnDefinition);
            case BOOLEAN:
                return new FieldLookupExpression<Boolean>(columnDefinition);
            case LONG:
                return new FieldLookupExpression<Long>(columnDefinition);
            default:
                return new FieldLookupExpression(columnDefinition);
        }
    }

    @Override
    public String toString() {
        return columnName();
    }

    @Override
    public String columnName() {
        return columnDefinition.columnName;
    }
}
