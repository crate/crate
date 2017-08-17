/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.symbol;

import io.crate.analyze.relations.QueriedRelation;
import io.crate.types.DataType;
import io.crate.types.SingleColumnTableType;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Symbol representing a sub-query
 */
public class SelectSymbol extends Symbol {

    private final QueriedRelation relation;
    private final SingleColumnTableType dataType;
    private final ResultType resultType;
    private boolean isPlanned;

    public enum ResultType {
        SINGLE_COLUMN_SINGLE_VALUE,
        SINGLE_COLUMN_MULTIPLE_VALUES
    }

    public SelectSymbol(QueriedRelation relation, SingleColumnTableType dataType, ResultType resultType) {
        this.relation = relation;
        this.dataType = dataType;
        this.resultType = resultType;
    }

    public QueriedRelation relation() {
        return relation;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Cannot stream SelectSymbol");
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.SELECT_SYMBOL;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitSelectSymbol(this, context);
    }

    @Override
    public DataType valueType() {
        if (resultType == ResultType.SINGLE_COLUMN_SINGLE_VALUE) {
            return dataType.innerType();
        }
        return dataType;
    }

    @Override
    public String toString() {
        return "SelectSymbol{" + dataType.toString() + "}";
    }

    @Override
    public String representation() {
        return "SubQuery{" + relation.getQualifiedName() + '}';
    }

    public boolean isPlanned() {
        return isPlanned;
    }

    public void markAsPlanned() {
        isPlanned = true;
    }

    public ResultType getResultType() {
        return resultType;
    }
}
