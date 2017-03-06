/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze;

import io.crate.sql.tree.*;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Locale;

public class DataTypeAnalyzer extends DefaultTraversalVisitor<DataType, Void> {

    private DataTypeAnalyzer() {}

    private final static DataTypeAnalyzer INSTANCE = new DataTypeAnalyzer();

    public static DataType convert(Expression expression) {
        return INSTANCE.process(expression, null);
    }

    @Override
    public DataType visitColumnType(ColumnType node, Void context) {
        String typeName = node.name();
        if (typeName == null) {
            return DataTypes.NOT_SUPPORTED;
        } else {
            typeName = typeName.toLowerCase(Locale.ENGLISH);
            return DataTypes.ofName(typeName);
        }
    }

    @Override
    public DataType visitObjectColumnType(ObjectColumnType node, Void context) {
        return DataTypes.OBJECT;
    }

    @Override
    public DataType visitCollectionColumnType(CollectionColumnType node, Void context) {
        if (node.type() == ColumnType.Type.SET) {
            throw new UnsupportedOperationException("the SET dataType is currently not supported");
        }

        if (node.innerType().type() != ColumnType.Type.PRIMITIVE) {
            throw new UnsupportedOperationException("Nesting ARRAY or SET types is not supported");
        }

        DataType innerType = process(node.innerType(), context);
        return new ArrayType(innerType);
    }


}
