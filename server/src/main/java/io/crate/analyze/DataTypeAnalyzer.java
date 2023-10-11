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

package io.crate.analyze;

import java.util.Locale;

import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.ObjectColumnType;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FloatVectorType;
import io.crate.types.ObjectType;

public final class DataTypeAnalyzer extends DefaultTraversalVisitor<DataType<?>, Void> {

    private DataTypeAnalyzer() {
    }

    private static final DataTypeAnalyzer INSTANCE = new DataTypeAnalyzer();

    public static DataType<?> convert(ColumnType<?> columnType) {
        return columnType.accept(INSTANCE, null);
    }

    @Override
    public DataType<?> visitColumnType(ColumnType<?> node, Void context) {
        String typeName = node.name();
        if (typeName == null) {
            return DataTypes.NOT_SUPPORTED;
        } else {
            return DataTypes.of(typeName.toLowerCase(Locale.ENGLISH), node.parameters());
        }
    }

    @Override
    public DataType<?> visitObjectColumnType(ObjectColumnType<?> node, Void context) {
        ObjectType.Builder builder = ObjectType.builder();
        for (ColumnDefinition<?> columnDefinition : node.nestedColumns()) {
            ColumnType<?> type = columnDefinition.type();
            // can be null for generated columns, as then the type is inferred from the expression.
            builder.setInnerType(
                columnDefinition.ident(),
                type == null ? DataTypes.UNDEFINED : type.accept(this, context)
            );
        }
        return builder.build();
    }

    @Override
    public DataType<?> visitCollectionColumnType(CollectionColumnType<?> node, Void context) {
        DataType<?> innerType = node.innerType().accept(this, context);
        if (innerType instanceof FloatVectorType) {
            // https://github.com/apache/lucene/issues/12313
            throw new UnsupportedOperationException("Arrays of " + innerType.getName() + " are not supported");
        }
        return new ArrayType<>(innerType);
    }
}
