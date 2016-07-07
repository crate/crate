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

package io.crate.analyze;

import com.google.common.collect.Lists;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;

import javax.annotation.Nullable;
import java.util.*;

public class QueriedSelectRelation implements QueriedRelation {

    private final AnalyzedRelation analyzedRelation;
    private final QuerySpec querySpec;
    private final Map<String, Field> fields;

    public QueriedSelectRelation(AnalyzedRelation analyzedRelation, Collection<? extends Path> paths, QuerySpec querySpec) {
        this.analyzedRelation = analyzedRelation;
        this.querySpec = querySpec;
        this.fields = new HashMap<>(paths.size());
        Iterator<Symbol> qsIter = querySpec.outputs().iterator();
        for (Path path : paths) {
            fields.put(path.outputName(), new Field(this, path, qsIter.next().valueType()));
        }
    }

    public AnalyzedRelation relation() {
        return analyzedRelation;
    }

    @Override
    public QuerySpec querySpec() {
        return querySpec;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitQueriedSelectRelation(this, context);
    }

    @Nullable
    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        if (operation != Operation.READ) {
            throw new UnsupportedOperationException("getField on QueriedSelectRelation is only supported for READ operations");
        }
        return fields.get(path.outputName());
    }

    @Override
    public List<Field> fields() {
        return Lists.newArrayList(fields.values());
    }
}
