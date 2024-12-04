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

package io.crate.expression.reference.file;

import io.crate.execution.engine.collect.files.LineCollectorExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.types.DataTypes;

public class SourceLineNumberExpression extends LineCollectorExpression<Long> {

    public static final String COLUMN_NAME = "_line_number";
    private static final ColumnIdent COLUMN_IDENT = ColumnIdent.of(COLUMN_NAME);

    public static Reference getReferenceForRelation(RelationName relationName) {
        return new SimpleReference(
            new ReferenceIdent(relationName, COLUMN_IDENT), RowGranularity.DOC, DataTypes.LONG, 0, null
        );
    }

    private LineContext lineContext;

    @Override
    public void startCollect(LineContext lineContext) {
        this.lineContext = lineContext;
    }

    @Override
    public Long value() {
        return lineContext.getCurrentLineNumber();
    }
}
