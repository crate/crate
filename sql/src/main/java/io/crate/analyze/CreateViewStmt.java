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

import io.crate.analyze.relations.QueriedRelation;
import io.crate.metadata.TableIdent;

public final class CreateViewStmt implements AnalyzedStatement {

    private final TableIdent name;
    private final QueriedRelation query;
    private final boolean replaceExisting;

    CreateViewStmt(TableIdent name, QueriedRelation query, boolean replaceExisting) {
        this.name = name;
        this.query = query;
        this.replaceExisting = replaceExisting;
    }

    public TableIdent name() {
        return name;
    }

    public QueriedRelation query() {
        return query;
    }

    public boolean replaceExisting() {
        return replaceExisting;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitCreateViewStmt(this, context);
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }
}
