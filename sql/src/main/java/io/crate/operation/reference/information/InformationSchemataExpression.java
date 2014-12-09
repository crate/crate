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

package io.crate.operation.reference.information;

import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.information.InformationSchemataTableInfo;
import io.crate.metadata.information.RowCollectExpression;
import io.crate.metadata.table.SchemaInfo;
import org.apache.lucene.util.BytesRef;

public abstract class InformationSchemataExpression<T> extends RowCollectExpression<SchemaInfo, T> {

    public static final SchemataSchemaNameExpression SCHEMA_NAME_EXPRESSION = new SchemataSchemaNameExpression();

    protected InformationSchemataExpression(ReferenceInfo info) {
        super(info);
    }

    private static class SchemataSchemaNameExpression extends InformationSchemataExpression<BytesRef> {

        protected SchemataSchemaNameExpression() {
            super(InformationSchemataTableInfo.ReferenceInfos.SCHEMA_NAME);
        }

        @Override
        public BytesRef value() {
            return new BytesRef(this.row.name());
        }
    }
}
