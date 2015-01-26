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

package io.crate.metadata.table;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.symbol.DynamicReference;
import org.elasticsearch.common.Nullable;

public abstract class AbstractDynamicTableInfo extends AbstractTableInfo {

    protected AbstractDynamicTableInfo(SchemaInfo schemaInfo) {
        super(schemaInfo);
    }

    @Nullable
    @Override
    public DynamicReference getDynamic(ColumnIdent ident, boolean forWrite) {
        boolean parentIsIgnored = false;
        ColumnPolicy parentPolicy = columnPolicy();
        if (!ident.isColumn()) {
            // see if parent is strict object
            ColumnIdent parentIdent = ident.getParent();
            ReferenceInfo parentInfo = null;

            while (parentIdent != null) {
                parentInfo = getReferenceInfo(parentIdent);
                if (parentInfo != null) {
                    break;
                }
                parentIdent = parentIdent.getParent();
            }

            if (parentInfo != null) {
                parentPolicy = parentInfo.columnPolicy();
            }
        }

        switch (parentPolicy) {
            case DYNAMIC:
                if (!forWrite) return null;
                break;
            case STRICT:
                if (forWrite) throw new ColumnUnknownException(ident.sqlFqn());
                return null;
            case IGNORED:
                parentIsIgnored = true;
                break;
            default:
                break;
        }
        DynamicReference reference = new DynamicReference(new ReferenceIdent(ident(), ident), rowGranularity());
        if (parentIsIgnored) {
            reference.columnPolicy(ColumnPolicy.IGNORED);
        }
        return reference;
    }

}
