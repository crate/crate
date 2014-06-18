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

package io.crate.operation.projectors;

import io.crate.metadata.ColumnIdent;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class IndexWriterProjector extends AbstractIndexWriterProjector {

    private final Input<?> sourceInput;

    private final @Nullable String[] includes;
    private final @Nullable String[] excludes;

    public IndexWriterProjector(Client client,
                                String tableName,
                                List<ColumnIdent> primaryKeys,
                                List<Input<?>> idInputs,
                                List<Input<?>> partitionedByInputs,
                                @Nullable ColumnIdent routingIdent,
                                @Nullable Input<?> routingInput,
                                Input<?> sourceInput,
                                CollectExpression<?>[] collectExpressions,
                                @Nullable Integer bulkActions,
                                @Nullable Integer concurrency,
                                @Nullable String[] includes,
                                @Nullable String[] excludes) {
        super(client, tableName, primaryKeys, idInputs, partitionedByInputs,
                routingIdent, routingInput,
                collectExpressions, bulkActions, concurrency);
        this.sourceInput = sourceInput;
        this.includes = includes;
        this.excludes = excludes;
    }

    public Object generateSource() {
        Object sourceValue = sourceInput.value();
        if (includes != null || excludes != null) {
            assert sourceValue instanceof Map;
            // exclude partitioned columns from source
            sourceValue = XContentMapValues.filter((Map)sourceValue, includes, excludes);
        }
        return sourceValue;
    }

}
