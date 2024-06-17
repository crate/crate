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

package io.crate.metadata.sys;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.function.LongSupplier;

import org.junit.jupiter.api.Test;

import io.crate.expression.reference.sys.operation.OperationContext;
import io.crate.expression.reference.sys.operation.OperationContextLog;
import io.crate.metadata.ColumnIdent;

public class SysOperationsLogTableInfoTest {

    @Test
    public void test_job_id_returns_job_id_of_operation_context_log() {
        var table = SysOperationsLogTableInfo.INSTANCE;
        var expressionFactory = table.expressions().get(new ColumnIdent("job_id"));
        var expression = expressionFactory.create();

        int id = 1;
        UUID jobId = UUID.randomUUID();
        String name = "Dummy";
        long started = 1;
        LongSupplier bytesUsed = () -> 10;
        String errorMessage = null;
        expression.setNextRow(new OperationContextLog(new OperationContext(id, jobId, name, started, bytesUsed), errorMessage));
        Object value = (String) expression.value();
        assertThat(value).isEqualTo(jobId.toString());
    }
}
