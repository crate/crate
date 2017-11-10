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

import io.crate.action.sql.SessionContext;
import io.crate.metadata.Schemas;
import io.crate.metadata.TransactionContext;
import io.crate.sql.tree.CreateIngestRule;
import io.crate.sql.tree.QualifiedName;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class MqttAnalyzerTest {

    @Test
    public void testDefaultSchemaHandling() {
        Schemas schemas = Mockito.mock(Schemas.class);
        CreateIngestionRuleAnalyzer analyzer = new CreateIngestionRuleAnalyzer(schemas);
        final CreateIngestRule createIngestRule =
            new CreateIngestRule(
                "rule",
                "source",
                QualifiedName.of("table"),
                Optional.empty());
        SessionContext sessionContext = SessionContext.create();
        sessionContext.setDefaultSchema("custom");
        ParameterContext parameterContext = Mockito.mock(ParameterContext.class);
        ParamTypeHints paramTypeHints = Mockito.mock(ParamTypeHints.class);
        Analysis analysis = new Analysis(new TransactionContext(sessionContext), parameterContext, paramTypeHints);

        CreateIngestionRuleAnalysedStatement analyzed = analyzer.analyze(createIngestRule, analysis);

        assertThat(analyzed.targetTable().schema(), is("custom"));
        assertThat(analyzed.targetTable().name(), is("table"));
    }
}
