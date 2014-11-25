/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.metadata.MetaDataModule;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.information.MetaDataInformationModule;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.testing.MockedClusterServiceModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CreateAnalyzerAnalyzerTest extends BaseAnalyzerTest {

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void bindSchemas() {
            super.bindSchemas();
            SchemaInfo schemaInfo = mock(SchemaInfo.class);
            when(schemaInfo.getTableInfo(TEST_DOC_TABLE_IDENT.name())).thenReturn(userTableInfo);
            schemaBinder.addBinding(DocSchemaInfo.NAME).toInstance(schemaInfo);
        }
    }

    @Override
    protected List<Module> getModules() {
        List<Module> modules = super.getModules();
        modules.addAll(Arrays.<Module>asList(
                new MockedClusterServiceModule(),
                new MetaDataInformationModule(),
                new TestMetaDataModule(),
                new MetaDataSysModule(),
                new OperatorModule())
        );
        return modules;
    }


    @Test
    public void testCreateAnalyzerSimple() throws Exception {
        AnalyzedStatement analyzedStatement = analyze("CREATE ANALYZER a1 (tokenizer lowercase)");
        assertThat(analyzedStatement, instanceOf(CreateAnalyzerAnalyzedStatement.class));
        CreateAnalyzerAnalyzedStatement createAnalyzerAnalysis = (CreateAnalyzerAnalyzedStatement) analyzedStatement;

        assertEquals("a1", createAnalyzerAnalysis.ident());
        assertEquals("lowercase", createAnalyzerAnalysis.tokenizerDefinition().v1());
        assertEquals(ImmutableSettings.EMPTY, createAnalyzerAnalysis.tokenizerDefinition().v2());

        // be sure build succeeds
        createAnalyzerAnalysis.buildSettings();
    }

    @Test
    public void testCreateAnalyzerWithCustomTokenizer() throws Exception {
        AnalyzedStatement analyzedStatement = analyze("CREATE ANALYZER a2 (" +
                "   tokenizer tok2 with (" +
                "       type='ngram'," +
                "       \"min_ngram\"=2," +
                "       \"token_chars\"=['letter', 'digits']" +
                "   )" +
                ")");
        assertThat(analyzedStatement, instanceOf(CreateAnalyzerAnalyzedStatement.class));
        CreateAnalyzerAnalyzedStatement createAnalyzerAnalysis = (CreateAnalyzerAnalyzedStatement) analyzedStatement;

        assertEquals("a2", createAnalyzerAnalysis.ident());
        assertEquals("a2_tok2", createAnalyzerAnalysis.tokenizerDefinition().v1());
        assertThat(
                createAnalyzerAnalysis.tokenizerDefinition().v2().getAsMap(),
                allOf(
                        hasEntry("index.analysis.tokenizer.a2_tok2.type", "ngram"),
                        hasEntry("index.analysis.tokenizer.a2_tok2.min_ngram", "2"),
                        hasEntry("index.analysis.tokenizer.a2_tok2.token_chars.0", "letter"),
                        hasEntry("index.analysis.tokenizer.a2_tok2.token_chars.1", "digits")
                )
        );

        // be sure build succeeds
        createAnalyzerAnalysis.buildSettings();
    }

    @Test
    public void testCreateAnalyzerWithCharFilters() throws Exception {
        AnalyzedStatement analyzedStatement = analyze("CREATE ANALYZER a3 (" +
                "   tokenizer lowercase," +
                "   char_filters (" +
                "       \"html_strip\"," +
                "       my_mapping WITH (" +
                "           type='mapping'," +
                "           mappings=['ph=>f', 'ß=>ss', 'ö=>oe']" +
                "       )" +
                "   )" +
                ")");
        assertThat(analyzedStatement, instanceOf(CreateAnalyzerAnalyzedStatement.class));
        CreateAnalyzerAnalyzedStatement createAnalyzerAnalysis = (CreateAnalyzerAnalyzedStatement) analyzedStatement;

        assertEquals("a3", createAnalyzerAnalysis.ident());
        assertEquals("lowercase", createAnalyzerAnalysis.tokenizerDefinition().v1());

        assertThat(
                createAnalyzerAnalysis.charFilters().keySet(),
                containsInAnyOrder("html_strip", "a3_my_mapping")
        );

        assertThat(
                createAnalyzerAnalysis.charFilters().get("a3_my_mapping").getAsMap(),
                hasEntry("index.analysis.char_filter.a3_my_mapping.type", "mapping")
        );
        assertThat(
                createAnalyzerAnalysis.charFilters().get("a3_my_mapping")
                        .getAsArray("index.analysis.char_filter.a3_my_mapping.mappings"),
                arrayContainingInAnyOrder("ph=>f", "ß=>ss", "ö=>oe")
        );

        // be sure build succeeds
        createAnalyzerAnalysis.buildSettings();
    }

    @Test
    public void testCreateAnalyzerWithTokenFilters() throws Exception {
        AnalyzedStatement analyzedStatement = analyze("CREATE ANALYZER a11 (" +
                "  TOKENIZER standard," +
                "  TOKEN_FILTERS (" +
                "    lowercase," +
                "    mystop WITH (" +
                "      type='stop'," +
                "      stopword=['the', 'over']" +
                "    )" +
                "  )" +
                ")");
        assertThat(analyzedStatement, instanceOf(CreateAnalyzerAnalyzedStatement.class));
        CreateAnalyzerAnalyzedStatement createAnalyzerAnalysis = (CreateAnalyzerAnalyzedStatement) analyzedStatement;

        assertEquals("a11", createAnalyzerAnalysis.ident());
        assertEquals("standard", createAnalyzerAnalysis.tokenizerDefinition().v1());

        assertThat(
                createAnalyzerAnalysis.tokenFilters().keySet(),
                containsInAnyOrder("lowercase", "a11_mystop")
        );

        assertThat(
                createAnalyzerAnalysis.tokenFilters().get("a11_mystop").getAsMap(),
                hasEntry("index.analysis.filter.a11_mystop.type", "stop")
        );
        assertThat(
                createAnalyzerAnalysis.tokenFilters().get("a11_mystop")
                        .getAsArray("index.analysis.filter.a11_mystop.stopword"),
                arrayContainingInAnyOrder("the", "over")
        );

        // be sure build succeeds
        createAnalyzerAnalysis.buildSettings();
    }

    @Test
    public void testCreateAnalyzerExtendingBuiltin() throws Exception {
        AnalyzedStatement analyzedStatement = analyze("CREATE ANALYZER a4 EXTENDS " +
                "german WITH (" +
                "   \"stop_words\"=['der', 'die', 'das']" +
                ")");
        assertThat(analyzedStatement, instanceOf(CreateAnalyzerAnalyzedStatement.class));
        CreateAnalyzerAnalyzedStatement createAnalyzerAnalysis = (CreateAnalyzerAnalyzedStatement) analyzedStatement;

        assertEquals("a4", createAnalyzerAnalysis.ident());
        assertEquals("german", createAnalyzerAnalysis.extendedAnalyzerName());

        assertThat(
                createAnalyzerAnalysis.genericAnalyzerSettings().getAsArray("index.analysis.analyzer.a4.stop_words"),
                arrayContainingInAnyOrder("der", "die", "das")
        );

        // be sure build succeeds
        createAnalyzerAnalysis.buildSettings();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void createAnalyzerWithoutTokenizer() throws Exception {
        CreateAnalyzerAnalyzedStatement analysis = (CreateAnalyzerAnalyzedStatement)analyze(
                "CREATE ANALYZER a6 (" +
                "  char_filters (" +
                "    \"html_strip\"" +
                "  )," +
                "  token_filters (" +
                "    lowercase" +
                "  )" +
                ")");
        analysis.buildSettings();
    }

    @Test( expected = IllegalArgumentException.class)
    public void overrideDefaultAnalyzer() throws Exception {
        analyze("CREATE ANALYZER \"default\" (" +
                "  TOKENIZER whitespace" +
                ")");
    }

    @Test(expected = IllegalArgumentException.class)
    public void overrideBuiltInAnalyzer() throws Exception {
        analyze("CREATE ANALYZER \"keyword\" (" +
                "  char_filters (" +
                "    html_strip" +
                "  )," +
                "  tokenizer standard" +
                ")");
    }
}
