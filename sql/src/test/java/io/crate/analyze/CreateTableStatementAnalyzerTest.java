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

package io.crate.analyze;

import io.crate.metadata.MetaDataModule;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.information.MetaDataInformationModule;
import io.crate.metadata.sys.MetaDataSysModule;
import io.crate.metadata.table.SchemaInfo;
import io.crate.operation.operator.OperatorModule;
import io.crate.metadata.FulltextAnalyzerResolver;
import org.elasticsearch.common.inject.Module;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CreateTableStatementAnalyzerTest extends BaseAnalyzerTest {

    static class TestMetaDataModule extends MetaDataModule {
        @Override
        protected void configure() {
            FulltextAnalyzerResolver fulltextAnalyzerResolver = mock(FulltextAnalyzerResolver.class);
            when(fulltextAnalyzerResolver.hasCustomAnalyzer(anyString())).thenReturn(false);
            bind(FulltextAnalyzerResolver.class).toInstance(fulltextAnalyzerResolver);
            super.configure();
        }

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
                new TestModule(),
                new MetaDataInformationModule(),
                new TestMetaDataModule(),
                new MetaDataSysModule(),
                new OperatorModule())
        );
        return modules;
    }

    @Test
    public void testCreateTableWithAlternativePrimaryKeySyntax() throws Exception {
        CreateTableAnalysis analysis = (CreateTableAnalysis)analyze(
                "create table foo (id integer, primary key (id))"
        );

        List<String> primaryKeys = analysis.primaryKeys();
        assertThat(primaryKeys.size(), is(1));
        assertThat(primaryKeys.get(0), is("id"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimpleCreateTable() throws Exception {
        CreateTableAnalysis analysis = (CreateTableAnalysis)analyze(
                "create table foo (id integer primary key, name string) " +
                "clustered into 3 shards with (number_of_replicas=0)");

        assertThat(analysis.indexSettings().get("number_of_shards"), is("3"));
        assertThat(analysis.indexSettings().get("number_of_replicas"), is("0"));

        Map<String, Object> metaMapping = analysis.metaMapping();
        Map<String, Object> metaName = (Map<String, Object>)((Map<String, Object>)
                metaMapping.get("columns")).get("name");

        assertNull(metaName.get("collection_type"));

        Map<String,Object> mappingProperties = analysis.mappingProperties();

        Map<String, Object> idMapping = (Map<String, Object>)mappingProperties.get("id");
        assertThat((Boolean)idMapping.get("store"), is(false));
        assertThat((String)idMapping.get("type"), is("integer"));

        Map<String, Object> nameMapping = (Map<String, Object>)mappingProperties.get("name");
        assertThat((Boolean)nameMapping.get("store"), is(false));
        assertThat((String)nameMapping.get("type"), is("string"));

        List<String> primaryKeys = analysis.primaryKeys();
        assertThat(primaryKeys.size(), is(1));
        assertThat(primaryKeys.get(0), is("id"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithObjects() throws Exception {
        CreateTableAnalysis analysis = (CreateTableAnalysis)analyze(
                "create table foo (id integer primary key, details object as (name string, age integer))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> details = (Map<String, Object>)mappingProperties.get("details");

        assertThat((String)details.get("type"), is("object"));
        assertThat((String)details.get("dynamic"), is("true"));

        Map<String, Object> detailsProperties = (Map<String, Object>)details.get("properties");
        Map<String, Object> nameProperties = (Map<String, Object>) detailsProperties.get("name");
        assertThat((String)nameProperties.get("type"), is("string"));

        Map<String, Object> ageProperties = (Map<String, Object>) detailsProperties.get("age");
        assertThat((String)ageProperties.get("type"), is("integer"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithStrictObject() throws Exception {
        CreateTableAnalysis analysis = (CreateTableAnalysis)analyze(
                "create table foo (id integer primary key, details object(strict) as (name string, age integer))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> details = (Map<String, Object>)mappingProperties.get("details");

        assertThat((String)details.get("type"), is("object"));
        assertThat((String)details.get("dynamic"), is("strict"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithIgnoredObject() throws Exception {
        CreateTableAnalysis analysis = (CreateTableAnalysis)analyze(
                "create table foo (id integer primary key, details object(ignored))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> details = (Map<String, Object>)mappingProperties.get("details");

        assertThat((String)details.get("type"), is("object"));
        assertThat((String)details.get("dynamic"), is("false"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithSubscriptInFulltextIndexDefinition() throws Exception {
        CreateTableAnalysis analysis = (CreateTableAnalysis)analyze(
                "create table my_table1g ("+
                        "title string, " +
                        "author object(dynamic) as ( " +
                            "name string, " +
                            "birthday timestamp " +
                        "), " +
                "INDEX author_title_ft using fulltext(title, author['name']))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> details = (Map<String, Object>)mappingProperties.get("author");
        Map<String, Object> nameMapping = (Map<String, Object>)((Map<String, Object>) details.get("properties")).get("name");

        assertThat(((List<String>) nameMapping.get("copy_to")).get(0), is("author_title_ft"));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testCreateTableWithInvalidFulltextIndexDefinition() throws Exception {
        analyze("create table my_table1g ("+
                        "title string, " +
                        "author object(dynamic) as ( " +
                        "name string, " +
                        "birthday timestamp " +
                        "), " +
                        "INDEX author_title_ft using fulltext(title, author['name']['foo']['bla']))");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithObjectsArray() throws Exception {
        CreateTableAnalysis analysis = (CreateTableAnalysis)analyze(
                "create table foo (id integer primary key, details array(object as (name string, age integer)))");

        Map<String, Object> metaMapping = analysis.metaMapping();
        Map<String, Object> metaDetails = (Map<String, Object>)((Map<String, Object>)
                metaMapping.get("columns")).get("details");

        assertThat((String) metaDetails.get("collection_type"), is("array"));

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> details = (Map<String, Object>)mappingProperties.get("details");

        assertThat((String)details.get("type"), is("object"));

        Map<String, Object> detailsProperties = (Map<String, Object>)details.get("properties");
        Map<String, Object> nameProperties = (Map<String, Object>) detailsProperties.get("name");
        assertThat((String)nameProperties.get("type"), is("string"));

        Map<String, Object> ageProperties = (Map<String, Object>) detailsProperties.get("age");
        assertThat((String)ageProperties.get("type"), is("integer"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithAnalyzer() throws Exception {
        CreateTableAnalysis analysis = (CreateTableAnalysis)analyze(
                "create table foo (id integer primary key, content string INDEX using fulltext with (analyzer='german'))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> contentMapping = (Map<String, Object>)mappingProperties.get("content");

        assertThat((String)contentMapping.get("index"), is("analyzed"));
        assertThat((String)contentMapping.get("analyzer"), is("german"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithAnalyzerParameter() throws Exception {
        CreateTableAnalysis analysis = (CreateTableAnalysis)analyze(
                "create table foo (id integer primary key, content string INDEX using fulltext with (analyzer=?))",
                new Object[] {"german"}
        );

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> contentMapping = (Map<String, Object>)mappingProperties.get("content");

        assertThat((String)contentMapping.get("index"), is("analyzed"));
        assertThat((String)contentMapping.get("analyzer"), is("german"));
    }

    @Test (expected = UnsupportedOperationException.class)
    public void testCreateTableWithSchemaName() throws Exception {
        analyze("create table something.foo (id integer primary key)");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithIndexColumn() throws Exception {
        CreateTableAnalysis analysis = (CreateTableAnalysis)analyze(
                "create table foo (id integer primary key, content string, INDEX content_ft using fulltext (content))");

        Map<String, Object> mappingProperties = analysis.mappingProperties();
        Map<String, Object> contentMapping = (Map<String, Object>)mappingProperties.get("content");

        assertThat((String)contentMapping.get("index"), is("not_analyzed"));
        assertThat(((List<String>)contentMapping.get("copy_to")).get(0), is("content_ft"));

        Map<String, Object> ft_mapping = (Map<String, Object>)mappingProperties.get("content_ft");
        assertThat((String)ft_mapping.get("index"), is("analyzed"));
        assertThat((String)ft_mapping.get("analyzer"), is("standard"));
    }
}
