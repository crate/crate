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

package io.crate.analyze;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.ANALYZER;
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.index.engine.EngineConfig.INDEX_CODEC_SETTING;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.TableElementsAnalyzer.RefBuilder;
import io.crate.common.collections.Maps;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.ConversionException;
import io.crate.exceptions.InvalidColumnNameException;
import io.crate.exceptions.InvalidRelationName;
import io.crate.exceptions.InvalidSchemaNameException;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.AlterTablePlan;
import io.crate.planner.node.ddl.CreateBlobTablePlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.parser.ParsingException;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.Asserts;
import io.crate.testing.SQLExecutor;
import io.crate.testing.TestingHelpers;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.FloatVectorType;

public class CreateAlterTableStatementAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void prepare() throws IOException {
        String analyzerSettings = FulltextAnalyzerResolver.encodeSettings(
            Settings.builder().put("search", "foobar").build()).utf8ToString();
        Metadata metadata = Metadata.builder()
            .persistentSettings(
                Settings.builder().put(ANALYZER.buildSettingName("ft_search"), analyzerSettings).build())
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        e = SQLExecutor.builder(clusterService)
            .setNumNodes(3)
            .build()
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addPartitionedTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS)
            .addTable(
                "create table doc.user_refresh_interval (" +
                "  id bigint," +
                "  content text" +
                ")" +
                " clustered by (id)");
        plannerContext = e.getPlannerContext();
    }

    private <S> S analyze(String stmt, Object... arguments) {
        return analyze(e, stmt, arguments);
    }

    @SuppressWarnings("unchecked")
    private <S> S analyze(SQLExecutor e, String stmt, Object... arguments) {
        AnalyzedStatement analyzedStatement = e.analyze(stmt);
        if (analyzedStatement instanceof AnalyzedCreateTable analyzedCreateTable) {
            return (S) analyzedCreateTable.bind(
                new NumberOfShards(clusterService),
                e.fulltextAnalyzerResolver(),
                plannerContext.nodeContext(),
                plannerContext.transactionContext(),
                new RowN(arguments),
                SubQueryResults.EMPTY
            );
        } else if (analyzedStatement instanceof AnalyzedAlterTable) {
            return (S) AlterTablePlan.bind(
                (AnalyzedAlterTable) analyzedStatement,
                plannerContext.transactionContext(),
                plannerContext.nodeContext(),
                new RowN(arguments),
                SubQueryResults.EMPTY,
                plannerContext.clusterState().metadata()
            );
        } else {
            return (S) analyzedStatement;
        }
    }

    @Test
    public void test_cannot_create_table_that_contains_a_column_definition_of_type_time() {
        assertThatThrownBy(() -> analyze("create table t (ts time with time zone)"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Type `time with time zone` does not support storage");
    }

    @Test
    public void testCreateTableInSystemSchemasIsProhibited() {
        for (String schema : Schemas.READ_ONLY_SYSTEM_SCHEMAS) {
            var stmt = String.format("CREATE TABLE %s.%s (ordinal INTEGER, name STRING)", schema, "my_table");
            assertThatThrownBy(() -> analyze(stmt))
                .as("create table in read-only schema must fail")
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot create relation in read-only schema: " + schema);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithAlternativePrimaryKeySyntax() {
        BoundCreateTable analysis = analyze(
            "create table foo (id integer, name string, primary key (id, name))"
        );

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> meta = (Map<String, Object>) mapping.get("_meta");
        List<String> primaryKeys = (List<String>) meta.get("primary_keys");
        assertThat(primaryKeys).hasSize(2);
        assertThat(primaryKeys.get(0)).isEqualTo("id");
        assertThat(primaryKeys.get(1)).isEqualTo("name");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimpleCreateTable() {
        BoundCreateTable analysis = analyze(
            "create table foo (id integer primary key, name string not null) " +
            "clustered into 3 shards with (number_of_replicas=0)");

        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey()))
            .isEqualTo("3");
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey()))
            .isEqualTo("0");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> metaMapping = ((Map<String, Object>) mapping.get("_meta"));


        assertThat(metaMapping.get("columns")).isNull();

        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");

        Map<String, Object> idMapping = (Map<String, Object>) mappingProperties.get("id");
        assertThat(idMapping.get("type")).isEqualTo("integer");

        Map<String, Object> nameMapping = (Map<String, Object>) mappingProperties.get("name");
        assertThat(nameMapping.get("type")).isEqualTo("keyword");

        List<String> primaryKeys = (List<String>) metaMapping.get("primary_keys");
        assertThat(primaryKeys).hasSize(1);
        assertThat(primaryKeys.get(0)).isEqualTo("id");

        Map<String, List<String>> constraints = (Map<String, List<String>>) metaMapping.get("constraints");
        List<String> notNullColumns = constraints != null ? constraints.get("not_null") : List.of();
        assertThat(notNullColumns).hasSize(1);
        assertThat(notNullColumns.get(0)).isEqualTo("name");
    }

    @Test
    public void testSimpleCreateTableWithNullConstraint() {
        assertThatThrownBy(() -> analyze("create table foo (id integer primary key null)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Column \"id\" is declared as PRIMARY KEY, therefore, cannot be declared NULL");

        assertThatThrownBy(() -> analyze("create table foo (id integer null primary key)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Column \"id\" is declared NULL, therefore, cannot be declared as a PRIMARY KEY");

        assertThatThrownBy(() -> analyze("create table foo (name string not null null)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Column \"name\" is declared as NOT NULL, therefore, cannot be declared NULL");

        assertThatThrownBy(() -> analyze("create table foo (name string null not null)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Column \"name\" is declared NULL, therefore, cannot be declared NOT NULL");

        AnalyzedCreateTable analysis = e.analyze("create table foo (name string null)");
        Map<ColumnIdent, RefBuilder> columns = analysis.columns();
        RefBuilder rb = columns.get(new ColumnIdent("name"));
        assertThat(rb.isExplicitlyNull()).isTrue();
    }

    @Test
    public void testCreateTableWithNullConstraintAndPrimaryKeyOnTableLevel() {
        assertThatThrownBy(() -> analyze("create table t1 (a int null, primary key(a))"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Column \"a\" is declared NULL, therefore, cannot be declared as a PRIMARY KEY");

        assertThatThrownBy(() -> analyze("create table t1 (a int null, b int, primary key(a, b))"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Column \"a\" is declared NULL, therefore, cannot be declared as a PRIMARY KEY");

        assertThatThrownBy(() -> analyze("create table t1 (a int, b int null, primary key(a, b))"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Column \"b\" is declared NULL, therefore, cannot be declared as a PRIMARY KEY");

        AnalyzedCreateTable analysis = e.analyze("create table foo (a int, b int, c int null, primary key(a, b))");
        Map<ColumnIdent, RefBuilder> columns = analysis.columns();
        RefBuilder rb = columns.get(new ColumnIdent("c"));
        assertThat(rb.isExplicitlyNull()).isTrue();
    }

    @Test
    public void testCreateTableWithDefaultNumberOfShards() {
        BoundCreateTable analysis = analyze("create table foo (id integer primary key, name string)");
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey()))
            .isEqualTo("6");
    }

    @Test
    public void testCreateTableWithDefaultNumberOfShardsWithClusterByClause() {
        BoundCreateTable analysis = analyze("create table foo (id integer primary key) clustered by (id)");
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey()))
            .isEqualTo("6");
    }

    @Test
    public void testCreateTableNumberOfShardsProvidedInClusteredClause() {
        BoundCreateTable analysis = analyze(
            "create table foo (id integer primary key) " +
            "clustered by (id) into 8 shards"
        );
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey()))
            .isEqualTo("8");
    }

    @Test
    public void testCreateTableWithTotalFieldsLimit() {
        BoundCreateTable analysis = analyze(
            "CREATE TABLE foo (id int primary key) " +
            "with (\"mapping.total_fields.limit\"=5000)");
        assertThat(analysis.tableParameter().settings().get(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey()))
            .isEqualTo("5000");
    }

    @Test
    public void testCreateTableWithRefreshInterval() {
        BoundCreateTable analysis = analyze(
            "CREATE TABLE foo (id int primary key, content string) " +
            "with (refresh_interval='5000ms')");
        assertThat(analysis.tableParameter().settings().get(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey()))
            .isEqualTo("5s");
    }

    @Test
    public void testCreateTableWithNumberOfShardsOnWithClauseIsInvalid() {
        assertThatThrownBy(
            () -> analyze("CREATE TABLE foo (id int primary key, content string) with (number_of_shards=8)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid property \"number_of_shards\" passed to [ALTER | CREATE] TABLE statement");
    }

    @Test
    public void testCreateTableWithRefreshIntervalWrongNumberFormat() {
        assertThatThrownBy(
            () -> analyze("CREATE TABLE foo (id int primary key, content string) " +
                          "with (refresh_interval='1asdf')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("failed to parse [1asdf] as a time value");
    }

    @Test
    public void testAlterTableWithRefreshInterval() {
        // alter t set
        BoundAlterTable analysisSet = analyze(
            "ALTER TABLE user_refresh_interval " +
            "SET (refresh_interval = '5000ms')");
        assertThat(analysisSet.tableParameter().settings().get(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey()))
            .isEqualTo("5s");

        // alter t reset
        BoundAlterTable analysisReset = analyze(
            "ALTER TABLE user_refresh_interval " +
            "RESET (refresh_interval)");
        assertThat(analysisReset.tableParameter().settings().get(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey()))
            .isEqualTo("1s");
    }

    @Test
    public void testTotalFieldsLimitCanBeUsedWithAlterTable() {
        BoundAlterTable analysisSet = analyze(
            "ALTER TABLE users " +
            "SET (\"mapping.total_fields.limit\" = '5000')");
        assertThat(analysisSet.tableParameter().settings().get(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey()))
            .isEqualTo("5000");

        // Check if resetting total_fields results in default value
        BoundAlterTable analysisReset = analyze(
            "ALTER TABLE users " +
            "RESET (\"mapping.total_fields.limit\")");
        assertThat(analysisReset.tableParameter().settings().get(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey()))
            .isEqualTo("1000");
    }

    @Test
    public void testAlterTableWithColumnPolicy() {
        BoundAlterTable analysisSet = analyze(
            "ALTER TABLE user_refresh_interval " +
            "SET (column_policy = 'strict')");
        assertThat(analysisSet.tableParameter().mappings().get(TableParameters.COLUMN_POLICY.getKey()))
            .isEqualTo(ColumnPolicy.STRICT.lowerCaseName());
    }

    @Test
    public void testAlterTableWithInvalidColumnPolicy() {
        assertThatThrownBy(() -> analyze("ALTER TABLE user_refresh_interval SET (column_policy = 'ignored')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid value for argument 'column_policy'");
    }

    @Test
    public void testAlterTableWithMaxNGramDiffSetting() {
        BoundAlterTable analysisSet = analyze(
            "ALTER TABLE users " +
            "SET (max_ngram_diff = 42)");
        assertThat(analysisSet.tableParameter().settings().get(IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey()))
            .isEqualTo("42");
    }

    @Test
    public void testAlterTableWithMaxShingleDiffSetting() {
        BoundAlterTable analysisSet = analyze(
            "ALTER TABLE users " +
            "SET (max_shingle_diff = 43)");
        assertThat(analysisSet.tableParameter().settings().get(IndexSettings.MAX_SHINGLE_DIFF_SETTING.getKey()))
            .isEqualTo("43");
    }

    @Test
    public void testCreateTableWithClusteredBy() {
        BoundCreateTable analysis = analyze(
            "create table foo (id integer, name string) clustered by(id)");

        assertThat(analysis.routingColumn().name()).isEqualTo("id");
    }

    @Test
    public void testCreateTableWithClusteredByNotInPrimaryKeys() {
        assertThatThrownBy(
            () -> analyze("create table foo (id integer primary key, name string) clustered by(name)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Clustered by column `name` must be part of primary keys: [id]");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithObjects() {
        BoundCreateTable analysis = analyze(
            "create table foo (id integer primary key, details object as (name string, age integer))");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");
        Map<String, Object> details = (Map<String, Object>) mappingProperties.get("details");

        assertThat(details.get("type")).isEqualTo("object");
        assertThat(details.get("dynamic")).isEqualTo("true");

        Map<String, Object> detailsProperties = (Map<String, Object>) details.get("properties");
        Map<String, Object> nameProperties = (Map<String, Object>) detailsProperties.get("name");
        assertThat(nameProperties.get("type")).isEqualTo("keyword");

        Map<String, Object> ageProperties = (Map<String, Object>) detailsProperties.get("age");
        assertThat(ageProperties.get("type")).isEqualTo("integer");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithStrictObject() {
        BoundCreateTable analysis = analyze(
            "create table foo (id integer primary key, details object(strict) as (name string, age integer))");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");
        Map<String, Object> details = (Map<String, Object>) mappingProperties.get("details");

        assertThat(details.get("type")).isEqualTo("object");
        assertThat(details.get("dynamic")).isEqualTo("strict");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithIgnoredObject() {
        BoundCreateTable analysis = analyze(
            "create table foo (id integer primary key, details object(ignored))");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");
        Map<String, Object> details = (Map<String, Object>) mappingProperties.get("details");

        assertThat(details.get("type")).isEqualTo("object");
        assertThat(details.get("dynamic")).isEqualTo("false");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithSubscriptInFulltextIndexDefinition() {
        BoundCreateTable analysis = analyze(
            "create table my_table1g (" +
            "   title string, " +
            "   author object(dynamic) as ( " +
            "   name string, " +
            "   birthday timestamp with time zone" +
            "), " +
            "INDEX author_title_ft using fulltext(title, author['name']))");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");
        assertThat((Map<String, Object>) mappingProperties.get("author_title_ft"))
            .containsEntry("sources", List.of("title", "author.name"));
    }

    @Test
    public void test_create_table_index_definition_cannot_contain_same_column() {
        assertThatThrownBy(() -> analyze(
            """
                create table test (
                   title string,
                   name string, INDEX test_ft using fulltext(title, title)
                )"""))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Index test_ft contains duplicate columns");

        // sub-column
        assertThatThrownBy(() -> analyze(
            """
                create table my_table1g (
                   title string,
                   author object(dynamic) as (name string),
                   INDEX nested_ft using fulltext(author['name'], author['name'])
                )"""))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Index nested_ft contains duplicate columns");
    }

    @Test
    public void testCreateTableWithInvalidFulltextIndexDefinition() {
        assertThatThrownBy(() -> analyze(
            """
                create table my_table1g (
                   title string,
                   author object(dynamic) as (name string, birthday timestamp with time zone),
                   INDEX author_title_ft using fulltext(title, author['name']['foo']['bla']))
                """))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessage("Column author['name']['foo']['bla'] unknown");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithArray() {
        BoundCreateTable analysis = analyze(
            "create table foo (id integer primary key, details array(string), more_details text[])");
        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");
        Map<String, Object> details = (Map<String, Object>) mappingProperties.get("details");
        assertThat(details.get("type")).isEqualTo("array");
        Map<String, Object> inner = (Map<String, Object>) details.get("inner");
        assertThat(inner.get("type")).isEqualTo("keyword");

        Map<String, Object> moreDetails = (Map<String, Object>) mappingProperties.get("more_details");
        assertThat(moreDetails.get("type")).isEqualTo("array");
        Map<String, Object> moreDetailsInner = (Map<String, Object>) details.get("inner");
        assertThat(moreDetailsInner.get("type")).isEqualTo("keyword");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithObjectsArray() {
        BoundCreateTable analysis = analyze(
            "create table foo (id integer primary key, details array(object as (name string, age integer, tags array(string))))");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");
        assertThat(mappingProperties).isEqualTo(Map.of(
            "details", Map.of(
                "type", "array",
                "inner", Map.of(
                    "dynamic", "true",
                    "position", 2,
                    "type", "object",
                    "properties", Map.of(
                        "age", Map.of(
                            "position", 4,
                            "type", "integer"
                        ),
                        "name", Map.of(
                            "position", 3,
                            "type", "keyword"
                        ),
                        "tags", Map.of(
                            "inner", Map.of(
                                "position", 5,
                                "type", "keyword"
                            ),
                            "type", "array"
                        )
                    )
                )
            ),
            "id", Map.of(
                "position", 1,
                "type", "integer"
            )
        ));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithAnalyzer() {
        BoundCreateTable analysis = analyze(
            "create table foo (id integer primary key, content string INDEX using fulltext with (analyzer='german'))");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");
        Map<String, Object> contentMapping = (Map<String, Object>) mappingProperties.get("content");

        assertThat(contentMapping.get("index")).isNull();
        assertThat(contentMapping.get("analyzer")).isEqualTo("german");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithAnalyzerParameter() {
        BoundCreateTable analysis = analyze(
            "create table foo (id integer primary key, content string INDEX using fulltext with (analyzer=?))",
            "german"
        );

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");
        Map<String, Object> contentMapping = (Map<String, Object>) mappingProperties.get("content");

        assertThat(contentMapping.get("index")).isNull();
        assertThat(contentMapping.get("analyzer")).isEqualTo("german");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void textCreateTableWithCustomAnalyzerInNestedColumn() {
        BoundCreateTable analysis = analyze(
            "create table ft_search (" +
            "\"user\" object (strict) as (" +
            "name string index using fulltext with (analyzer='ft_search') " +
            ")" +
            ")");
        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");
        Map<String, Object> details = (Map<String, Object>) mappingProperties.get("user");
        Map<String, Object> nameMapping = (Map<String, Object>) ((Map<String, Object>) details.get("properties")).get("name");

        assertThat(nameMapping.get("index")).isNull();
        assertThat(nameMapping.get("analyzer")).isEqualTo("ft_search");

        assertThat(analysis.tableParameter().settings().get("search")).isEqualTo("foobar");
    }

    @Test
    public void testCreateTableWithSchemaName() {
        BoundCreateTable analysis =
            analyze("create table something.foo (id integer primary key)");
        RelationName relationName = analysis.tableName();
        assertThat(relationName.schema()).isEqualTo("something");
        assertThat(relationName.name()).isEqualTo("foo");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithIndexColumn() {
        BoundCreateTable analysis = analyze(
            "create table foo (id integer primary key, content string, INDEX content_ft using fulltext (content))");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");
        Map<String, Object> contentMapping = (Map<String, Object>) mappingProperties.get("content");

        assertThat((String) contentMapping.get("index")).isBlank();

        Map<String, Object> ft_mapping = (Map<String, Object>) mappingProperties.get("content_ft");
        assertThat(ft_mapping.get("index")).isNull();
        assertThat(ft_mapping.get("analyzer")).isEqualTo("standard");
        assertThat(ft_mapping.get("sources")).isEqualTo(List.of("content"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithPlainIndexColumn() {
        BoundCreateTable analysis = analyze(
            "create table foo (id integer primary key, content string, INDEX content_ft using plain (content))");
        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");
        Map<String, Object> contentMapping = (Map<String, Object>) mappingProperties.get("content");

        assertThat((String) contentMapping.get("index")).isBlank();

        Map<String, Object> ft_mapping = (Map<String, Object>) mappingProperties.get("content_ft");
        assertThat(ft_mapping.get("index")).isNull();
        assertThat(ft_mapping.get("analyzer")).isEqualTo("keyword");
        assertThat(ft_mapping.get("sources")).isEqualTo(List.of("content"));
    }

    @Test
    public void testCreateTableWithIndexColumnOverNonString() {
        assertThatThrownBy(
            () -> analyze("create table foo (id integer, id2 integer, INDEX id_ft using fulltext (id, id2))"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("INDEX source columns require `string` types. Cannot use `id` (integer) as source for `id_ft`");
    }

    @Test
    public void testCreateTableWithIndexColumnOverNonString2() {
        assertThatThrownBy(
            () -> analyze("create table foo (id integer, name string, INDEX id_ft using fulltext (id, name))"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("INDEX source columns require `string` types. Cannot use `id` (integer) as source for `id_ft`");
    }

    @Test
    public void testChangeNumberOfReplicas() {
        BoundAlterTable analysis =
            analyze("alter table users set (number_of_replicas=2)");

        assertThat(analysis.table().ident().name()).isEqualTo("users");
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey())).isEqualTo("2");
    }

    @Test
    public void testResetNumberOfReplicas() {
        BoundAlterTable analysis =
            analyze("alter table users reset (number_of_replicas)");

        assertThat(analysis.table().ident().name()).isEqualTo("users");
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey())).isEqualTo("0");
        assertThat(analysis.tableParameter().settings().get(AutoExpandReplicas.SETTING.getKey())).isEqualTo("0-1");
    }

    @Test
    public void testAlterTableWithInvalidProperty() {
        assertThatThrownBy(() -> analyze("alter table users set (foobar='2')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid property \"foobar\" passed to [ALTER | CREATE] TABLE statement");
    }

    @Test
    public void testAlterSystemTable() {
        assertThatThrownBy(() -> analyze("alter table sys.shards reset (number_of_replicas)"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"sys.shards\" doesn't support or allow ALTER operations");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithMultiplePrimaryKeys() {
        BoundCreateTable analysis = analyze(
            "create table test (id integer primary key, name string primary key)");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> meta = (Map<String, Object>) mapping.get("_meta");
        List<String> primaryKeys = (List<String>) meta.get("primary_keys");
        assertThat(primaryKeys).hasSize(2);
        assertThat(primaryKeys.get(0)).isEqualTo("id");
        assertThat(primaryKeys.get(1)).isEqualTo("name");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithMultiplePrimaryKeysAndClusteredBy() {
        BoundCreateTable analysis = analyze(
            "create table test (id integer primary key, name string primary key) " +
            "clustered by(name)");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> meta = (Map<String, Object>) mapping.get("_meta");
        List<String> primaryKeys = (List<String>) meta.get("primary_keys");
        assertThat(primaryKeys).hasSize(2);
        assertThat(primaryKeys.get(0)).isEqualTo("id");
        assertThat(primaryKeys.get(1)).isEqualTo("name");

        assertThat(analysis.routingColumn().fqn()).isEqualTo("name");
    }

    @Test
    public void testCreateTableWithObjectAndUnderscoreColumnPrefix() {
        BoundCreateTable analysis = analyze("create table test (o object as (_id integer), name string)");
        Map<ColumnIdent, Reference> columns = analysis.columns();
        assertThat(columns).containsOnlyKeys(
            new ColumnIdent("o"),
            new ColumnIdent("o", "_id"),
            new ColumnIdent("name")
        );
    }

    @Test
    public void testCreateTableWithUnderscoreColumnPrefix() {
        assertThatThrownBy(() -> analyze("create table test (_id integer, name string)"))
            .isExactlyInstanceOf(InvalidColumnNameException.class)
            .hasMessage("\"_id\" conflicts with system column pattern");
    }

    @Test
    public void testCreateTableWithColumnDot() {
        assertThatThrownBy(() -> analyze("create table test (dot.column integer)"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:24: no viable alternative at input 'create table test (dot.column'");
    }

    @Test
    public void testCreateTableIllegalTableName() {
        assertThatThrownBy(() -> analyze("create table \"abc.def\" (id integer primary key, name string)"))
            .isExactlyInstanceOf(InvalidRelationName.class)
            .hasMessage("Relation name \"doc.abc.def\" is invalid.");
    }

    @Test
    public void testHasColumnDefinition() {
        BoundCreateTable analysis = analyze(
            "create table my_table (" +
            "  id integer primary key, " +
            "  name string, " +
            "  indexed string index using fulltext with (analyzer='german')," +
            "  arr array(object as(" +
            "    nested float," +
            "    nested_object object as (id byte)" +
            "  ))," +
            "  obj object as ( content string )," +
            "  index ft using fulltext(name, obj['content']) with (analyzer='standard')" +
            ")");
        assertThat(analysis.columns()).containsKey(ColumnIdent.fromPath("id"));
        assertThat(analysis.columns()).containsKey(ColumnIdent.fromPath("name"));
        assertThat(analysis.columns()).containsKey(ColumnIdent.fromPath("indexed"));
        assertThat(analysis.columns()).containsKey(ColumnIdent.fromPath("arr"));
        assertThat(analysis.columns()).containsKey(ColumnIdent.fromPath("arr.nested"));
        assertThat(analysis.columns()).containsKey(ColumnIdent.fromPath("arr.nested_object.id"));
        assertThat(analysis.columns()).containsKey(ColumnIdent.fromPath("obj"));
        assertThat(analysis.columns()).containsKey(ColumnIdent.fromPath("obj.content"));
        assertThat(analysis.columns()).containsKey(ColumnIdent.fromPath("ft"));
        assertThat(analysis.columns().get(ColumnIdent.fromPath("ft"))).isExactlyInstanceOf(IndexReference.class);

        assertThat(analysis.columns()).doesNotContainKey(ColumnIdent.fromPath("arr.nested.wrong"));
        assertThat(analysis.columns()).doesNotContainKey(ColumnIdent.fromPath("obj.content.ft"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithGeoPoint() {
        BoundCreateTable analyze = analyze(
            """
                create table geo_point_table (
                    id integer primary key,
                    my_point geo_point
                )
                """);
        Map<String, Object> mapping = TestingHelpers.toMapping(analyze);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");
        Map<String, Object> my_point = (Map<String, Object>) mappingProperties.get("my_point");
        assertThat(my_point.get("type")).isEqualTo("geo_point");
    }

    @Test
    public void testClusteredIntoZeroShards() {
        assertThatThrownBy(() -> analyze("""
                                        create table my_table (
                                            id integer,
                                            name string) clustered into 0 shards
                                        """))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("num_shards in CLUSTERED clause must be greater than 0");
    }

    @Test
    public void testClusteredIntoNullShards() {
        // If number of shards is null, use default setting
        BoundCreateTable analysis = analyze(
            "create table t (id int primary key) clustered into null shards");
        assertThat(Integer.parseInt(
                       analysis.tableParameter().settings().get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey())))
            .isGreaterThan(0);
    }

    @Test
    public void testBlobTableClusteredIntoZeroShards() {
        AnalyzedCreateBlobTable blobTable = analyze("create blob table my_table clustered into 0 shards");

        assertThatThrownBy(() ->
                CreateBlobTablePlan.buildSettings(
                    blobTable.createBlobTable(),
                    plannerContext.transactionContext(),
                    plannerContext.nodeContext(),
                    new RowN(),
                    SubQueryResults.EMPTY,
                    new NumberOfShards(clusterService)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("num_shards in CLUSTERED clause must be greater than 0");
    }

    @Test
    public void testBlobTableClusteredIntoNullShards() {
        // If number of shards is null, use default setting
        AnalyzedCreateBlobTable blobTable = analyze("create blob table my_table clustered into null shards");

        Settings settings = CreateBlobTablePlan.buildSettings(
            blobTable.createBlobTable(),
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            new RowN(),
            SubQueryResults.EMPTY,
            new NumberOfShards(clusterService));

        assertThat(Integer.parseInt(settings.get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey())))
            .isGreaterThan(0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEarlyPrimaryKeyConstraint() {
        BoundCreateTable analysis = analyze(
            "create table my_table (" +
            "primary key (id1, id2)," +
            "id1 integer," +
            "id2 long" +
            ")");
        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> meta = (Map<String, Object>) mapping.get("_meta");
        List<String> primaryKeys = (List<String>) meta.get("primary_keys");
        assertThat(primaryKeys).hasSize(2);
        assertThat(primaryKeys).containsExactly("id1", "id2");
    }

    @Test
    public void testPrimaryKeyConstraintNonExistingColumns() {
        assertThatThrownBy(() ->
                analyze("create table my_table (" +
                    "primary key (id1, id2)," +
                    "title string," +
                    "name string" +
                    ")"))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessage("Column id1 unknown");
    }

    @Test
    public void testEarlyIndexDefinition() {
        BoundCreateTable analysis = analyze(
            "create table my_table (" +
            "index ft using fulltext(title, name) with (analyzer='snowball')," +
            "title string," +
            "name string" +
            ")");
        Map<ColumnIdent, Reference> columns = analysis.columns();
        ColumnIdent ft = new ColumnIdent("ft");
        assertThat(columns).containsKey(ft);
        Reference ftRef = columns.get(ft);
        assertThat(ftRef).isExactlyInstanceOf(IndexReference.class);
        assertThat(((IndexReference) ftRef).columns()).satisfiesExactly(
            x -> assertThat(x).isReference().hasName("title"),
            x -> assertThat(x).isReference().hasName("name")
        );
    }

    @Test
    public void testIndexDefinitionNonExistingColumns() {
        assertThatThrownBy(
            () -> analyze("""
                              create table my_table (
                                index ft using fulltext(id1, id2) with (analyzer='snowball'),
                                title string,name string)
                          """))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessage("Column id1 unknown");
    }

    @Test
    public void testAnalyzerOnInvalidType() {
        assertThatThrownBy(
            () -> analyze("create table my_table (x integer INDEX using fulltext with (analyzer='snowball'))"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Can't use an Analyzer on column x because analyzers are only allowed on columns of type " +
                        "\"text\" of the unbound length limit.");
    }

    @Test
    public void createTableNegativeReplicas() {
        assertThatThrownBy(() -> analyze("create table t (id int, name string) with (number_of_replicas=-1)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Failed to parse value [-1] for setting [number_of_replicas] must be >= 0");
    }

    @Test
    public void testCreateTableSameColumn() {
        // Same name, different type.
        assertThatThrownBy(() -> analyze("create table my_table (title string, title integer)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("column \"title\" specified more than once");

        // Same name, same type.
        assertThatThrownBy(() -> analyze("create table my_table (title string, title string)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("column \"title\" specified more than once");
    }


    @Test
    public void testCreateTableWithArrayPrimaryKeyUnsupported() {
        assertThatThrownBy(() -> analyze("create table t (id array(int) primary key)"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot use column \"id\" with type \"integer_array\" as primary key");
    }

    @Test
    public void testCreateTableWithClusteredIntoShardsParameter() {
        BoundCreateTable analysis = analyze(
            "create table t (id int primary key) clustered into ? shards", 2);
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey()))
            .isEqualTo("2");
        analysis = analyze(
            "create table t (id int primary key) clustered into ?::int shards", "21");
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey()))
            .isEqualTo("21");
    }

    @Test
    public void testCreateTableWithClusteredIntoShardsParameterNonNumeric() {
        assertThatThrownBy(
            () -> analyze("create table t (id int primary key) clustered into ? shards", "foo"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("invalid number 'foo'");
    }

    @Test
    public void testCreateTableWithParitionedColumnInClusteredBy() {
        assertThatThrownBy(
            () -> analyze("create table t(id int primary key) partitioned by (id) clustered by (id)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot use CLUSTERED BY column `id` in PARTITIONED BY clause");
    }

    @Test
    public void testCreateTableUsesDefaultSchema() {
        e.setSearchPath("firstSchema", "secondSchema");

        BoundCreateTable analysis = analyze(e, "create table t (id int)");
        assertThat(analysis.tableName().schema()).isEqualTo(e.getSessionSettings().searchPath().currentSchema());
    }

    @Test
    public void testCreateTableWithEmptySchema() {
        assertThatThrownBy(() -> analyze("create table \"\".my_table (id long primary key)"))
            .isExactlyInstanceOf(InvalidSchemaNameException.class)
            .hasMessage("schema name \"\" is invalid.");
    }

    @Test
    public void testCreateTableWithIllegalSchema() {
        assertThatThrownBy(() -> analyze("create table \"with.\".my_table (id long primary key)"))
            .isExactlyInstanceOf(InvalidSchemaNameException.class)
            .hasMessage("schema name \"with.\" is invalid.");
    }

    @Test
    public void testCreateTableWithInvalidColumnName() {
        assertThatThrownBy(() -> analyze("create table my_table (\"_test\" string)"))
            .isExactlyInstanceOf(InvalidColumnNameException.class)
            .hasMessage("\"_test\" conflicts with system column pattern");
    }

    @Test
    public void testExplicitSchemaHasPrecedenceOverDefaultSchema() {
        SQLExecutor e = SQLExecutor.of(clusterService).setSearchPath("hoschi");
        BoundCreateTable statement = analyze(e, "create table foo.bar (x string)");

        // schema from statement must take precedence
        assertThat(statement.tableName().schema()).isEqualTo("foo");
    }

    @Test
    public void testDefaultSchemaIsAddedToTableIdentIfNoExplicitSchemaExistsInTheStatement() {
        SQLExecutor e = SQLExecutor.of(clusterService).setSearchPath("hoschi");
        BoundCreateTable statement = analyze(e, "create table bar (x string)");

        assertThat(statement.tableName().schema()).isEqualTo("hoschi");
    }

    @Test
    public void testChangeReadBlock() {
        BoundAlterTable analysis =
            analyze("alter table users set (\"blocks.read\"=true)");
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_BLOCKS_READ_SETTING.getKey()))
            .isEqualTo("true");
    }

    @Test
    public void testChangeWriteBlock() {
        BoundAlterTable analysis =
            analyze("alter table users set (\"blocks.write\"=true)");
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()))
            .isEqualTo("true");
    }

    @Test
    public void testChangeMetadataBlock() {
        BoundAlterTable analysis =
            analyze("alter table users set (\"blocks.metadata\"=true)");
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_BLOCKS_METADATA_SETTING.getKey()))
            .isEqualTo("true");
    }

    @Test
    public void testChangeReadOnlyBlock() {
        BoundAlterTable analysis =
            analyze("alter table users set (\"blocks.read_only\"=true)");
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey()))
            .isEqualTo("true");
    }

    @Test
    public void testChangeBlockReadOnlyAllowDelete() {
        BoundAlterTable analysis =
            analyze("alter table users set (\"blocks.read_only_allow_delete\"=true)");
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey()))
            .isEqualTo("true");
    }

    @Test
    public void testChangeBlockReadOnlyAllowedDeletePartitionedTable() {
        BoundAlterTable analysis =
            analyze("alter table parted set (\"blocks.read_only_allow_delete\"=true)");
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey()))
            .isEqualTo("true");
    }

    @Test
    public void testChangeFlushThresholdSize() {
        BoundAlterTable analysis =
            analyze("alter table users set (\"translog.flush_threshold_size\"='300b')");
        assertThat(analysis.tableParameter().settings().get(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey()))
            .isEqualTo("300b");
    }

    @Test
    public void testChangeTranslogInterval() {
        BoundAlterTable analysis =
            analyze("alter table users set (\"translog.sync_interval\"='100ms')");
        assertThat(analysis.tableParameter().settings().get(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey()))
            .isEqualTo("100ms");
    }

    @Test
    public void testChangeTranslogDurability() {
        BoundAlterTable analysis =
            analyze("alter table users set (\"translog.durability\"='ASYNC')");
        assertThat(analysis.tableParameter().settings().get(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey()))
            .isEqualTo("ASYNC");
    }

    @Test
    public void testRoutingAllocationEnable() {
        BoundAlterTable analysis =
            analyze("alter table users set (\"routing.allocation.enable\"=\"none\")");
        assertThat(analysis.tableParameter().settings().get(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey()))
            .isEqualTo("none");
    }

    @Test
    public void testRoutingAllocationValidation() {
        assertThatThrownBy(() -> analyze("alter table users set (\"routing.allocation.enable\"=\"foo\")"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Illegal allocation.enable value [FOO]");
    }

    @Test
    public void testAlterTableSetShards() {
        BoundAlterTable analysis =
            analyze("alter table users set (\"number_of_shards\"=1)");
        assertThat(analysis.table().ident().name()).isEqualTo("users");
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey()))
            .isEqualTo("1");
    }

    @Test
    public void testAlterTableResetShards() {
        BoundAlterTable analysis =
            analyze("alter table users reset (\"number_of_shards\")");
        assertThat(analysis.table().ident().name()).isEqualTo("users");
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey()))
            .isEqualTo("5");
    }

    @Test
    public void testTranslogSyncInterval() {
        BoundAlterTable analysis =
            analyze("alter table users set (\"translog.sync_interval\"='1s')");
        assertThat(analysis.table().ident().name()).isEqualTo("users");
        assertThat(analysis.tableParameter().settings().get(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey())).isEqualTo("1s");
    }

    @Test
    public void testAllocationMaxRetriesValidation() {
        BoundAlterTable analysis =
            analyze("alter table users set (\"allocation.max_retries\"=1)");
        assertThat(analysis.tableParameter().settings().get(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey())).isEqualTo("1");
    }

    @Test
    public void testCreateReadOnlyTable() {
        BoundCreateTable analysis = analyze(
            "create table foo (id integer primary key, name string) "
            + "clustered into 3 shards with (\"blocks.read_only\"=true)");
        assertThat(analysis.tableParameter().settings().get(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey())).isEqualTo("true");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableWithGeneratedColumn() {
        BoundCreateTable analysis = analyze(
            "create table foo (" +
            "   ts timestamp with time zone," +
            "   day as date_trunc('day', ts))");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> metaMap = (Map<String, Object>) mapping.get("_meta");

        Map<String, String> generatedColumnsMapping = (Map<String, String>) metaMap.get("generated_columns");
        assertThat(generatedColumnsMapping).hasSize(1);
        assertThat(generatedColumnsMapping.get("day")).isEqualTo("date_trunc('day', ts)");

        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");
        Map<String, Object> dayMapping = (Map<String, Object>) mappingProperties.get("day");
        assertThat(dayMapping.get("type")).isEqualTo("date");
        Map<String, Object> tsMapping = (Map<String, Object>) mappingProperties.get("ts");
        assertThat(tsMapping.get("type")).isEqualTo("date");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithColumnOfArrayTypeAndGeneratedExpression() {
        BoundCreateTable analysis = analyze(
            "create table foo (arr array(integer) as ([1.0, 2.0]))");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");

        assertThat(mappingProperties).isEqualTo(Map.of(
            "arr", Map.of(
                "inner", Map.of(
                    "position", 1,
                    "type", "integer"
                ),
                "type", "array"
            )
        ));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableGeneratedColumnWithCast() {
        BoundCreateTable analysis = analyze(
            "create table foo (" +
            "   ts timestamp with time zone," +
            "   day timestamp with time zone GENERATED ALWAYS as ts + 1)");
        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> metaMapping = (Map<String, Object>) mapping.get("_meta");

        Map<String, String> generatedColumnsMapping = (Map<String, String>) metaMapping.get("generated_columns");
        assertThat(generatedColumnsMapping.get("day")).isEqualTo("(ts + 1::bigint)");

        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");
        Map<String, Object> dayMapping = (Map<String, Object>) mappingProperties.get("day");
        assertThat(dayMapping.get("type")).isEqualTo("date");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithCurrentTimestampAsGeneratedColumnIsntNormalized() {
        BoundCreateTable analysis = analyze(
            "create table foo (ts timestamp with time zone GENERATED ALWAYS as current_timestamp(3))");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> metaMapping = (Map<String, Object>) mapping.get("_meta");

        Map<String, String> generatedColumnsMapping = (Map<String, String>) metaMapping.get("generated_columns");
        assertThat(generatedColumnsMapping).hasSize(1);
        // current_timestamp used to get evaluated and then this contained the actual timestamp instead of the function name
        assertThat(generatedColumnsMapping.get("ts")).isEqualTo("current_timestamp(3)");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableGeneratedColumnWithSubscript() {
        BoundCreateTable analysis = analyze(
            "create table foo (\"user\" object as (name string), name as concat(\"user\"['name'], 'foo'))");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> metaMapping = (Map<String, Object>) mapping.get("_meta");

        Map<String, String> generatedColumnsMapping = (Map<String, String>) metaMapping.get("generated_columns");
        assertThat(generatedColumnsMapping.get("name")).isEqualTo("concat(\"user\"['name'], 'foo')");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateTableGeneratedColumnParameter() {
        BoundCreateTable analysis = analyze(
            "create table foo (\"user\" object as (name string), name as concat(\"user\"['name'], ?))", $("foo"));
        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> metaMapping = (Map<String, Object>) mapping.get("_meta");

        Map<String, String> generatedColumnsMapping = (Map<String, String>) metaMapping.get("generated_columns");
        assertThat(generatedColumnsMapping.get("name")).isEqualTo("concat(\"user\"['name'], 'foo')");
    }

    @Test
    public void testCreateTableGeneratedColumnWithInvalidType() {
        assertThatThrownBy(() -> analyze("create table foo (" +
                                         "   ts timestamp with time zone," +
                                         "   day ip GENERATED ALWAYS as date_trunc('day', ts))"))
            .isExactlyInstanceOf(ConversionException.class)
            .hasMessage("Cannot cast expressions from type `timestamp with time zone` to type `ip`");
    }

    @Test
    public void testCreateTableGeneratedColumnWithMatch() {
        assertThatThrownBy(() -> analyze("create table foo (name string, bar as match(name, 'crate'))"))
            .isExactlyInstanceOf(UnsupportedFeatureException.class)
            .hasMessage("Cannot use MATCH in CREATE TABLE statements");
    }

    @Test
    public void testCreateTableGeneratedColumnBasedOnGeneratedColumn() {
        assertThatThrownBy(
            () -> analyze(
                """
                create table foo (
                    ts timestamp with time zone,
                    day as date_trunc('day', ts),
                    date_string as cast(day as string)
                )
                    """))
            .isExactlyInstanceOf(ColumnValidationException.class)
            .hasMessage("Validation failed for date_string: Generated column cannot be based on generated column `day`");
    }

    @Test
    public void testCreateTableGeneratedColumnBasedOnUnknownColumn() {
        assertThatThrownBy(() -> analyze(
                                    "create table foo (" +
                                         "   ts timestamp with time zone," +
                                         "   day as date_trunc('day', ts)," +
                                         "   date_string as cast(unknown_col as string))"))
            .isExactlyInstanceOf(ColumnUnknownException.class)
            .hasMessage("Column unknown_col unknown");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCreateTableWithDefaultExpressionLiteral() {
        BoundCreateTable analysis = analyze(
            "create table foo (name text default 'bar')");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = (Map<String, Object>) mapping.get("properties");

        assertThat(mappingProperties).isEqualTo(Map.of(
            "name", Map.of(
                "default_expr", "'bar'",
                "position", 1,
                "type", "keyword"
            )
        ));
    }

    @Test
    public void testCreateTableWithDefaultExpressionFunction() {
        BoundCreateTable analysis = analyze(
            "create table foo (name text default upper('bar'))");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = Maps.get(mapping, "properties");

        assertThat(mappingProperties).isEqualTo(Map.of(
            "name", Map.of(
                "default_expr", "'BAR'",
                "position", 1,
                "type", "keyword"
            )
        ));
    }

    @Test
    public void testCreateTableWithDefaultExpressionWithCast() {
        BoundCreateTable analysis = analyze(
            "create table foo (id int default 3.5)");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = Maps.get(mapping, "properties");

        assertThat(mappingProperties).isEqualTo(Map.of(
            "id", Map.of(
                "default_expr", "3.5",
                "position", 1,
                "type", "integer"
            )
        ));
    }

    @Test
    public void testCreateTableWithDefaultExpressionIsNotNormalized() {
        BoundCreateTable analysis = analyze(
            "create table foo (ts timestamp with time zone default current_timestamp(3))");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = Maps.get(mapping, "properties");
        assertThat(mappingProperties).isEqualTo(Map.of(
            "ts", Map.of(
                "default_expr", "current_timestamp(3)",
                "format", "epoch_millis||strict_date_optional_time",
                "position", 1,
                "type", "date"
            )
        ));
    }

    @Test
    public void testCreateTableWithDefaultExpressionAsCompoundTypes() {
        BoundCreateTable analysis = analyze(
            "create table foo (" +
            "   arr array(long) default [1, 2])");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = Maps.get(mapping, "properties");

        assertThat(mappingProperties).isEqualTo(Map.of(
            "arr", Map.of(
                "inner", Map.of(
                    "default_expr", "[1, 2]",
                    "position", 1,
                    "type", "long"
                ),
                "type", "array"
            )
        ));
    }

    @Test
    public void testCreateTableWithDefaultExpressionAsGeoTypes() {
        BoundCreateTable analysis = analyze(
            "create table foo (" +
            "   p geo_point default [0,0]," +
            "   s geo_shape default 'LINESTRING (0 0, 1 1)')");

        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, Object> mappingProperties = Maps.get(mapping, "properties");

        assertThat(mappingProperties).isEqualTo(Map.of(
            "p", Map.of(
                "default_expr", "[0, 0]",
                "position", 1,
                "type", "geo_point"
            ),
            "s", Map.of(
                "default_expr", "'LINESTRING (0 0, 1 1)'",
                "position", 2,
                "tree", "geohash",
                "type", "geo_shape"
            )
        ));
    }

    @Test
    public void test_object_cols_with_default_value_not_allowed() {
        assertThatThrownBy(() -> analyze("""
                                             create table foo (
                                                obj object as (key text) default {key=''}
                                             )"""))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Default values are not allowed for object columns: obj");
    }

    @Test
    public void testCreateTableWithDefaultExpressionRefToColumnsNotAllowed() {
        assertThatThrownBy(() -> analyze("create table foo (name text, name_def text default upper(name))"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage(
                "Cannot reference columns in DEFAULT expression of `name_def`. "
                + "Maybe you wanted to use a string literal with single quotes instead: 'name'");
    }

    @Test
    public void testCreateTableWithObjectAsPrimaryKey() {
        assertThatThrownBy(() -> analyze("create table t (obj object as (x int) primary key)"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot use columns of type \"object\" as primary key");
    }

    @Test
    public void testCreateTableWithGeoPointAsPrimaryKey() {
        assertThatThrownBy(() -> analyze("create table t (c geo_point primary key)"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot use columns of type \"geo_point\" as primary key");
    }

    @Test
    public void testCreateTableWithGeoShapeAsPrimaryKey() {
        assertThatThrownBy(() -> analyze("create table t (c geo_shape primary key)"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot use columns of type \"geo_shape\" as primary key");
    }

    @Test
    public void testCreateTableWithDuplicatePrimaryKey() {
        assertDuplicatePrimaryKey("create table t (id int, primary key (id, id))");
        assertDuplicatePrimaryKey("create table t (obj object as (id int), primary key (obj['id'], obj['id']))");
        assertDuplicatePrimaryKey("create table t (id int primary key, primary key (id))");
        assertDuplicatePrimaryKey("create table t (obj object as (id int primary key), primary key (obj['id']))");
    }

    private void assertDuplicatePrimaryKey(String stmt) {
        assertThatThrownBy(() -> analyze(stmt))
            .as(String.format(Locale.ENGLISH, "Statement '%s' did not result in duplicate primary key exception", stmt))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("appears twice in primary key constraint");
    }

    @Test
    public void test_alter_table_add_generated_column_based_on_generated_column() throws IOException {
        SQLExecutor.of(clusterService)
            .addTable("CREATE TABLE tbl (col1 INT, col2 INT GENERATED ALWAYS AS col1*2)");
        assertThatThrownBy(
            () -> {
                AnalyzedAlterTableAddColumn analyze = analyze(
                    """
                        ALTER TABLE tbl
                            ADD COLUMN col3 INT GENERATED ALWAYS AS col2+1
                    """);
                analyze.bind(e.nodeCtx, CoordinatorTxnCtx.systemTransactionContext(), Row.EMPTY, SubQueryResults.EMPTY);
            })
            .isExactlyInstanceOf(ColumnValidationException.class)
            .hasMessage("Validation failed for col3: Generated column cannot be based on generated column `col2`");
    }

    @Test
    public void test_create_table_with_check_constraint_on_generated_column() {
        BoundCreateTable analysis = analyze(
            """
                CREATE TABLE foo (
                    col1 INT,
                    col2 INT GENERATED ALWAYS AS col1*2 CONSTRAINT check_col2_ge_zero CHECK (col2 > 0))
            """);
        Map<String, Object> mapping = TestingHelpers.toMapping(analysis);
        Map<String, String> checkConstraints = analysis.getCheckConstraints();
        assertThat(checkConstraints).hasSize(1);
        assertThat(checkConstraints.get("check_col2_ge_zero")).isEqualTo(
                     Maps.getByPath(mapping, Arrays.asList("_meta", "check_constraints", "check_col2_ge_zero")));
    }


    @Test
    public void testCreateTableWithPrimaryKeyConstraintInArrayItem() {
        assertThatThrownBy(() -> analyze("create table test (arr array(object as (id long primary key)))"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot use column \"id\" as primary key within an array object");
    }

    @Test
    public void testCreateTableWithDeepNestedPrimaryKeyConstraintInArrayItem() {
        assertThatThrownBy(
            () -> analyze("create table test (arr array(object as (\"user\" object as (name string primary key), id long)))"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot use column \"name\" as primary key within an array object");
    }

    @Test
    public void testCreateTableWithInvalidIndexConstraint() {
        assertThatThrownBy(() -> analyze("create table test (obj object index off)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("INDEX constraint cannot be used on columns of type \"object\": `obj`");
    }

    @Test
    public void test_check_constraint_on_nested_object_sub_column_has_correct_type_and_expression() {
        BoundCreateTable createTable = analyze(
            "create table t (i int, o1 object as (o2 object as (b int check (o1['o2']['b'] > 100))))");

        ColumnIdent o2b = new ColumnIdent("o1", List.of("o2", "b"));
        Map<ColumnIdent, Reference> columns = createTable.columns();
        assertThat(columns).containsKey(o2b);
        assertThat(columns.get(o2b)).isReference().hasName("o1['o2']['b']").hasType(DataTypes.INTEGER);
        assertThat(createTable.checks()).hasSize(1);
        AnalyzedCheck check = createTable.checks().values().iterator().next();
        assertThat(check.check()).isSQL("(doc.t.o1['o2']['b'] > 100)");
        assertThat(check.check()).isFunction("op_>", List.of(DataTypes.INTEGER, DataTypes.INTEGER));
    }

    @Test
    public void test_check_constraint_cannot_be_added_to_nested_object_sub_column_without_full_path() {
        assertThatThrownBy(
            () -> analyze("create table t (i int, o1 object as (o2 object as (b text check (b != 'foo'))))"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("CHECK constraint on column `o1['o2']['b']` cannot refer to column `b`. Use full path to " +
                        "refer to a sub-column or a table check constraint instead");
    }

    @Test
    public void test_create_table_with_column_store_disabled() {
        for (var dataType : DataTypes.PRIMITIVE_TYPES) {
            var stmt = "create table columnstore_disabled (s " + dataType + " STORAGE WITH (columnstore = false))";

            if (dataType.storageSupport() != null && dataType.storageSupport().supportsDocValuesOff()) {
                BoundCreateTable analysis = analyze(stmt);
                var mapping = TestingHelpers.toMapping(analysis);
                Map<String, Object> mappingProperties = Maps.get(mapping, "properties");
                Map<String, Object> sProperties = Maps.get(mappingProperties, "s");
                assertThat(sProperties)
                    .containsEntry("doc_values", "false")
                    .containsEntry("position", 1)
                    .containsEntry("type", DataTypes.esMappingNameFrom(dataType.id()));
            } else if (dataType.storageSupport() != null) {
                assertThatThrownBy(() -> analyze(stmt))
                    .isExactlyInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid storage option \"columnstore\" for data type \"" + dataType.getName() + "\" for column: s");
            }
        }
    }

    @Test
    public void testGeneratedColumnInsideObjectIsProcessed() {
        BoundCreateTable stmt = analyze("create table t (obj object as (c as 1 + 1))");
        Reference reference = stmt.columns().get(new ColumnIdent("obj", "c"));

        assertThat(reference.valueType()).isEqualTo(DataTypes.INTEGER);
        assertThat(((GeneratedReference) reference).formattedGeneratedExpression()).isEqualTo("2");
        assertThat(TestingHelpers.toMapping(stmt).toString())
                   .isEqualTo("{_meta={generated_columns={obj.c=2}}, dynamic=strict, " +
                      "properties={obj={dynamic=true, position=1, type=object, properties={c={position=2, type=integer}}}}}");
    }

    @Test
    public void testNumberOfRoutingShardsCanBeSetAtCreateTable() {
        BoundCreateTable stmt = analyze(
            """
                    create table t (x int)
                    clustered into 2 shards
                    with (number_of_routing_shards = 10)
                """);
        assertThat(stmt.tableParameter().settings().get("index.number_of_routing_shards")).isEqualTo("10");
    }

    @Test
    public void testNumberOfRoutingShardsCanBeSetAtCreateTableForPartitionedTables() {
        BoundCreateTable stmt = analyze(
            "create table t (p int, x int) clustered into 2 shards partitioned by (p) " +
            "with (number_of_routing_shards = 10)");
        assertThat(stmt.tableParameter().settings().get("index.number_of_routing_shards")).isEqualTo("10");
    }

    @Test
    public void testAlterTableSetDynamicSetting() {
        BoundAlterTable analysis =
            analyze("alter table users set (\"routing.allocation.exclude.foo\"='bar')");
        assertThat(analysis.tableParameter().settings().get(INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "foo")).isEqualTo("bar");
    }

    @Test
    public void test_alter_table_dynamic_setting_on_closed_table() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table doc.test(i int)")
            .closeTable("test");
        BoundAlterTable analysis = analyze(e, "alter table test set (\"routing.allocation.exclude.foo\"='bar')");
        assertThat(analysis.tableParameter().settings().get(INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "foo")).isEqualTo("bar");
    }

    @Test
    public void test_alter_table_non_dynamic_setting_on_closed_table() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table doc.test(i int)")
            .closeTable("test");
        BoundAlterTable analysis = analyze(e, "ALTER TABLE test SET (codec = 'best_compression')");
        assertThat(analysis.tableParameter().settings().get(INDEX_CODEC_SETTING.getKey())).isEqualTo("best_compression");
    }

    @Test
    public void test_alter_table_update_final_setting_on_open_table() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table doc.test(i int)");
        Asserts.assertSQLError(() -> analyze(e, "alter table test SET (\"store.type\" = 'simplefs')"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(INTERNAL_SERVER_ERROR, 5000)
            .hasMessageContaining("Invalid property \"store.type\" passed to [ALTER | CREATE] TABLE statement");
    }

    @Test
    public void test_alter_table_update_final_setting_on_closed_table() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable("create table doc.test(i int)")
            .closeTable("test");
        Asserts.assertSQLError(() -> analyze(e, "alter table test SET (number_of_routing_shards = 5)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(INTERNAL_SERVER_ERROR, 5000)
            .hasMessageContaining("Invalid property \"number_of_routing_shards\" passed to [ALTER | CREATE] TABLE statement");
    }

    @Test
    public void testAlterTableResetDynamicSetting() {
        BoundAlterTable analysis =
            analyze("alter table users reset (\"routing.allocation.exclude.foo\")");
        assertThat(analysis.tableParameter().settings().get(INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "foo"))
            .isNull();
    }

    @Test
    public void testCreateTableWithIntervalFails() {
        assertThatThrownBy(() -> analyze("create table test (i interval)"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Type `interval` does not support storage");
    }

    @Test
    public void test_character_varying_type_can_be_used_in_create_table() throws Exception {
        BoundCreateTable stmt = analyze("create table tbl (name character varying)");

        Map<String, Object> mapping = TestingHelpers.toMapping(stmt);
        Map<String, Object> mappingProperties = Maps.get(mapping, "properties");

        assertThat(mappingProperties).isEqualTo(Map.of(
            "name", Map.of(
                "position", 1,
                "type", "keyword"
            )
        ));
    }

    @Test
    public void test_create_table_with_varchar_column_of_limited_length() {
        BoundCreateTable stmt = analyze("CREATE TABLE tbl (name character varying(2))");

        Map<String, Object> mapping = TestingHelpers.toMapping(stmt);
        Map<String, Object> mappingProperties = Maps.get(mapping, "properties");

        assertThat(mappingProperties).isEqualTo(Map.of(
            "name", Map.of(
                "length_limit", 2,
                "position", 1,
                "type", "keyword"
            )
        ));
    }

    @Test
    public void test_create_table_with_varchar_column_of_limited_length_with_analyzer_throws_exception() {
        assertThatThrownBy(
            () -> analyze("CREATE TABLE tbl (name varchar(2) INDEX using fulltext WITH (analyzer='german'))"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Can't use an Analyzer on column name because analyzers are only allowed on columns " +
                        "of type \"" + DataTypes.STRING.getName() + "\" of the unbound length limit.");
    }

    @Test
    public void test_oidvector_cannot_be_used_in_create_table() throws Exception {
        assertThatThrownBy(() -> analyze("CREATE TABLE tbl (x oidvector)"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Type `oidvector` does not support storage");
    }

    @Test
    public void test_generated_column_arguments_are_detected_as_array_and_validation_fails_with_missing_overload() throws Exception {
        assertThatThrownBy(() -> analyze("CREATE TABLE tbl (xs int[], x as max(xs))"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: max(doc.tbl.xs), no overload found for matching argument types: (integer_array)");
    }

    @Test
    public void test_prohibit_using_aggregations_in_generated_columns() throws Exception {
        assertThatThrownBy(() -> analyze("CREATE TABLE tbl (x int, y as max(x))"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Aggregation functions are not allowed in generated columns: max(x)");
    }

    @Test
    public void test_can_use_bit_type_in_create_table_statement() throws Exception {
        BoundCreateTable stmt = analyze("CREATE TABLE tbl (xs bit(20))");

        Map<String, Object> mapping = TestingHelpers.toMapping(stmt);
        Map<String, Object> mappingProperties = Maps.get(mapping, "properties");

        assertThat(mappingProperties).isEqualTo(Map.of(
            "xs", Map.of(
                "length", 20,
                "position", 1,
                "type", "bit"
            )
        ));
    }

    @Test
    public void test_bit_type_defaults_to_length_1() throws Exception {
        BoundCreateTable stmt = analyze("CREATE TABLE tbl (xs bit)");

        Map<String, Object> mapping = TestingHelpers.toMapping(stmt);
        Map<String, Object> mappingProperties = Maps.get(mapping, "properties");

        assertThat(mappingProperties).isEqualTo(Map.of(
            "xs", Map.of(
                "length", 1,
                "position", 1,
                "type", "bit"
            )
        ));
    }

    @Test
    public void test_can_use_character_type_in_create_table_statement() {
        BoundCreateTable stmt = analyze("CREATE TABLE tbl (c character(10))");

        Map<String, Object> mapping = TestingHelpers.toMapping(stmt);
        Map<String, Object> mappingProperties = Maps.get(mapping, "properties");

        assertThat(mappingProperties).isEqualTo(Map.of(
            "c", Map.of(
                "blank_padding", true,
                "length_limit", 10,
                "position", 1,
                "type", "keyword"
            )
        ));
    }

    @Test
    public void test_character_type_defaults_to_length_1() throws Exception {
        BoundCreateTable stmt = analyze("CREATE TABLE tbl (c character)");

        Map<String, Object> mapping = TestingHelpers.toMapping(stmt);
        Map<String, Object> mappingProperties = Maps.get(mapping, "properties");

        assertThat(mappingProperties).isEqualTo(Map.of(
            "c", Map.of(
                "blank_padding", true,
                "length_limit", 1,
                "position", 1,
                "type", "keyword"
            ))
        );
    }

    @Test
    public void test_char_is_alias_for_character_type() throws Exception {
        BoundCreateTable stmt = analyze("CREATE TABLE tbl (c char)");

        Map<String, Object> mapping = TestingHelpers.toMapping(stmt);
        Map<String, Object> mappingProperties = Maps.get(mapping, "properties");

        assertThat(mappingProperties).isEqualTo(Map.of(
            "c", Map.of(
                "blank_padding", true,
                "length_limit", 1,
                "position", 1,
                "type", "keyword"
            )
        ));
    }

    @Test
    public void test_now_function_is_not_normalized_to_literal_in_create_table() throws Exception {
        BoundCreateTable stmt = analyze("create table tbl (ts timestamp with time zone default now())");

        Map<String, Object> mapping = TestingHelpers.toMapping(stmt);
        Map<String, Object> mappingProperties = Maps.get(mapping, "properties");

        assertThat(mappingProperties)
            .containsKey("ts")
            .extractingByKey("ts", InstanceOfAssertFactories.MAP)
            .containsEntry("default_expr", "now()");
    }

    @Test
    public void test_current_user_function_is_not_normalized_to_literal_in_create_table() throws Exception {
        BoundCreateTable stmt = analyze("create table tbl (user_name text default current_user)");

        Map<String, Object> mapping = TestingHelpers.toMapping(stmt);
        Map<String, Object> mappingProperties = Maps.get(mapping, "properties");

        assertThat(mappingProperties)
            .containsKey("user_name")
            .extractingByKey("user_name", InstanceOfAssertFactories.MAP)
                .containsEntry("default_expr", "CURRENT_USER")
                .containsEntry("position", 1)
                .containsEntry("type", "keyword");
    }

    @Test
    public void test_create_table_with_invalid_storage_option_errors_with_invalid_property_name() throws Exception {
        assertThatThrownBy(() -> analyze("create table tbl (name text storage with (foobar = true))"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid STORAGE WITH option `foobar`");
    }

    @Test
    public void test_create_table_validates_null_property() {
        assertThatThrownBy(() -> analyze("CREATE TABLE tbl (name text) WITH (number_of_replicas = NULL)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot set NULL to property number_of_replicas.");
    }

    @Test
    public void test_alter_table_set_mapping_validates_null_property() {
        // column_policy on table is the only property which is handled not like a setting but like mapping, needs separate test.
        assertThatThrownBy(() -> analyze("ALTER TABLE users SET (column_policy = null)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot set NULL to property column_policy.");
    }

    @Test
    public void test_alter_table_set_setting_validates_null_property() {
        assertThatThrownBy(() -> analyze("ALTER TABLE users SET (refresh_interval = null)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot set NULL to property refresh_interval.");
    }

    @Test
    public void test_create_nested_array_column() throws Exception {
        BoundCreateTable createTable = analyze("create table tbl (x int[][])");
        ColumnIdent x = new ColumnIdent("x");
        Map<ColumnIdent, Reference> columns = createTable.columns();
        assertThat(columns).containsKeys(x);
        Reference xRef = columns.get(x);
        assertThat(xRef).isReference().hasName("x").hasType(new ArrayType<>(new ArrayType<>(DataTypes.INTEGER)));
    }


    @Test
    public void test_can_use_vector_in_create_table() throws Exception {
        BoundCreateTable stmt = analyze("create table tbl (x float_vector)");
        assertThat(stmt.columns()).hasEntrySatisfying(
            new ColumnIdent("x"), Asserts.isReference("x", FloatVectorType.INSTANCE_ONE)
        );
    }

    @Test
    public void test_cannot_use_arrays_of_float_vector() throws Exception {
        assertThatThrownBy(() -> analyze("create table tbl (xs array(float_vector))"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Arrays of float_vector are not supported");
    }

    @Test
    public void test_named_primary_key_constraints() {
        BoundCreateTable createTable = analyze("create table tbl (a int constraint c_1 check (a > 10) constraint c_2 primary key constraint c_3 check (a < 20))");
        assertThat(createTable.pkConstraintName()).isEqualTo("c_2");

        analyze("create table tbl (a int constraint c_1 check (a > 10) constraint c_3 check (a < 20), constraint c_2 primary key (a))");
        assertThat(createTable.pkConstraintName()).isEqualTo("c_2");
    }

    @Test
    public void test_cannot_define_more_than_one_name_for_primary_key_constraint() {
        assertThatThrownBy(() -> analyze("create table tbl (a int constraint c_1 primary key, b int constraint c_2 primary key)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("More than one name for PRIMARY KEY constraint provided: c_1,c_2");

        assertThatThrownBy(() -> analyze("create table tbl (a int primary key, b int constraint c_2 primary key)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("More than one name for PRIMARY KEY constraint provided: null,c_2");
    }

    @Test
    public void test_empty_string_cannot_be_name_for_primary_key_constraints() {
        assertThatThrownBy(() -> analyze("create table tbl (a int constraint \"\" primary key)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The name of primary key constraint must not be empty, please either use a name or remove the CONSTRAINT keyword");
    }

    @Test
    public void test_cannot_use_table_functions_in_generated_columns() throws Exception {
        assertThatThrownBy(() -> analyze("create table tbl (x text, y text[] as regexp_matches(x, '(a(.+)z)'))"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot use table function in generated expression of column `y`");
    }

    @Test
    public void test_cannot_use_table_function_in_default_expression() throws Exception {
        assertThatThrownBy(() -> analyze("create table tbl (x int default generate_series(1, 10))"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Cannot use table function in default expression of column `x`");
    }
}
