package io.crate.analyze.relations;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.crate.expression.symbol.format.Style;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class RelationFormatterTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_format_simple_select() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int)")
            .build();

        AnalyzedRelation stmt = e.analyze("select * from tbl where x = 10");
        assertThat(RelationFormatter.format(stmt, Style.QUALIFIED)).isEqualTo(
            """
            SELECT
                doc.tbl.x,
                doc.tbl.y
            FROM
                doc.tbl
            WHERE
                (doc.tbl.x = 10)
            """
        );
    }

    @Test
    public void test_format_select_group_by_having() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int)")
            .build();

        AnalyzedRelation stmt = e.analyze("select * from tbl");
        assertThat(RelationFormatter.format(stmt, Style.QUALIFIED)).isEqualTo(
            """
            SELECT
                doc.tbl.x,
                doc.tbl.y
            FROM
                doc.tbl
            """
        );
    }
}

