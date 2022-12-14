
package io.crate.execution.dml.upsert;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import io.crate.execution.dml.IndexItem;
import io.crate.expression.reference.Doc;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class UpdateToInsertTest extends CrateDummyClusterServiceUnitTest {

    private static Doc doc(String index, Map<String, Object> source) {
        Supplier<String> rawSource = () -> {
            try {
                return Strings.toString(XContentFactory.jsonBuilder().map(source));
            } catch (IOException e1) {
                throw new RuntimeException(e1);
            }
        };
        return new Doc(1, index, "id-1", 1, 1, 1, source, rawSource);
    }

    @Test
    public void test_update_one_column_generates_all_insert_values() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int)")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            new String[] { "y" }
        );
        Map<String, Object> source = Map.of("x", 10, "y", 5);
        Doc doc = doc(table.concreteIndices()[0], source);

        IndexItem item = updateToInsert.convert(
            doc,
            new Symbol[] { Literal.of(20) },
            new Object[0]
        );
        assertThat(item.insertValues())
            .containsExactly(10, 20);
    }

    @Test
    public void test_update_can_use_excluded_columns() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int)")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            new String[] { "y" }
        );
        Map<String, Object> source = Map.of("x", 10, "y", 5);
        Doc doc = doc(table.concreteIndices()[0], source);

        IndexItem item = updateToInsert.convert(
            doc,
            new Symbol[] { new InputColumn(0) },
            new Object[] { 20 }
        );
        assertThat(item.insertValues())
            .containsExactly(10, 20);
    }

    @Test
    public void test_can_assign_value_to_object_child() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, o object as (y int))")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            new String[] { "o.y" }
        );
        Map<String, Object> source = Map.of("x", 1, "o", Map.of("y", 2));
        Doc doc = doc(table.concreteIndices()[0], source);
        IndexItem item = updateToInsert.convert(
            doc,
            new Symbol[] { Literal.of(3) },
            new Object[] {}
        );
        assertThat(item.insertValues())
            .containsExactly(1, Map.of("y", 3));
    }

    @Test
    public void test_generated_columns_are_updated_using_new_values() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int as x + 4)")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            new String[] { "x" }
        );
        Map<String, Object> source = Map.of("x", 1, "y", 5);
        Doc doc = doc(table.concreteIndices()[0], source);
        IndexItem item = updateToInsert.convert(
            doc,
            new Symbol[] { Literal.of(8) },
            new Object[] {}
        );
        assertThat(item.insertValues())
            .containsExactly(8, 12);
    }

    @Test
    public void test_checks_are_ignored() throws Exception {
        /**
         * Checks can be ignored because the index operation afterwards will run them.
         **/
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int check (x > 10))")
            .build();
        DocTableInfo table = e.resolveTableInfo("tbl");
        UpdateToInsert updateToInsert = new UpdateToInsert(
            e.nodeCtx,
            new CoordinatorTxnCtx(e.getSessionSettings()),
            table,
            new String[] { "x" }
        );
        Map<String, Object> source = Map.of("x", 12);
        Doc doc = doc(table.concreteIndices()[0], source);

        Symbol[] assignments = new Symbol[] { Literal.of(8) };
        IndexItem item = updateToInsert.convert(doc, assignments, new Object[0]);
        assertThat(item.insertValues())
            .containsExactly(8);
    }
}
