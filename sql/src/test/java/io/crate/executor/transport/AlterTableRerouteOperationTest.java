package io.crate.executor.transport;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.RerouteMoveShardAnalyzedStatement;
import io.crate.analyze.TableDefinitions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Routing;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TestingTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.core.Is.is;

public class AlterTableRerouteOperationTest extends CrateUnitTest {

    private static final Routing PARTED_ROUTING = new Routing(
        ImmutableMap.of(CrateDummyClusterServiceUnitTest.NODE_ID,
            ImmutableMap.of("parted", Collections.singletonList(0))));

    private static final Routing TABLE_ROUTING = new Routing(
        ImmutableMap.of(CrateDummyClusterServiceUnitTest.NODE_ID,
            ImmutableMap.of("my_table", Collections.singletonList(0))));


    public static final DocTableInfo PARTED_TABLE_INFO =
        new TestingTableInfo.Builder(new TableIdent(Schemas.DOC_SCHEMA_NAME, "parted"), PARTED_ROUTING)
            .add("a", DataTypes.STRING, null, true)
            .build();

    public static final DocTableInfo TABLE_INFO =
        new TestingTableInfo.Builder(new TableIdent(Schemas.DOC_SCHEMA_NAME, "my_table"), TABLE_ROUTING)
            .add("a", DataTypes.STRING)
            .build();

    public static final BlobTableInfo BLOB_TABLE_INFO = TableDefinitions.createBlobTable(
        new TableIdent(BlobSchemaInfo.NAME, "screenshots"));

    @Test
    public void testRerouteIndexOfBlobIndex() throws Exception {
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            BLOB_TABLE_INFO,
            null,
            0,
            "node1",
            "node2"
        );
        String index = AlterTableOperation.getRerouteIndex(statement);
        assertThat(index, is(".blob"));
    }

    @Test
    public void testRerouteIndexOfPartitionedIndex() throws Exception {
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            PARTED_TABLE_INFO,
            new PartitionName(PARTED_TABLE_INFO.ident().schema(), PARTED_TABLE_INFO.ident().indexName(), ImmutableList.of(new BytesRef("a"))),
            0,
            "node1",
            "node2"
        );
        String index = AlterTableOperation.getRerouteIndex(statement);
        assertThat(index, is(".partitioned.parted.04162"));
    }

    @Test
    public void testMoveShard() throws Exception {
        RerouteMoveShardAnalyzedStatement statement = new RerouteMoveShardAnalyzedStatement(
            TABLE_INFO,
            PartitionName.fromIndexOrTemplate(TABLE_INFO.ident().indexName()),
            0,
            "node1",
            "node2"
        );
    }
}
