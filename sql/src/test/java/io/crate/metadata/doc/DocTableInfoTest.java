package io.crate.metadata.doc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.symbol.DynamicReference;
import io.crate.metadata.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DocTableInfoTest extends CrateUnitTest {

    ExecutorService executorService;

    @Before
    public void before() throws Exception {
        executorService = Executors.newSingleThreadExecutor();
    }

    @After
    public void after() throws Exception {
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void testGetColumnInfo() throws Exception {
        TableIdent tableIdent = new TableIdent(null, "dummy");

        DocTableInfo info = new DocTableInfo(
                tableIdent,
                ImmutableList.of(
                        new ReferenceInfo(new ReferenceIdent(tableIdent, new ColumnIdent("o", ImmutableList.<String>of())), RowGranularity.DOC, DataTypes.OBJECT)
                ),
                ImmutableList.<ReferenceInfo>of(),
                ImmutableList.<GeneratedReferenceInfo>of(),
                ImmutableMap.<ColumnIdent, IndexReferenceInfo>of(),
                ImmutableMap.<ColumnIdent, ReferenceInfo>of(),
                ImmutableMap.<ColumnIdent, String>of(),
                ImmutableList.<ColumnIdent>of(),
                null,
                false,
                true,
                new String[0],
                null,
                new IndexNameExpressionResolver(Settings.EMPTY),
                5,
                new BytesRef("0"),
                ImmutableMap.<String,Object>of(),
                ImmutableList.<ColumnIdent>of(),
                ImmutableList.<PartitionName>of(),
                ColumnPolicy.DYNAMIC,
                executorService
        );

        ReferenceInfo foobar = info.getReferenceInfo(new ColumnIdent("o", ImmutableList.of("foobar")));
        assertNull(foobar);
        DynamicReference reference = info.getDynamic(new ColumnIdent("o", ImmutableList.of("foobar")), false);
        assertNull(reference);
        reference = info.getDynamic(new ColumnIdent("o", ImmutableList.of("foobar")), true);
        assertNotNull(reference);
        assertSame(reference.valueType(), DataTypes.UNDEFINED);
    }

    @Test
    public void testGetColumnInfoStrictParent() throws Exception {

        TableIdent dummy = new TableIdent(null, "dummy");
        ReferenceIdent foobarIdent = new ReferenceIdent(dummy, new ColumnIdent("foobar"));
        ReferenceInfo strictParent = new ReferenceInfo(
                foobarIdent,
                RowGranularity.DOC,
                DataTypes.OBJECT,
                ColumnPolicy.STRICT,
                ReferenceInfo.IndexType.NOT_ANALYZED
        );

        ImmutableMap<ColumnIdent, ReferenceInfo> references = ImmutableMap.<ColumnIdent, ReferenceInfo>builder()
                .put(new ColumnIdent("foobar"), strictParent)
                .build();

        DocTableInfo info = new DocTableInfo(
                dummy,
                ImmutableList.<ReferenceInfo>of(strictParent),
                ImmutableList.<ReferenceInfo>of(),
                ImmutableList.<GeneratedReferenceInfo>of(),
                ImmutableMap.<ColumnIdent, IndexReferenceInfo>of(),
                references,
                ImmutableMap.<ColumnIdent, String>of(),
                ImmutableList.<ColumnIdent>of(),
                null,
                false,
                true,
                new String[0],
                null,
                new IndexNameExpressionResolver(Settings.EMPTY),
                5,
                new BytesRef("0"),
                ImmutableMap.<String, Object>of(),
                ImmutableList.<ColumnIdent>of(),
                ImmutableList.<PartitionName>of(),
                ColumnPolicy.DYNAMIC,
                executorService
        );


        ColumnIdent columnIdent = new ColumnIdent("foobar", Arrays.asList("foo", "bar"));
        assertNull(info.getReferenceInfo(columnIdent));
        assertNull(info.getDynamic(columnIdent, false));

        columnIdent = new ColumnIdent("foobar", Arrays.asList("foo"));
        assertNull(info.getReferenceInfo(columnIdent));
        assertNull(info.getDynamic(columnIdent, false));

        ReferenceInfo colInfo = info.getReferenceInfo(new ColumnIdent("foobar"));
        assertNotNull(colInfo);
    }
}
