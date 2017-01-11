package io.crate.metadata.doc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.symbol.DynamicReference;
import io.crate.metadata.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.Operation;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.junit.Test;

import java.util.Arrays;

public class DocTableInfoTest extends CrateUnitTest {

    @Test
    public void testGetColumnInfo() throws Exception {
        TableIdent tableIdent = new TableIdent(null, "dummy");

        DocTableInfo info = new DocTableInfo(
            tableIdent,
            ImmutableList.of(
                new Reference(new ReferenceIdent(tableIdent, new ColumnIdent("o", ImmutableList.<String>of())), RowGranularity.DOC, DataTypes.OBJECT)
            ),
            ImmutableList.<Reference>of(),
            ImmutableList.<GeneratedReference>of(),
            ImmutableMap.<ColumnIdent, IndexReference>of(),
            ImmutableMap.<ColumnIdent, Reference>of(),
            ImmutableMap.<ColumnIdent, String>of(),
            ImmutableList.<ColumnIdent>of(),
            null,
            false,
            true,
            Index.EMPTY_ARRAY,
            null,
            new IndexNameExpressionResolver(Settings.EMPTY),
            5,
            new BytesRef("0"),
            ImmutableMap.<String, Object>of(),
            ImmutableList.<ColumnIdent>of(),
            ImmutableList.<PartitionName>of(),
            ColumnPolicy.DYNAMIC,
            Operation.ALL
        );

        Reference foobar = info.getReference(new ColumnIdent("o", ImmutableList.of("foobar")));
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
        Reference strictParent = new Reference(
            foobarIdent,
            RowGranularity.DOC,
            DataTypes.OBJECT,
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            true
        );

        ImmutableMap<ColumnIdent, Reference> references = ImmutableMap.<ColumnIdent, Reference>builder()
            .put(new ColumnIdent("foobar"), strictParent)
            .build();

        DocTableInfo info = new DocTableInfo(
            dummy,
            ImmutableList.<Reference>of(strictParent),
            ImmutableList.<Reference>of(),
            ImmutableList.<GeneratedReference>of(),
            ImmutableMap.<ColumnIdent, IndexReference>of(),
            references,
            ImmutableMap.<ColumnIdent, String>of(),
            ImmutableList.<ColumnIdent>of(),
            null,
            false,
            true,
            Index.EMPTY_ARRAY,
            null,
            new IndexNameExpressionResolver(Settings.EMPTY),
            5,
            new BytesRef("0"),
            ImmutableMap.<String, Object>of(),
            ImmutableList.<ColumnIdent>of(),
            ImmutableList.<PartitionName>of(),
            ColumnPolicy.DYNAMIC,
            Operation.ALL
        );


        ColumnIdent columnIdent = new ColumnIdent("foobar", Arrays.asList("foo", "bar"));
        assertNull(info.getReference(columnIdent));
        assertNull(info.getDynamic(columnIdent, false));

        columnIdent = new ColumnIdent("foobar", Arrays.asList("foo"));
        assertNull(info.getReference(columnIdent));
        assertNull(info.getDynamic(columnIdent, false));

        Reference colInfo = info.getReference(new ColumnIdent("foobar"));
        assertNotNull(colInfo);
    }
}
