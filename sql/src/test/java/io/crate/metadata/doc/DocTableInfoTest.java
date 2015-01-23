package io.crate.metadata.doc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.DynamicReference;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class DocTableInfoTest {

    @Test
    public void testGetColumnInfo() throws Exception {
        TableIdent tableIdent = new TableIdent(null, "dummy");

        DocTableInfo info = new DocTableInfo(
                mock(DocSchemaInfo.class),
                tableIdent,
                ImmutableList.<ReferenceInfo>of(
                        new ReferenceInfo(new ReferenceIdent(tableIdent, new ColumnIdent("o", ImmutableList.<String>of())), RowGranularity.DOC, DataTypes.OBJECT)
                ),
                ImmutableList.<ReferenceInfo>of(),
                ImmutableMap.<ColumnIdent, IndexReferenceInfo>of(),
                ImmutableMap.<ColumnIdent, ReferenceInfo>of(),
                ImmutableList.<ColumnIdent>of(),
                null,
                false,
                true,
                new String[0],
                null,
                5,
                new BytesRef("0"),
                ImmutableList.<ColumnIdent>of(),
                ImmutableList.<PartitionName>of(),
                ColumnPolicy.DYNAMIC);

        ReferenceInfo foobar = info.getReferenceInfo(new ColumnIdent("o", ImmutableList.of("foobar")));
        assertNull(foobar);
        DynamicReference reference = info.getDynamic(new ColumnIdent("o", ImmutableList.of("foobar")));
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
                mock(DocSchemaInfo.class),
                dummy,
                ImmutableList.<ReferenceInfo>of(strictParent),
                ImmutableList.<ReferenceInfo>of(),
                ImmutableMap.<ColumnIdent, IndexReferenceInfo>of(),
                references,
                ImmutableList.<ColumnIdent>of(),
                null,
                false,
                true,
                new String[0],
                null,
                5,
                new BytesRef("0"),
                ImmutableList.<ColumnIdent>of(),
                ImmutableList.<PartitionName>of(),
                ColumnPolicy.DYNAMIC);


        ColumnIdent columnIdent = new ColumnIdent("foobar", Arrays.asList("foo", "bar"));
        assertNull(info.getReferenceInfo(columnIdent));
        assertNull(info.getDynamic(columnIdent));

        columnIdent = new ColumnIdent("foobar", Arrays.asList("foo"));
        assertNull(info.getReferenceInfo(columnIdent));
        assertNull(info.getDynamic(columnIdent));

        ReferenceInfo colInfo = info.getReferenceInfo(new ColumnIdent("foobar"));
        assertNotNull(colInfo);
    }
}
