package io.crate.metadata.doc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.DataType;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.DynamicReference;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class DocTableInfoTest {

    @Test
    public void testGetColumnInfo() throws Exception {

        DocTableInfo info = new DocTableInfo(
                new TableIdent(null, "dummy"),
                ImmutableList.<ReferenceInfo>of(),
                ImmutableList.<ReferenceInfo>of(),
                ImmutableMap.<ColumnIdent, ReferenceInfo>of(),
                ImmutableList.<String>of(),
                null,
                false,
                true,
                new String[0],
                null,
                5,
                new BytesRef("0"));

        ReferenceInfo foobar = info.getColumnInfo(new ColumnIdent("foobar"));
        assertNull(foobar);
        DynamicReference reference = info.getDynamic(new ColumnIdent("foobar"));
        assertNotNull(reference);
        assertThat(reference.valueType(), is(DataType.NULL));
    }

    @Test
    public void testGetColumnInfoStrictParent() throws Exception {

        TableIdent dummy = new TableIdent(null, "dummy");
        ReferenceIdent foobarIdent = new ReferenceIdent(dummy, new ColumnIdent("foobar"));
        ReferenceInfo strictParent = new ReferenceInfo(
                foobarIdent,
                RowGranularity.DOC,
                DataType.OBJECT,
                ReferenceInfo.ObjectType.STRICT
        );

        ImmutableMap<ColumnIdent, ReferenceInfo> references = ImmutableMap.<ColumnIdent, ReferenceInfo>builder()
                .put(new ColumnIdent("foobar"), strictParent)
                .build();

        DocTableInfo info = new DocTableInfo(
                dummy,
                ImmutableList.<ReferenceInfo>of(strictParent),
                ImmutableList.<ReferenceInfo>of(),
                references,
                ImmutableList.<String>of(),
                null,
                false,
                true,
                new String[0],
                null,
                5,
                new BytesRef("0"));


        try {
            ColumnIdent columnIdent = new ColumnIdent("foobar", Arrays.asList("foo", "bar"));
            assertNull(info.getColumnInfo(columnIdent));
            info.getDynamic(columnIdent);
            fail();
        } catch (ColumnUnknownException e) {

        }
        try {
            ColumnIdent columnIdent = new ColumnIdent("foobar", Arrays.asList("foo"));
            assertNull(info.getColumnInfo(columnIdent));
            info.getDynamic(columnIdent);
            fail();
        } catch (ColumnUnknownException e) {

        }
        ReferenceInfo colInfo = info.getColumnInfo(new ColumnIdent("foobar"));
        assertNotNull(colInfo);
    }
}
