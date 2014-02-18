package io.crate.metadata.doc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import org.cratedb.DataType;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DocTableInfoTest {

    @Test
    public void testGetColumnInfo() throws Exception {

        DocTableInfo info = new DocTableInfo(
                new TableIdent(null, "dummy"),
                ImmutableList.<ReferenceInfo>of(),
                ImmutableMap.<ColumnIdent, ReferenceInfo>of(),
                ImmutableList.<String>of(),
                null,
                false,
                new String[0],
                null);

        ReferenceInfo foobar = info.getColumnInfo(new ColumnIdent("foobar"));
        assertNotNull(foobar);
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
                references,
                ImmutableList.<String>of(),
                null,
                false,
                new String[0],
                null);

        assertNull(info.getColumnInfo(new ColumnIdent("foobar", Arrays.asList("foo", "bar"))));
    }
}
