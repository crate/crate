package io.crate.metadata.doc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.expression.symbol.DynamicReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class DocTableInfoTest extends CrateUnitTest {

    @Test
    public void testGetColumnInfo() throws Exception {
        RelationName relationName = new RelationName(Schemas.DOC_SCHEMA_NAME, "dummy");

        DocTableInfo info = new DocTableInfo(
            relationName,
            ImmutableList.of(
                new Reference(
                    new ReferenceIdent(relationName, new ColumnIdent("o", ImmutableList.of())),
                    RowGranularity.DOC,
                    ObjectType.untyped(),
                    null,
                    null
                )
            ),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            true,
            new String[0],
            new String[0],
            new IndexNameExpressionResolver(),
            5,
            "0",
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            ColumnPolicy.DYNAMIC,
            Version.CURRENT,
            null,
            false,
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
        RelationName dummy = new RelationName(Schemas.DOC_SCHEMA_NAME, "dummy");
        ReferenceIdent foobarIdent = new ReferenceIdent(dummy, new ColumnIdent("foobar"));
        Reference strictParent = new Reference(
            foobarIdent,
            RowGranularity.DOC,
            ObjectType.untyped(),
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            true,
            null,
            null
        );

        ImmutableMap<ColumnIdent, Reference> references = ImmutableMap.<ColumnIdent, Reference>builder()
            .put(new ColumnIdent("foobar"), strictParent)
            .build();

        DocTableInfo info = new DocTableInfo(
            dummy,
            ImmutableList.of(strictParent),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(),
            references,
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            true,
            new String[0],
            new String[0],
            new IndexNameExpressionResolver(),
            5,
            "0",
            ImmutableMap.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            ColumnPolicy.DYNAMIC,
            Version.CURRENT,
            null,
            false,
            Operation.ALL
        );


        ColumnIdent columnIdent = new ColumnIdent("foobar", Arrays.asList("foo", "bar"));
        assertNull(info.getReference(columnIdent));
        assertNull(info.getDynamic(columnIdent, false));

        columnIdent = new ColumnIdent("foobar", Collections.singletonList("foo"));
        assertNull(info.getReference(columnIdent));
        assertNull(info.getDynamic(columnIdent, false));

        Reference colInfo = info.getReference(new ColumnIdent("foobar"));
        assertNotNull(colInfo);
    }
}
