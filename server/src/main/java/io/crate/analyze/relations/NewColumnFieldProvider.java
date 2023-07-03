
package io.crate.analyze.relations;

import java.util.List;

import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.DynamicReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

public class NewColumnFieldProvider implements FieldProvider<Reference> {

    private final RelationName table;

    public NewColumnFieldProvider(RelationName table) {
        this.table = table;
    }

    @Override
    public Reference resolveField(QualifiedName qualifiedName,
                                  @Nullable List<String> path,
                                  Operation operation,
                                  boolean errorOnUnknownObjectKey) {
        ColumnIdent column = ColumnIdent.fromNameSafe(qualifiedName, path == null ? List.of() : path);
        return new DynamicReference(new ReferenceIdent(table, column), RowGranularity.DOC, -1);
    }
}
