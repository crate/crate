
package io.crate.analyze.relations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.DataTypeAnalyzer;
import io.crate.expression.symbol.DynamicReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.TableElement;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public class NewColumnFieldProvider implements FieldProvider<Reference> {

    private final RelationName table;
    private final Map<ColumnIdent, DataType<?>> types;

    public NewColumnFieldProvider(RelationName table, List<? extends TableElement<Expression>> tableElements) {
        this.table = table;
        this.types = new HashMap<>();
        for (var tableElement : tableElements) {
            if (tableElement instanceof ColumnDefinition<Expression> columnDef) {
                ColumnType<Expression> columnType = columnDef.type();
                if (columnType == null) {
                    continue;
                }
                DataType<?> type = DataTypeAnalyzer.convert(columnType);
                ColumnIdent column = new ColumnIdent(columnDef.ident());
                types.put(column, type);
                if (type instanceof ObjectType objectType) {
                    addChildren(types, column, objectType);
                }
            }
        }
    }

    private static void addChildren(Map<ColumnIdent, DataType<?>> types, ColumnIdent column, ObjectType objectType) {
        for (var entry : objectType.innerTypes().entrySet()) {
            String childName = entry.getKey();
            DataType<?> childType = entry.getValue();
            ColumnIdent childColumn = column.append(childName);
            types.put(childColumn, childType);
            if (childType instanceof ObjectType childObjectType) {
                addChildren(types, childColumn, childObjectType);
            }
        }
    }

    @Override
    public Reference resolveField(QualifiedName qualifiedName,
                                  @Nullable List<String> path,
                                  Operation operation,
                                  boolean errorOnUnknownObjectKey) {
        ColumnIdent column = ColumnIdent.fromNameSafe(qualifiedName, path == null ? List.of() : path);
        DataType<?> type = types.getOrDefault(column, DataTypes.UNDEFINED);
        var dynamicRef = new DynamicReference(new ReferenceIdent(table, column), RowGranularity.DOC, -1);
        dynamicRef.valueType(type);
        return dynamicRef;
    }
}
