package io.crate.metadata.table;

import com.google.common.collect.ImmutableList;
import io.crate.PartitionName;
import io.crate.metadata.relation.AnalyzedRelation;
import io.crate.metadata.relation.RelationVisitor;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexReferenceInfo;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.symbol.DynamicReference;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class AbstractTableInfo implements TableInfo {

    private static final BytesRef ZERO_REPLICAS = new BytesRef("0");
    private final SchemaInfo schemaInfo;

    protected AbstractTableInfo(SchemaInfo schemaInfo) {
        this.schemaInfo = schemaInfo;
    }

    @Override
    public SchemaInfo schemaInfo() {
        return schemaInfo;
    }

    @Override
    public int numberOfShards() {
        return 1;
    }

    @Override
    public BytesRef numberOfReplicas() {
        return ZERO_REPLICAS;
    }

    @Override
    public boolean hasAutoGeneratedPrimaryKey() {
        return false;
    }

    @Override
    public boolean isAlias() {
        return false;
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }

    @Override
    public List<ReferenceInfo> partitionedByColumns() {
        return ImmutableList.of();
    }

    @Override
    public Collection<IndexReferenceInfo> indexColumns() {
        return ImmutableList.of();
    }

    @Nullable
    @Override
    public IndexReferenceInfo indexColumn(ColumnIdent ident) {
        return null;
    }

    @Nullable
    @Override
    public ColumnIdent clusteredBy() {
        return null;
    }

    @Override
    public DynamicReference getDynamic(ColumnIdent ident) {
        return null;
    }

    @Override
    public List<PartitionName> partitions() {
        return new ArrayList<>(0);
    }

    @Override
    public List<ColumnIdent> partitionedBy() {
        return ImmutableList.of();
    }

    @Override
    public List<AnalyzedRelation> children() {
        return ImmutableList.of();
    }

    @Override
    public int numRelations() {
        return 1;
    }

    @Override
    public List<TableInfo> tables() {
        return ImmutableList.of();
    }

    @Override
    public <C, R> R accept(RelationVisitor<C, R> relationVisitor, C context) {
        return relationVisitor.visitTableInfo(this, context);
    }

    @Override
    public boolean resolvesToName(String relationName) {
        return ident().name().equals(relationName);
    }
}
