package io.crate.metadata.table;

import com.google.common.collect.ImmutableList;
import io.crate.PartitionName;
import io.crate.analyze.TableParameterInfo;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexReferenceInfo;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.symbol.DynamicReference;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.util.ArrayList;
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

    public DynamicReference getDynamic(ColumnIdent ident) {
        return getDynamic(ident, false);
    }

    @Override
    public DynamicReference getDynamic(ColumnIdent ident, boolean forWrite) {
        boolean parentIsIgnored = false;
        if (!ident.isColumn()) {
            // see if parent is strict object
            ColumnIdent parentIdent = ident.getParent();
            ReferenceInfo parentInfo = null;

            while (parentIdent != null) {
                parentInfo = getReferenceInfo(parentIdent);
                if (parentInfo != null) {
                    break;
                }
                parentIdent = parentIdent.getParent();
            }

            if (parentInfo != null) {
                switch (parentInfo.columnPolicy()) {
                    case STRICT:
                        throw new ColumnUnknownException(ident.fqn());
                    case IGNORED:
                        parentIsIgnored = true;
                        break;
                }
            }
        } else if(forWrite == false && columnPolicy() != ColumnPolicy.IGNORED) {
            throw new ColumnUnknownException(ident.fqn());
        } else {
            switch (columnPolicy()) {
                case STRICT:
                    throw new ColumnUnknownException(ident.fqn());
                case IGNORED:
                    parentIsIgnored = true;
                    break;
                default:
                    break;
            }
        }
        DynamicReference reference = new DynamicReference(new ReferenceIdent(ident(), ident), rowGranularity());
        if (parentIsIgnored) {
            reference.columnPolicy(ColumnPolicy.IGNORED);
        }
        return reference;
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
    public TableParameterInfo tableParameterInfo() {
        return null;
    }

    @Override
    public String toString() {
        return String.format("%s.%s", schemaInfo.name(), ident().name());
    }
}
