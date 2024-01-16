/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata.doc;

import static io.crate.expression.reference.doc.lucene.SourceParser.UNKNOWN_COLUMN_PREFIX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.analyze.BoundCreateTable;
import io.crate.analyze.DropColumn;
import io.crate.analyze.NumberOfReplicas;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.WhereClause;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.TableReferenceResolver;
import io.crate.common.CheckedFunction;
import io.crate.common.collections.Lists;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.execution.ddl.tables.MappingUtil;
import io.crate.execution.ddl.tables.MappingUtil.AllocPosition;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.VoidReference;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.sys.TableColumn;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.ShardedTable;
import io.crate.metadata.table.StoredTable;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.Expression;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;


/**
 * Represents a user table.
 * <p>
 *     A user table either maps to 1 lucene index (if not partitioned)
 *     Or to multiple indices (if partitioned, or an alias)
 * </p>
 *
 * <p>
 *     See the following table for examples how the indexName is encoded.
 *     Functions to encode/decode are in {@link io.crate.metadata.IndexParts}
 * </p>
 *
 * <table>
 *     <tr>
 *         <th>schema</th>
 *         <th>tableName</th>
 *         <th>indices</th>
 *         <th>partitioned</th>
 *         <th>templateName</th>
 *     </tr>
 *
 *     <tr>
 *         <td>doc</td>
 *         <td>t1</td>
 *         <td>[ t1 ]</td>
 *         <td>NO</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>doc</td>
 *         <td>t1p</td>
 *         <td>[ .partitioned.t1p.&lt;ident&gt; ]</td>
 *         <td>YES</td>
 *         <td>.partitioned.t1p.</td>
 *     </tr>
 *     <tr>
 *         <td>custom</td>
 *         <td>t1</td>
 *         <td>[ custom.t1 ]</td>
 *         <td>NO</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>custom</td>
 *         <td>t1p</td>
 *         <td>[ custom..partitioned.t1p.&lt;ident&gt; ]</td>
 *         <td>YES</td>
 *         <td>custom..partitioned.t1p.</td>
 *     </tr>
 * </table>
 *
 */
public class DocTableInfo implements TableInfo, ShardedTable, StoredTable {


    private final Collection<Reference> columns;
    private final Set<Reference> droppedColumns;
    private final List<GeneratedReference> generatedColumns;
    private final List<Reference> partitionedByColumns;
    private final List<Reference> defaultExpressionColumns;
    private final Collection<ColumnIdent> notNullColumns;
    private final Map<ColumnIdent, IndexReference> indexColumns;
    private final Map<ColumnIdent, Reference> references;
    private final Map<String, String> leafNamesByOid;
    private final Map<ColumnIdent, String> analyzers;
    private final RelationName ident;
    @Nullable
    private final String pkConstraintName;
    private final List<ColumnIdent> primaryKeys;
    private final List<CheckConstraint<Symbol>> checkConstraints;
    private final ColumnIdent clusteredBy;
    private final String[] concreteIndices;
    private final String[] concreteOpenIndices;
    private final List<ColumnIdent> partitionedBy;
    private final int numberOfShards;
    private final String numberOfReplicas;
    private final Settings tableParameters;
    private final TableColumn docColumn;
    private final Set<Operation> supportedOperations;

    private final List<PartitionName> partitions;

    private final boolean hasAutoGeneratedPrimaryKey;
    private final boolean isPartitioned;
    private final Version versionCreated;
    private final Version versionUpgraded;

    private final boolean closed;

    private final ColumnPolicy columnPolicy;

    public DocTableInfo(RelationName ident,
                        Map<ColumnIdent, Reference> references,
                        Map<ColumnIdent, IndexReference> indexColumns,
                        Map<ColumnIdent, String> analyzers,
                        @Nullable String pkConstraintName,
                        List<ColumnIdent> primaryKeys,
                        List<CheckConstraint<Symbol>> checkConstraints,
                        ColumnIdent clusteredBy,
                        String[] concreteIndices,
                        String[] concreteOpenIndices,
                        Settings tableParameters,
                        List<ColumnIdent> partitionedBy,
                        List<PartitionName> partitions,
                        ColumnPolicy columnPolicy,
                        Version versionCreated,
                        @Nullable Version versionUpgraded,
                        boolean closed,
                        Set<Operation> supportedOperations) {
        this.notNullColumns = references.values().stream()
            .filter(r -> !r.column().isSystemColumn())
            .filter(r -> !primaryKeys.contains(r.column()))
            .filter(r -> !r.isNullable())
            .sorted(Reference.CMP_BY_POSITION_THEN_NAME)
            .map(Reference::column)
            .toList();
        this.droppedColumns = references.values().stream()
            .filter(Reference::isDropped)
            .collect(Collectors.toSet());
        this.references = references.entrySet().stream()
            .filter(entry -> !entry.getValue().isDropped())
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        DocSysColumns.forTable(ident, this.references::put);
        this.columns = this.references.values().stream()
            .filter(r -> !r.column().isSystemColumn())
            .filter(r -> r.column().isRoot())
            .sorted(Reference.CMP_BY_POSITION_THEN_NAME)
            .toList();
        this.partitionedByColumns = Lists.map(partitionedBy, x -> {
            Reference ref = this.references.get(x);
            assert ref != null : "Column in `partitionedBy` must be present in `references`";
            return ref;
        });
        this.generatedColumns = this.references.values().stream()
            .filter(r -> r instanceof GeneratedReference)
            .map(r -> (GeneratedReference) r)
            .toList();
        this.indexColumns = indexColumns;
        leafNamesByOid = new HashMap<>();
        Stream.concat(Stream.concat(this.references.values().stream(), indexColumns.values().stream()), droppedColumns.stream())
            .filter(r -> r.oid() != Metadata.COLUMN_OID_UNASSIGNED)
            .forEach(r -> leafNamesByOid.put(Long.toString(r.oid()), r.column().leafName()));
        this.analyzers = analyzers;
        this.ident = ident;
        this.pkConstraintName = pkConstraintName;

        // `_id` is implicitly added to primaryKeys ONLY if clusteredBy is empty and the table is not partitioned
        // because `select * from tbl where _id = ?` wouldn't uniquely identify a row on partitioned tables
        //
        // For the same reason, `hasAutoGeneratedPrimaryKey` is false in that case
        boolean isClusteredBySysId = clusteredBy == null || clusteredBy.equals(DocSysColumns.ID);
        this.primaryKeys = primaryKeys.isEmpty() && isClusteredBySysId && partitionedBy.isEmpty()
            ? List.of(DocSysColumns.ID)
            : primaryKeys;
        this.hasAutoGeneratedPrimaryKey =
            isClusteredBySysId
            && (this.primaryKeys.size() == 1 && this.primaryKeys.get(0).equals(DocSysColumns.ID))
            && partitionedBy.isEmpty();

        this.checkConstraints = checkConstraints;
        this.clusteredBy = clusteredBy;
        this.concreteIndices = concreteIndices;
        this.concreteOpenIndices = concreteOpenIndices;
        Integer maybeNumberOfShards = tableParameters.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, null);
        if (maybeNumberOfShards == null) {
            throw new IllegalArgumentException("must specify numberOfShards for " + ident);
        }
        this.numberOfShards = maybeNumberOfShards;
        this.numberOfReplicas = NumberOfReplicas.fromSettings(tableParameters);
        this.tableParameters = tableParameters;
        isPartitioned = !partitionedByColumns.isEmpty();
        this.partitionedBy = partitionedBy;
        this.partitions = partitions;
        this.columnPolicy = columnPolicy;
        assert versionCreated.after(Version.V_EMPTY) : "Table must have a versionCreated";
        this.versionCreated = versionCreated;
        this.versionUpgraded = versionUpgraded;
        this.closed = closed;
        this.supportedOperations = supportedOperations;
        this.docColumn = new TableColumn(DocSysColumns.DOC, this.references);
        this.defaultExpressionColumns = this.references.values()
            .stream()
            .filter(r -> r.defaultExpression() != null)
            .toList();
    }

    @Nullable
    public Reference getReference(ColumnIdent columnIdent) {
        Reference reference = references.get(columnIdent);
        if (reference == null) {
            return docColumn.getReference(ident(), columnIdent);
        }
        return reference;
    }

    @Override
    public Collection<Reference> columns() {
        return columns;
    }

    @Override
    public Set<Reference> droppedColumns() {
        return droppedColumns;
    }

    public int maxPosition() {
        return Math.max(
            references.values().stream()
                .filter(ref -> !ref.column().isSystemColumn())
                .mapToInt(Reference::position)
                .max()
                .orElse(0),
            indexColumns.values().stream()
                .mapToInt(IndexReference::position)
                .max()
                .orElse(0)
        );
    }

    public List<Reference> defaultExpressionColumns() {
        return defaultExpressionColumns;
    }

    public List<GeneratedReference> generatedColumns() {
        return generatedColumns;
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public RelationName ident() {
        return ident;
    }

    @Override
    public Routing getRouting(ClusterState state,
                              RoutingProvider routingProvider,
                              final WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              CoordinatorSessionSettings sessionSettings) {
        String[] indices;
        if (whereClause.partitions().isEmpty()) {
            indices = concreteOpenIndices;
        } else {
            indices = whereClause.partitions().toArray(new String[0]);
        }
        Map<String, Set<String>> routingMap = null;
        if (whereClause.clusteredBy().isEmpty() == false) {
            Set<String> routing = whereClause.routingValues();
            if (routing == null) {
                routing = Collections.emptySet();
            }
            routingMap = IndexNameExpressionResolver.resolveSearchRouting(
                state,
                routing,
                indices
            );
        }

        if (routingMap == null) {
            routingMap = Collections.emptyMap();
        }
        return routingProvider.forIndices(state, indices, routingMap, isPartitioned, shardSelection);
    }

    @Override
    @Nullable
    public String pkConstraintName() {
        return pkConstraintName;
    }

    public List<ColumnIdent> primaryKey() {
        return primaryKeys;
    }

    @Override
    public List<CheckConstraint<Symbol>> checkConstraints() {
        return checkConstraints;
    }

    @Override
    public int numberOfShards() {
        return numberOfShards;
    }

    @Override
    public String numberOfReplicas() {
        return numberOfReplicas;
    }

    @Override
    public ColumnIdent clusteredBy() {
        return clusteredBy;
    }

    public boolean hasAutoGeneratedPrimaryKey() {
        return hasAutoGeneratedPrimaryKey;
    }

    public String[] concreteIndices() {
        return concreteIndices;
    }

    public String[] concreteOpenIndices() {
        return concreteOpenIndices;
    }

    /**
     * columns this table is partitioned by.
     * <p>
     * guaranteed to be in the same order as defined in CREATE TABLE statement
     *
     * @return always a list, never null
     */
    public List<Reference> partitionedByColumns() {
        return partitionedByColumns;
    }

    /**
     * column names of columns this table is partitioned by (in dotted syntax).
     * <p>
     * guaranteed to be in the same order as defined in CREATE TABLE statement
     *
     * @return always a list, never null
     */
    public List<ColumnIdent> partitionedBy() {
        return partitionedBy;
    }

    public List<PartitionName> partitions() {
        return partitions;
    }

    /**
     * returns <code>true</code> if this table is a partitioned table,
     * <code>false</code> otherwise
     * <p>
     * if so, {@linkplain #partitions()} returns infos about the concrete indices that make
     * up this virtual partitioned table
     */
    public boolean isPartitioned() {
        return isPartitioned;
    }

    public IndexReference indexColumn(ColumnIdent ident) {
        return indexColumns.get(ident);
    }

    public Collection<IndexReference> indexColumns() {
        return indexColumns.values();
    }

    @Override
    public Iterator<Reference> iterator() {
        return references.values().stream()
            .sorted(Reference.CMP_BY_POSITION_THEN_NAME)
            .iterator();
    }

    /**
     * return the column policy of this table
     * that defines how adding new columns will be handled.
     * <ul>
     * <li><code>STRICT</code> means no new columns are allowed
     * <li><code>DYNAMIC</code> means new columns will be added to the schema
     * <li><code>IGNORED</code> means new columns will not be added to the schema.
     * those ignored columns can only be selected.
     * </ul>
     */
    public ColumnPolicy columnPolicy() {
        return columnPolicy;
    }

    @Nullable
    @Override
    public Version versionCreated() {
        return versionCreated;
    }

    @Nullable
    @Override
    public Version versionUpgraded() {
        return versionUpgraded;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    public Settings parameters() {
        return tableParameters;
    }

    @Override
    public Set<Operation> supportedOperations() {
        return supportedOperations;
    }

    @Override
    public RelationType relationType() {
        return RelationType.BASE_TABLE;
    }

    public String getAnalyzerForColumnIdent(ColumnIdent ident) {
        return analyzers.get(ident);
    }

    @Nullable
    public DynamicReference getDynamic(ColumnIdent ident,
                                       boolean forWrite,
                                       boolean errorOnUnknownObjectKey) {
        boolean parentIsIgnored = false;
        ColumnPolicy parentPolicy = columnPolicy();
        int position = 0;

        for (var parent : getParents(ident)) {
            if (parent != null) {
                parentPolicy = parent.columnPolicy();
                position = parent.position();
                break;
            }
        }
        switch (parentPolicy) {
            case DYNAMIC:
                if (!forWrite) {
                    if (!errorOnUnknownObjectKey) {
                        return new VoidReference(new ReferenceIdent(ident(), ident), rowGranularity(), position);
                    }
                    return null;
                }
                break;
            case STRICT:
                if (forWrite) {
                    throw new ColumnUnknownException(ident, ident());
                }
                return null;
            case IGNORED:
                parentIsIgnored = true;
                break;
            default:
                break;
        }
        if (parentIsIgnored) {
            return new DynamicReference(
                new ReferenceIdent(ident(), ident),
                rowGranularity(),
                ColumnPolicy.IGNORED,
                position
            );
        }
        return new DynamicReference(new ReferenceIdent(ident(), ident), rowGranularity(), position);
    }

    @NotNull
    public Reference resolveColumn(String targetColumnName,
                                   boolean forWrite,
                                   boolean errorOnUnknownObjectKey) throws ColumnUnknownException {
        ColumnIdent columnIdent = ColumnIdent.fromPath(targetColumnName);
        Reference reference = getReference(columnIdent);
        if (reference == null) {
            reference = getDynamic(columnIdent, forWrite, errorOnUnknownObjectKey);
            if (reference == null) {
                throw new ColumnUnknownException(columnIdent, ident);
            }
        }
        return reference;
    }

    @Override
    public String toString() {
        return ident.fqn();
    }

    /**
     * @return columns which are not nullable; excludes primary keys which are implicitly not-null
     **/
    public Collection<ColumnIdent> notNullColumns() {
        return notNullColumns;
    }

    /**
     * Starting from 5.5 column OID-s are used as source keys.
     * Even of 5.5, there are no OIDs (and thus no source key rewrite happening) for:
     * <ul>
     *  <li>OBJECT (IGNORED) sub-columns</li>
     *  <li>Empty arrays, or arrays with only null values</li>
     *  <li>Internal object keys of the geo shape column, such as "coordinates", "type"</li>
     * </ul>
     */
    public Function<String, String> lookupNameBySourceKey() {
        if (versionCreated.onOrAfter(Version.V_5_5_0)) {
            return oidOrName -> {
                String name = leafNamesByOid.get(oidOrName);
                if (name == null) {
                    if (oidOrName.startsWith(UNKNOWN_COLUMN_PREFIX)) {
                        assert oidOrName.length() >= UNKNOWN_COLUMN_PREFIX.length() + 1 : "Column name must consist of at least one character";
                        return oidOrName.substring(UNKNOWN_COLUMN_PREFIX.length());
                    }
                    return oidOrName;
                }
                return name;
            };
        } else {
            return Function.identity();
        }
    }


    private void validateDropColumns(List<DropColumn> dropColumns) {
        var leftOverCols = columns().stream().map(Reference::column).collect(Collectors.toSet());
        for (int i = 0 ; i < dropColumns.size(); i++) {
            var refToDrop = dropColumns.get(i).ref();
            var colToDrop = refToDrop.column();
            for (var indexRef : indexColumns()) {
                if (indexRef.columns().contains(refToDrop)) {
                    throw new UnsupportedOperationException("Dropping column: " + colToDrop.sqlFqn() + " which " +
                                                            "is part of INDEX: " + indexRef + " is not allowed");
                }
            }
            for (var genRef : generatedColumns()) {
                if (genRef.referencedReferences().contains(refToDrop)) {
                    throw new UnsupportedOperationException(String.format(
                        Locale.ENGLISH,
                        "Cannot drop column `%s`. It's used in generated column `%s`: %s",
                        colToDrop.sqlFqn(),
                        genRef.column().sqlFqn(),
                        genRef.formattedGeneratedExpression()
                    ));
                }
            }
            for (var checkConstraint : checkConstraints()) {
                Set<ColumnIdent> columnsInConstraint = new HashSet<>();
                RefVisitor.visitRefs(checkConstraint.expression(), r -> columnsInConstraint.add(r.column()));
                if (columnsInConstraint.size() > 1 && columnsInConstraint.contains(colToDrop)) {
                    throw new UnsupportedOperationException("Dropping column: " + colToDrop.sqlFqn() + " which " +
                        "is used in CHECK CONSTRAINT: " + checkConstraint.name() + " is not allowed");
                }
                boolean constraintColIsSubColOfColToDrop = false;
                for (var columnInConstraint : columnsInConstraint) {
                    if (columnInConstraint.isChildOf(colToDrop)) {
                        constraintColIsSubColOfColToDrop = true; // subcol of the dropped col referred in constraint
                    }
                }
                if (constraintColIsSubColOfColToDrop) {
                    for (var columnInConstraint : columnsInConstraint) {
                        // Check if sibling, parent, or cols of another object are contained in the same constraint
                        if (columnInConstraint.isChildOf(colToDrop) == false
                            && columnInConstraint.path().equals(colToDrop.path()) == false) {
                            throw new UnsupportedOperationException("Dropping column: " + colToDrop.sqlFqn() +
                                " which is used in CHECK CONSTRAINT: " + checkConstraint.name() +
                                " is not allowed");
                        }
                    }
                }
            }
            leftOverCols.remove(colToDrop);
        }
        if (leftOverCols.isEmpty()) {
            throw new UnsupportedOperationException("Dropping all columns of a table is not allowed");
        }
    }

    /**
     * Propagates the changes occurred to child columns to the parent columns inner types, e.g.::
     *   ALTER TABLE ADD COLUMN o['o2']['o3']['x'] INT;
     *   ==> after adding the column `x`, column `o`, `o['o2']`, and `o['o2']['o3']` also need to append `x` to its inner types.
     * @param column the column changed
     * @param type the type that the column changed to (null if dropped)
     * @param newReferences all references of the current table including the changed references
     */
    private void updateParentsInnerTypes(ColumnIdent column,
                                         @Nullable DataType<?> type,
                                         Map<ColumnIdent, Reference> newReferences) {
        ColumnIdent[] child = new ColumnIdent[]{column};
        DataType<?>[] childType = new DataType[]{type};
        for (var parent : column.parents()) {
            Reference parentRef = newReferences.get(parent);
            if (parentRef == null) {
                throw new ColumnUnknownException(column, ident);
            }
            DataType<?> newParentType = ArrayType.updateLeaf(
                parentRef.valueType(),
                leaf -> childType[0] == null ?
                    ((ObjectType) leaf).withoutChild(child[0].leafName()) :
                    ((ObjectType) leaf).withChild(child[0].leafName(), childType[0])
            );
            Reference updatedParent = parentRef.withValueType(newParentType);
            newReferences.replace(parent, updatedParent);
            child[0] = updatedParent.column();
            childType[0] = updatedParent.valueType();
        }
    }

    public DocTableInfo dropColumns(List<DropColumn> columns) {
        validateDropColumns(columns);
        HashSet<Reference> toDrop = HashSet.newHashSet(columns.size());
        HashMap<ColumnIdent, Reference> newReferences = new HashMap<>(references);
        droppedColumns.forEach(ref -> newReferences.put(ref.column(), ref));
        for (var column : columns) {
            ColumnIdent columnIdent = column.ref().column();
            Reference reference = references.get(columnIdent);
            if (toDrop.contains(reference)) {
                continue;
            }
            if (reference == null || reference.isDropped()) {
                if (!column.ifExists()) {
                    throw new ColumnUnknownException(columnIdent, ident);
                }
                continue;
            }
            if (columns.stream().noneMatch(c -> column.ref().column().isChildOf(c.ref().column()))) {
                // if a parent and its child are dropped together,
                // fixing the inner types of the ancestors will be handled by the parent.
                updateParentsInnerTypes(columnIdent, null, newReferences);
            }
            toDrop.add(reference.withDropped(true));
            newReferences.replace(columnIdent, reference.withDropped(true));
            for (var ref : references.values()) {
                if (ref.column().isChildOf(columnIdent)) {
                    toDrop.add(ref);
                    newReferences.remove(ref.column());
                }
            }
        }
        if (toDrop.isEmpty()) {
            return this;
        }
        Function<Symbol, Symbol> updateRef = symbol -> RefReplacer.replaceRefs(symbol, ref -> newReferences.getOrDefault(ref.column(), ref));
        ArrayList<CheckConstraint<Symbol>> newCheckConstraints = new ArrayList<>(checkConstraints.size());
        for (var constraint : checkConstraints) {
            boolean drop = false;
            for (var ref : toDrop) {
                drop = Symbols.containsColumn(constraint.expression(), ref.column());
                if (drop) {
                    break;
                }
            }
            if (!drop) {
                newCheckConstraints.add(constraint.map(updateRef));
            }
        }
        return new DocTableInfo(
            ident,
            newReferences,
            indexColumns,
            analyzers,
            pkConstraintName,
            primaryKeys,
            newCheckConstraints,
            clusteredBy,
            concreteIndices,
            concreteOpenIndices,
            tableParameters,
            partitionedBy,
            partitions,
            columnPolicy,
            versionCreated,
            versionUpgraded,
            closed,
            supportedOperations
        );
    }

    private void validateRenameColumn(Reference refToRename, ColumnIdent newName) {
        var oldName = refToRename.column();
        var reference = getReference(oldName);
        if (reference == null) {
            reference = indexColumn(oldName);
        }
        if (!refToRename.equals(reference)) {
            throw new ColumnUnknownException(oldName, ident);
        }
        if (getReference(newName) != null || indexColumn(newName) != null) {
            throw new IllegalArgumentException("Cannot rename column to a name that is in use");
        }
    }

    public DocTableInfo renameColumn(Reference refToRename, ColumnIdent newName) {
        validateRenameColumn(refToRename, newName);
        final ColumnIdent oldName = refToRename.column();

        Predicate<ColumnIdent> toBeRenamed = c -> c.equals(oldName) || c.isChildOf(oldName);

        // Renaming columns are done in 2 steps:
        //      1) rename SimpleReferences' own ColumnIdents
        //      2) rename dependencies such as GeneratedReferences, IndexReferences, Check Constraints, etc.
        // where 1) is used to perform 2).

        Map<ColumnIdent, Reference> oldNameToRenamedRefs = new HashMap<>();
        for (var ref : references.values()) {
            ColumnIdent column = ref.column();
            if (toBeRenamed.test(column)) {
                var renamedRef = ref.withReferenceIdent(
                    new ReferenceIdent(ident, ref.column().replacePrefix(newName)));
                oldNameToRenamedRefs.put(column, renamedRef);
            } else {
                oldNameToRenamedRefs.put(column, ref);
            }
        }

        // remove oldNames from the inner types of the ancestors
        updateParentsInnerTypes(oldName, null, oldNameToRenamedRefs);
        // add newNames to the inner types of the ancestors
        updateParentsInnerTypes(newName, refToRename.valueType(), oldNameToRenamedRefs);

        Function<Reference, Reference> renameGeneratedRefs = ref -> {
            if (ref instanceof GeneratedReference genRef) {
                return new GeneratedReference(
                    genRef.reference(), // already renamed in step 1)
                    RefReplacer.replaceRefs(genRef.generatedExpression(), r -> oldNameToRenamedRefs.getOrDefault(r.column(), r))
                );
            }
            return ref;
        };

        Function<IndexReference, IndexReference> renameIndexRefs = idxRef -> {
            var updatedRef = idxRef.updateColumns(
                Lists.map(idxRef.columns(), r -> oldNameToRenamedRefs.getOrDefault(r.column(), r)));
            if (toBeRenamed.test(idxRef.column())) {
                return (IndexReference) updatedRef.withReferenceIdent(
                    new ReferenceIdent(idxRef.ident().tableIdent(), idxRef.column().replacePrefix(newName)));
            }
            return updatedRef;
        };

        Function<CheckConstraint<Symbol>, CheckConstraint<Symbol>> renameCheckConstraints = check -> {
            var renamed = RefReplacer.replaceRefs(check.expression(), r -> oldNameToRenamedRefs.getOrDefault(r.column(), r));
            return new CheckConstraint<>(
                check.name(),
                renamed,
                ExpressionFormatter.formatStandaloneExpression(
                    SqlParser.createExpression(renamed.toString(Style.UNQUALIFIED))));
        };

        var renamedReferences = oldNameToRenamedRefs.values().stream()
            .map(renameGeneratedRefs)
            .collect(Collectors.toMap(Reference::column, ref -> ref));
        var renamedIndexColumns = indexColumns.values().stream()
            .map(renameIndexRefs)
            .collect(Collectors.toMap(Reference::column, ref -> ref));

        Function<ColumnIdent, ColumnIdent> renameColumnIfMatch = column -> toBeRenamed.test(column) ? column.replacePrefix(newName) : column;

        var renamedClusteredBy = renameColumnIfMatch.apply(clusteredBy);
        var renamedPrimaryKeys = Lists.map(primaryKeys, renameColumnIfMatch);
        var renamedPartitionedBy = Lists.map(partitionedBy, renameColumnIfMatch);
        var renamedCheckConstraints = Lists.map(checkConstraints, renameCheckConstraints);
        var renamedAnalyzers = analyzers.entrySet().stream()
            .collect(Collectors.toMap(e -> renameColumnIfMatch.apply(e.getKey()), Entry::getValue));

        return new DocTableInfo(
            ident,
            renamedReferences,
            renamedIndexColumns,
            renamedAnalyzers,
            pkConstraintName,
            renamedPrimaryKeys,
            renamedCheckConstraints,
            renamedClusteredBy,
            concreteIndices,
            concreteOpenIndices,
            tableParameters,
            renamedPartitionedBy,
            partitions,
            columnPolicy,
            versionCreated,
            versionUpgraded,
            closed,
            supportedOperations
        );
    }

    public Metadata.Builder writeTo(CheckedFunction<IndexMetadata, MapperService, IOException> createMapperService,
                                    Metadata metadata,
                                    Metadata.Builder metadataBuilder) throws IOException {
        List<Reference> allColumns = Stream.concat(
                Stream.concat(
                    droppedColumns.stream(),
                    indexColumns.values().stream()
                ),
                references.values().stream()
            )
            .filter(ref -> !ref.column().isSystemColumn())
            .sorted(Reference.CMP_BY_POSITION_THEN_NAME)
            .toList();
        IntArrayList pKeyIndices = new IntArrayList(primaryKeys.size());
        for (ColumnIdent pk : primaryKeys) {
            int idx = Reference.indexOf(allColumns, pk);
            if (idx >= 0) {
                pKeyIndices.add(idx);
            }
        }
        LinkedHashMap<String, String> checkConstraintMap = LinkedHashMap.newLinkedHashMap(checkConstraints.size());
        for (var check : checkConstraints) {
            checkConstraintMap.put(check.name(), check.expressionStr());
        }
        AllocPosition allocPosition = AllocPosition.forTable(this);
        Map<String, Object> mapping = Map.of("default", MappingUtil.createMapping(
            allocPosition,
            pkConstraintName,
            allColumns,
            pKeyIndices,
            checkConstraintMap,
            Lists.map(partitionedByColumns, BoundCreateTable::toPartitionMapping),
            columnPolicy,
            clusteredBy == DocSysColumns.ID ? null : clusteredBy.fqn()
        ));
        for (String indexName : concreteIndices) {
            IndexMetadata indexMetadata = metadata.index(indexName);
            if (indexMetadata == null) {
                throw new UnsupportedOperationException("Cannot create index via DocTableInfo.writeTo");
            }
            MapperService mapperService = createMapperService.apply(indexMetadata);
            DocumentMapper mapper = mapperService.merge(mapping, MapperService.MergeReason.MAPPING_UPDATE);
            metadataBuilder.put(
                IndexMetadata.builder(indexMetadata)
                    .putMapping(new MappingMetadata(mapper.mappingSource()))
                    .numberOfShards(numberOfShards)
                    .mappingVersion(indexMetadata.getMappingVersion() + 1)
            );
        }
        if (isPartitioned) {
            String templateName = PartitionName.templateName(ident.schema(), ident.name());
            IndexTemplateMetadata indexTemplateMetadata = metadata.templates().get(templateName);
            if (indexTemplateMetadata == null) {
                throw new UnsupportedOperationException("Cannot create template via DocTableInfo.writeTo");
            }
            Integer version = indexTemplateMetadata.getVersion();
            var template = new IndexTemplateMetadata.Builder(indexTemplateMetadata)
                .putMapping(Strings.toString(JsonXContent.builder().map(mapping)))
                .version(version == null ? 1 : version + 1)
                .build();
            metadataBuilder.put(template);
        }
        return metadataBuilder;
    }

    private boolean addNewReferences(LongSupplier acquireOid,
                                     AtomicInteger positions,
                                     HashMap<ColumnIdent, Reference> newReferences,
                                     HashMap<ColumnIdent, List<Reference>> tree,
                                     @Nullable ColumnIdent node) {
        List<Reference> children = tree.get(node);
        if (children == null) {
            return false;
        }
        boolean addedColumn = false;
        for (Reference newRef : children) {
            ColumnIdent newColumn = newRef.column();
            Reference exists = getReference(newColumn);
            if (exists == null) {
                if (indexColumns.containsKey(newColumn)) {
                    throw new UnsupportedOperationException(String.format(
                        Locale.ENGLISH,
                        "Index column `%s` already exists",
                        newColumn
                    ));
                }
                addedColumn = true;
                newReferences.put(newColumn, newRef.withOidAndPosition(acquireOid, positions::incrementAndGet));
            } else if (exists.valueType().id() != newRef.valueType().id()) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Column `%s` already exists with type `%s`. Cannot add same column with type `%s`",
                    newColumn,
                    exists.valueType().getName(),
                    newRef.valueType().getName()));
            }
            boolean addedChildren = addNewReferences(acquireOid, positions, newReferences, tree, newColumn);
            addedColumn = addedColumn || addedChildren;
        }
        return addedColumn;
    }

    private List<Reference> addMissingParents(List<Reference> columns) {
        ArrayList<Reference> result = new ArrayList<>(columns);
        for (Reference ref : columns) {
            for (ColumnIdent parent : ref.column().parents()) {
                if (!Symbols.containsColumn(result, parent)) {
                    Reference parentRef = getReference(parent);
                    if (parentRef == null) {
                        throw new UnsupportedOperationException(
                            "Cannot create parents of new column implicitly. `" + parent + "` is undefined");
                    }
                    result.add(parentRef);
                }
            }
        }
        return result;
    }

    public DocTableInfo addColumns(NodeContext nodeCtx,
                                   LongSupplier acquireOid,
                                   List<Reference> newColumns,
                                   IntArrayList pKeyIndices,
                                   Map<String, String> newCheckConstraints) {
        HashMap<ColumnIdent, Reference> newReferences = new HashMap<>(references);
        droppedColumns.forEach(ref -> newReferences.put(ref.column(), ref));
        int maxPosition = maxPosition();
        AtomicInteger positions = new AtomicInteger(maxPosition);
        List<Reference> newColumnsWithParents = addMissingParents(newColumns);
        HashMap<ColumnIdent, List<Reference>> tree = Reference.buildTree(newColumnsWithParents);
        boolean addedColumn = addNewReferences(acquireOid, positions, newReferences, tree, null);
        if (!addedColumn) {
            return this;
        }
        for (Reference newRef : newColumns) {
            if (newColumns.stream().noneMatch(r -> r.column().isChildOf(newRef.column()))) {
                // if a child and its parent is added together,
                // fixing the inner types of the ancestors will be handled by the child
                updateParentsInnerTypes(newRef.column(), newRef.valueType(), newReferences);
            }
        }
        List<ColumnIdent> newPrimaryKeys;
        if (pKeyIndices.isEmpty()) {
            newPrimaryKeys = primaryKeys;
        } else {
            newPrimaryKeys = new ArrayList<>(primaryKeys);
            for (var cursor : pKeyIndices) {
                int pkIndex = cursor.value;
                Reference pkColumn = newColumns.get(pkIndex);
                newPrimaryKeys.add(pkColumn.column());
            }
        }
        List<CheckConstraint<Symbol>> newChecks;
        if (newCheckConstraints.isEmpty()) {
            newChecks = checkConstraints;
        } else {
            newChecks = new ArrayList<>(checkConstraints);
            CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
            ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
                txnCtx,
                nodeCtx,
                ParamTypeHints.EMPTY,
                new TableReferenceResolver(newReferences, ident),
                null
            );
            var expressionAnalysisContext = new ExpressionAnalysisContext(txnCtx.sessionSettings());
            for (var entry : newCheckConstraints.entrySet()) {
                String name = entry.getKey();
                String expressionStr = entry.getValue();
                Expression expression = SqlParser.createExpression(expressionStr);
                Symbol expressionSymbol = expressionAnalyzer.convert(expression, expressionAnalysisContext);
                newChecks.add(new CheckConstraint<Symbol>(name, expressionSymbol, expressionStr));
            }
        }
        return new DocTableInfo(
            ident,
            newReferences,
            indexColumns,
            analyzers,
            pkConstraintName,
            newPrimaryKeys,
            newChecks,
            clusteredBy,
            concreteIndices,
            concreteOpenIndices,
            tableParameters,
            partitionedBy,
            partitions,
            columnPolicy,
            versionCreated,
            versionUpgraded,
            closed,
            supportedOperations
        );
    }

}
