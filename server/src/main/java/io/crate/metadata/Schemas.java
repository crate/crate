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

package io.crate.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.spell.LevenshteinDistance;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Sets;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.expression.udf.UserDefinedFunctionsMetadata;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.doc.DocSchemaInfoFactory;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.View;
import io.crate.metadata.view.ViewMetadata;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.sql.tree.QualifiedName;
import io.crate.user.Privilege;
import io.crate.user.Role;


@Singleton
public class Schemas extends AbstractLifecycleComponent implements Iterable<SchemaInfo>, ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(Schemas.class);

    public static final Collection<String> READ_ONLY_SYSTEM_SCHEMAS = Set.of(
        SysSchemaInfo.NAME,
        InformationSchemaInfo.NAME,
        PgCatalogSchemaInfo.NAME
    );


    private static final Pattern SCHEMA_PATTERN = Pattern.compile("^([^.]+)\\.(.+)");

    /**
     * CrateDB's default schema name if the user hasn't specified anything.
     * Caution: Don't assume that this schema is _always_ set as the default!
     * {@see SessionContext}
     */
    public static final String DOC_SCHEMA_NAME = "doc";

    private final ClusterService clusterService;
    private final DocSchemaInfoFactory docSchemaInfoFactory;
    private final Map<String, SchemaInfo> schemas = new ConcurrentHashMap<>();
    private final Map<String, SchemaInfo> builtInSchemas;

    @Inject
    public Schemas(Map<String, SchemaInfo> builtInSchemas,
                   ClusterService clusterService,
                   DocSchemaInfoFactory docSchemaInfoFactory) {
        this.clusterService = clusterService;
        this.docSchemaInfoFactory = docSchemaInfoFactory;
        schemas.putAll(builtInSchemas);
        this.builtInSchemas = builtInSchemas;
    }

    public TableInfo resolveTableInfo(QualifiedName ident, Operation operation, Role user, SearchPath searchPath) {
        String identSchema = schemaName(ident);
        String tableName = relationName(ident);

        SchemaInfo schemaInfo;
        TableInfo tableInfo = null;
        if (identSchema == null) {
            for (String pathSchema : searchPath) {
                schemaInfo = schemas.get(pathSchema);
                if (schemaInfo != null) {
                    tableInfo = schemaInfo.getTableInfo(tableName);
                    if (tableInfo != null) {
                        break;
                    }
                }
            }
            if (tableInfo == null) {
                SchemaInfo currentSchema = schemas.get(searchPath.currentSchema());
                if (currentSchema == null) {
                    throw new RelationUnknown(tableName);
                } else {
                    throw RelationUnknown.of(tableName, getSimilarTables(user, tableName, currentSchema.getTables()));
                }
            }
        } else {
            schemaInfo = schemas.get(identSchema);
            if (schemaInfo == null) {
                throw SchemaUnknownException.of(identSchema, getSimilarSchemas(user, identSchema));
            } else {
                tableInfo = schemaInfo.getTableInfo(tableName);
                if (tableInfo == null) {
                    throw RelationUnknown.of(identSchema + "." + tableName,
                                             getSimilarTables(user, tableName, schemaInfo.getTables()));
                }
            }
        }
        Operation.blockedRaiseException(tableInfo, operation);
        return tableInfo;
    }

    private static List<String> getSimilarTables(Role user, String tableName, Iterable<TableInfo> tables) {
        LevenshteinDistance levenshteinDistance = new LevenshteinDistance();
        ArrayList<Candidate> candidates = new ArrayList<>();
        for (TableInfo table : tables) {
            if (user.hasAnyPrivilege(Privilege.Clazz.TABLE, table.ident().fqn())) {
                String candidate = table.ident().name();
                float score = levenshteinDistance.getDistance(tableName.toLowerCase(Locale.ENGLISH), candidate.toLowerCase(Locale.ENGLISH));
                if (score > 0.7f) {
                    candidates.add(new Candidate(score, candidate));
                }
            }
        }
        candidates.sort(Comparator.comparing((Candidate x) -> x.score).reversed());
        return candidates.stream()
            .limit(5)
            .map(x -> x.name)
            .collect(Collectors.toList());
    }

    private List<String> getSimilarSchemas(Role user, String schema) {
        LevenshteinDistance levenshteinDistance = new LevenshteinDistance();
        ArrayList<Candidate> candidates = new ArrayList<>();
        for (String availableSchema : schemas.keySet()) {
            if (user.hasAnyPrivilege(Privilege.Clazz.SCHEMA, availableSchema)) {
                float score = levenshteinDistance.getDistance(schema.toLowerCase(Locale.ENGLISH), availableSchema.toLowerCase(Locale.ENGLISH));
                if (score > 0.7f) {
                    candidates.add(new Candidate(score, availableSchema));
                }
            }
        }
        candidates.sort(Comparator.comparing((Candidate x) -> x.score).reversed());
        return candidates.stream()
            .limit(5)
            .map(x -> x.name)
            .collect(Collectors.toList());
    }

    static class Candidate {
        final double score;
        final String name;

        Candidate(double score, String name) {
            this.score = score;
            this.name = name;
        }
    }


    /**
     * Resolves the provided ident relation (table or view) against the search path.
     * @param ident
     * @param searchPath
     * @throws RelationUnknown in case a valid relation cannot be resolved in the search path.
     * @return the corresponding RelationName
     */
    public RelationName resolveRelation(QualifiedName ident, SearchPath searchPath) {
        String identSchema = schemaName(ident);
        String relation = relationName(ident);

        ViewsMetadata views = clusterService.state().metadata().custom(ViewsMetadata.TYPE);
        if (identSchema == null) {
            for (String pathSchema : searchPath) {
                RelationName tableOrViewRelation = getTableOrViewRelation(pathSchema, relation, views);
                if (tableOrViewRelation != null) {
                    return tableOrViewRelation;
                }
            }
        } else {
            RelationName tableOrViewRelation = getTableOrViewRelation(identSchema, relation, views);
            if (tableOrViewRelation != null) {
                return tableOrViewRelation;
            }
        }
        throw new RelationUnknown(ident.toString());
    }

    @Nullable
    private RelationName getTableOrViewRelation(String pathSchema, String relation, ViewsMetadata views) {
        SchemaInfo schemaInfo = schemas.get(pathSchema);
        if (schemaInfo != null) {
            TableInfo tableInfo = schemaInfo.getTableInfo(relation);
            if (tableInfo != null) {
                return new RelationName(pathSchema, relation);
            } else {
                if (views != null) {
                    RelationName viewRelation = new RelationName(pathSchema, relation);
                    if (views.contains(viewRelation)) {
                        return viewRelation;
                    }
                }
            }
        }
        return null;
    }

    @Nullable
    private static String schemaName(QualifiedName ident) {
        assert ident.getParts().size() <=
               3 : "When identifying schemas or tables a qualified name should not have more the 3 parts";
        List<String> parts = ident.getParts();
        if (parts.size() >= 2) {
            return parts.get(parts.size() - 2);
        } else {
            return null;
        }
    }

    private static String relationName(QualifiedName ident) {
        assert ident.getParts().size() <=
               3 : "When identifying schemas or tables a qualified name should not have more the 3 parts";
        List<String> parts = ident.getParts();
        return parts.get(parts.size() - 1);
    }

    /**
     * @param ident the table ident to get a TableInfo for
     * @return an instance of TableInfo for the given ident, guaranteed to be not null
     * @throws io.crate.exceptions.SchemaUnknownException if schema given in <code>ident</code>
     *                                                    does not exist
     * @throws RelationUnknown  if table given in <code>ident</code> does
     *                                                    not exist in the given schema
     */
    public <T extends TableInfo> T getTableInfo(RelationName ident) {
        SchemaInfo schemaInfo = getSchemaInfo(ident);
        TableInfo info = schemaInfo.getTableInfo(ident.name());
        if (info == null) {
            throw new RelationUnknown(ident);
        }
        return (T) info;
    }

    /**
     * @param ident the table ident to get a TableInfo for
     * @param operation The opreation planned to be performed on the table
     * @return an instance of TableInfo for the given ident, guaranteed to be not null and to support the operation
     * required on it.
     * @throws io.crate.exceptions.SchemaUnknownException if schema given in <code>ident</code>
     *                                                    does not exist
     * @throws RelationUnknown  if table given in <code>ident</code> does
     *                                                    not exist in the given schema
     */
    public <T extends TableInfo> T getTableInfo(RelationName ident, Operation operation) {
        TableInfo tableInfo = getTableInfo(ident);
        Operation.blockedRaiseException(tableInfo, operation);
        return (T) tableInfo;
    }

    private SchemaInfo getSchemaInfo(RelationName ident) {
        String schemaName = ident.schema();
        SchemaInfo schemaInfo = schemas.get(schemaName);
        if (schemaInfo == null) {
            throw new SchemaUnknownException(schemaName);
        }
        return schemaInfo;
    }

    @NotNull
    public Iterator<SchemaInfo> iterator() {
        return schemas.values().iterator();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.metadataChanged()) {
            return;
        }

        Set<String> newCurrentSchemas = getNewCurrentSchemas(event.state().metadata());
        synchronized (schemas) {
            Set<String> nonBuiltInSchemas = Sets.difference(schemas.keySet(), builtInSchemas.keySet());
            Set<String> deleted = Sets.difference(nonBuiltInSchemas, newCurrentSchemas);
            Set<String> added = Sets.difference(newCurrentSchemas, schemas.keySet());

            for (String deletedSchema : deleted) {
                try {
                    schemas.remove(deletedSchema).close();
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }

            for (String addedSchema : added) {
                schemas.put(addedSchema, getCustomSchemaInfo(addedSchema));
            }

            // update all existing schemas
            for (SchemaInfo schemaInfo : this) {
                schemaInfo.update(event);
            }
        }
    }

    @VisibleForTesting
    static Set<String> getNewCurrentSchemas(Metadata metadata) {
        Set<String> schemas = new HashSet<>();
        // 'doc' schema is always available and has the special property that its indices
        // don't have to be prefixed with the schema name
        schemas.add(DOC_SCHEMA_NAME);
        for (String index : metadata.getConcreteAllIndices()) {
            addIfSchema(schemas, index);
        }
        for (ObjectCursor<String> cursor : metadata.templates().keys()) {
            addIfSchema(schemas, cursor.value);
        }
        UserDefinedFunctionsMetadata udfMetadata = metadata.custom(UserDefinedFunctionsMetadata.TYPE);
        if (udfMetadata != null) {
            udfMetadata.functionsMetadata()
                .stream()
                .map(UserDefinedFunctionMetadata::schema)
                .forEach(schemas::add);
        }
        ViewsMetadata viewsMetadata = metadata.custom(ViewsMetadata.TYPE);
        if (viewsMetadata != null) {
            StreamSupport.stream(viewsMetadata.names().spliterator(), false)
                .map(IndexParts::new)
                .map(IndexParts::getSchema)
                .forEach(schemas::add);
        }
        return schemas;
    }

    private static void addIfSchema(Set<String> schemas, String indexOrTemplate) {
        Matcher matcher = SCHEMA_PATTERN.matcher(indexOrTemplate);
        if (matcher.matches()) {
            schemas.add(matcher.group(1));
        }
    }

    /**
     * Create a custom schema info.
     *
     * @param name The schema name
     * @return an instance of SchemaInfo for the given name
     */
    private SchemaInfo getCustomSchemaInfo(String name) {
        return docSchemaInfoFactory.create(name, clusterService);
    }

    /**
     * Checks if a given schema name string is a user defined schema or the default one.
     *
     * @param schemaName The schema name as a string.
     */
    public static boolean isDefaultOrCustomSchema(@Nullable String schemaName) {
        if (schemaName == null) {
            return true;
        }
        return !schemaName.equals(InformationSchemaInfo.NAME)
               && !schemaName.equals(SysSchemaInfo.NAME)
               && !schemaName.equals(BlobSchemaInfo.NAME)
               && !schemaName.equals(PgCatalogSchemaInfo.NAME);
    }

    public boolean tableExists(RelationName relationName) {
        SchemaInfo schemaInfo = schemas.get(relationName.schema());
        if (schemaInfo == null) {
            return false;
        }
        schemaInfo.invalidateTableCache(relationName.name());
        TableInfo tableInfo = schemaInfo.getTableInfo(relationName.name());
        if (tableInfo == null) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    protected void doStart() {
        // add listener here to avoid guice proxy errors if the ClusterService could not be build
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() {
    }

    /**
     * @throws RelationUnknown if the view cannot be resolved against the search path.
     */
    public View resolveView(QualifiedName ident, SearchPath searchPath) {
        ViewsMetadata views = clusterService.state().metadata().custom(ViewsMetadata.TYPE);
        ViewMetadata metadata = null;
        RelationName name = null;
        String identSchema = schemaName(ident);
        String viewName = relationName(ident);
        if (views != null) {
            if (identSchema == null) {
                for (String pathSchema : searchPath) {
                    SchemaInfo schemaInfo = schemas.get(pathSchema);
                    if (schemaInfo != null) {
                        name = new RelationName(pathSchema, viewName);
                        metadata = views.getView(name);
                        if (metadata != null) {
                            break;
                        }
                    }
                }
            } else {
                name = new RelationName(identSchema, viewName);
                metadata = views.getView(name);
            }
        }

        if (metadata == null) {
            throw new RelationUnknown(viewName);
        }
        return new View(name, metadata);
    }

    /**
     * Performs a lookup to see if a view with the relationName exists.
     */
    public boolean viewExists(RelationName relationName) {
        ViewsMetadata views = clusterService.state().metadata().custom(ViewsMetadata.TYPE);
        return views != null && views.getView(relationName) != null;
    }
}
