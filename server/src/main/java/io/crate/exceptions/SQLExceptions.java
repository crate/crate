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

package io.crate.exceptions;

import java.util.Locale;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.UncheckedExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.InvalidSnapshotNameException;
import org.elasticsearch.snapshots.SnapshotCreationException;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.transport.TransportException;

import io.crate.auth.AccessControl;
import io.crate.metadata.PartitionName;
import io.crate.sql.parser.ParsingException;

public class SQLExceptions {

    private static final Logger LOGGER = LogManager.getLogger(SQLExceptions.class);

    private static final Predicate<Throwable> EXCEPTIONS_TO_UNWRAP = throwable ->
        throwable instanceof TransportException ||
        throwable instanceof UncheckedExecutionException ||
        throwable instanceof CompletionException ||
        throwable instanceof UncategorizedExecutionException ||
        throwable instanceof ExecutionException;

    public static Throwable unwrap(@Nonnull Throwable t, @Nullable Predicate<Throwable> additionalUnwrapCondition) {
        int counter = 0;
        Throwable result = t;
        Predicate<Throwable> unwrapCondition = EXCEPTIONS_TO_UNWRAP;
        if (additionalUnwrapCondition != null) {
            unwrapCondition = unwrapCondition.or(additionalUnwrapCondition);
        }
        while (unwrapCondition.test(result)) {
            Throwable cause = result.getCause();
            if (cause == null) {
                return result;
            }
            if (cause == result) {
                return result;
            }
            if (counter > 10) {
                LOGGER.warn("Exception cause unwrapping ran for 10 levels. Aborting unwrap", t);
                return result;
            }
            counter++;
            result = cause;
        }
        return result;
    }

    public static Throwable unwrap(@Nonnull Throwable t) {
        return unwrap(t, null);
    }

    public static String messageOf(@Nullable Throwable t) {
        if (t == null) {
            return "Unknown";
        }
        // throwable not thrown
        Throwable unwrappedT = unwrap(t);
        if (unwrappedT.getMessage() == null) {
            if (unwrappedT instanceof CrateException && unwrappedT.getCause() != null) {
                unwrappedT = unwrappedT.getCause();   // use cause because it contains a more meaningful error in most cases
            }
            StackTraceElement[] stackTraceElements = unwrappedT.getStackTrace();
            if (stackTraceElements.length > 0) {
                return String.format(Locale.ENGLISH,
                                     "%s in %s",
                                     unwrappedT.getClass().getSimpleName(),
                                     stackTraceElements[0]);
            } else {
                return "Error in " + unwrappedT.getClass().getSimpleName();
            }
        } else {
            return unwrappedT.getMessage();
        }
    }

    public static boolean isShardFailure(Throwable e) {
        e = SQLExceptions.unwrap(e);
        return e instanceof ShardNotFoundException || e instanceof IllegalIndexShardStateException;
    }

    public static RuntimeException prepareForClientTransmission(AccessControl accessControl, Throwable e) {
        Throwable unwrappedError = SQLExceptions.unwrap(e);
        e = esToCrateException(unwrappedError);
        try {
            accessControl.ensureMaySee(e);
        } catch (Exception mpe) {
            e = mpe;
        }
        return Exceptions.toRuntimeException(e);
    }

    private static Throwable esToCrateException(Throwable unwrappedError) {
        if (unwrappedError instanceof IllegalArgumentException || unwrappedError instanceof ParsingException) {
            return new SQLParseException(unwrappedError.getMessage(), (Exception) unwrappedError);
        } else if (unwrappedError instanceof UnsupportedOperationException) {
            return new UnsupportedFeatureException(unwrappedError.getMessage(), (Exception) unwrappedError);
        } else if (isDocumentAlreadyExistsException(unwrappedError)) {
            return new DuplicateKeyException(
                ((EngineException) unwrappedError).getIndex().getName(),
                "A document with the same primary key exists already", unwrappedError);
        } else if (unwrappedError instanceof ResourceAlreadyExistsException) {
            return new RelationAlreadyExists(((ResourceAlreadyExistsException) unwrappedError).getIndex().getName(), unwrappedError);
        } else if ((unwrappedError instanceof InvalidIndexNameException)) {
            if (unwrappedError.getMessage().contains("already exists as alias")) {
                // treat an alias like a table as aliases are not officially supported
                return new RelationAlreadyExists(((InvalidIndexNameException) unwrappedError).getIndex().getName(),
                    unwrappedError);
            }
            return new InvalidRelationName(((InvalidIndexNameException) unwrappedError).getIndex().getName(), unwrappedError);
        } else if (unwrappedError instanceof InvalidIndexTemplateException) {
            PartitionName partitionName = PartitionName.fromIndexOrTemplate(((InvalidIndexTemplateException) unwrappedError).name());
            return new InvalidRelationName(partitionName.relationName().fqn(), unwrappedError);
        } else if (unwrappedError instanceof IndexNotFoundException) {
            return new RelationUnknown(((IndexNotFoundException) unwrappedError).getIndex().getName(), unwrappedError);
        } else if (unwrappedError instanceof InterruptedException) {
            return JobKilledException.of(unwrappedError.getMessage());
        } else if (unwrappedError instanceof RepositoryMissingException) {
            return new RepositoryUnknownException(((RepositoryMissingException) unwrappedError).repository());
        } else if (unwrappedError instanceof InvalidSnapshotNameException) {
            return new SnapshotNameInvalidException(unwrappedError.getMessage());
        } else if (unwrappedError instanceof SnapshotMissingException) {
            SnapshotMissingException snapshotException = (SnapshotMissingException) unwrappedError;
            return new SnapshotUnknownException(snapshotException.getRepositoryName(), snapshotException.getSnapshotName(), unwrappedError);
        } else if (unwrappedError instanceof SnapshotCreationException) {
            SnapshotCreationException creationException = (SnapshotCreationException) unwrappedError;
            return new SnapshotAlreadyExistsException(creationException.getRepositoryName(), creationException.getSnapshotName());
        }
        return unwrappedError;
    }

    public static boolean isDocumentAlreadyExistsException(Throwable e) {
        return e instanceof VersionConflictEngineException
                   && e.getMessage().contains("document already exists");
    }

    /**
     * Converts a possible ES exception to a Crate one and returns the message.
     * The message will not contain any information about possible nested exceptions.
     */
    public static String userFriendlyCrateExceptionTopOnly(Throwable e) {
        return esToCrateException(e).getMessage();
    }
}
