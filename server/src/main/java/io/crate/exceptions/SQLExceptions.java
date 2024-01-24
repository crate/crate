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

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NoSeedNodeLeftException;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.RemoteTransportException;
import org.jetbrains.annotations.Nullable;

import io.crate.auth.AccessControl;
import io.crate.common.exceptions.Exceptions;
import io.crate.metadata.PartitionName;
import io.crate.sql.parser.ParsingException;

public class SQLExceptions {

    private static final Logger LOGGER = LogManager.getLogger(SQLExceptions.class);

    private static final Predicate<Throwable> EXCEPTIONS_TO_UNWRAP = throwable ->
        throwable instanceof RemoteTransportException ||
        throwable instanceof CompletionException ||
        throwable instanceof UncategorizedExecutionException ||
        throwable instanceof ElasticsearchWrapperException ||
        throwable instanceof ExecutionException ||
        throwable instanceof NotSerializableExceptionWrapper ||
        throwable.getClass() == RuntimeException.class;

    /**
     * Removes wrapper exceptions like {@link CompletionException}
     */
    public static Throwable unwrap(Throwable t) {
        int counter = 0;
        Throwable result = t;
        while (EXCEPTIONS_TO_UNWRAP.test(result)) {
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

    public static RestStatus status(Throwable t) {
        if (t != null) {
            if (t instanceof ElasticsearchException) {
                return ((ElasticsearchException) t).status();
            } else if (t instanceof IllegalArgumentException) {
                return RestStatus.BAD_REQUEST;
            } else if (t instanceof EsRejectedExecutionException) {
                return RestStatus.TOO_MANY_REQUESTS;
            }
        }
        return RestStatus.INTERNAL_SERVER_ERROR;
    }

    /**
     * Throws a runtime exception with all given exceptions added as suppressed.
     * If the given list is empty no exception is thrown
     */
    public static <T extends Throwable> void maybeThrowRuntimeAndSuppress(List<T> exceptions) {
        T main = null;
        for (T ex : exceptions) {
            main = Exceptions.useOrSuppress(main, ex);
        }
        if (main != null) {
            throw new ElasticsearchException(main);
        }
    }

    private static final List<Class<? extends IOException>> CORRUPTION_EXCEPTIONS =
        List.of(CorruptIndexException.class, IndexFormatTooOldException.class, IndexFormatTooNewException.class);

    /**
     * Looks at the given Throwable's and its cause(s) as well as any suppressed exceptions on the Throwable as well as its causes
     * and returns the first corruption indicating exception (as defined by {@link #CORRUPTION_EXCEPTIONS}) it finds.
     * @param t Throwable
     * @return Corruption indicating exception if one is found, otherwise {@code null}
     */
    public static IOException unwrapCorruption(Throwable t) {
        return t == null ? null : Exceptions.<IOException>firstCauseOrSuppressed(t, cause -> {
            for (Class<?> clazz : CORRUPTION_EXCEPTIONS) {
                if (clazz.isInstance(cause)) {
                    return true;
                }
            }
            return false;
        }).orElse(null);
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

    /***
     * @return true if the error may be temporary; E.g. a network error, a shard initializing or a node booting up
     */
    public static boolean maybeTemporary(Throwable t) {
        return t instanceof NodeNotConnectedException
            || t instanceof NodeClosedException
            || t instanceof NodeDisconnectedException
            || t instanceof ConnectTransportException
            || t instanceof ConnectException
            || t instanceof ClusterBlockException
            || t instanceof NoSeedNodeLeftException
            || t instanceof IndexNotFoundException
            || t instanceof NoShardAvailableActionException
            || t instanceof AlreadyClosedException
            || t instanceof ElasticsearchTimeoutException;
    }

    public static Throwable prepareForClientTransmission(AccessControl accessControl, Throwable e) {
        Throwable unwrappedError = SQLExceptions.unwrap(e);
        e = esToCrateException(unwrappedError);
        try {
            accessControl.ensureMaySee(e);
        } catch (Exception mpe) {
            e = mpe;
        }
        return e;
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
            return new RelationAlreadyExists(((ResourceAlreadyExistsException) unwrappedError).getIndex(), unwrappedError);
        } else if ((unwrappedError instanceof InvalidIndexNameException)) {
            if (unwrappedError.getMessage().contains("already exists as alias")) {
                // treat an alias like a table as aliases are not officially supported
                return new RelationAlreadyExists(((InvalidIndexNameException) unwrappedError).getIndex(),
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
        }
        return unwrappedError;
    }

    public static boolean isDocumentAlreadyExistsException(Throwable e) {
        return e instanceof VersionConflictEngineException
                   && e.getMessage().contains("document already exists");
    }
}
