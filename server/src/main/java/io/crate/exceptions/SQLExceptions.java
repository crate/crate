/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.util.concurrent.UncheckedExecutionException;
import io.crate.action.sql.SQLActionException;
import io.crate.auth.user.AccessControl;
import io.crate.metadata.PartitionName;
import io.crate.sql.parser.ParsingException;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.InvalidSnapshotNameException;
import org.elasticsearch.snapshots.SnapshotCreationException;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.transport.TransportException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

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
        return Objects.requireNonNullElse(unwrappedT.getMessage(), unwrappedT.toString());
    }

    public static boolean isShardFailure(Throwable e) {
        e = SQLExceptions.unwrap(e);
        return e instanceof ShardNotFoundException || e instanceof IllegalIndexShardStateException;
    }

    public static Function<Throwable, Exception> forWireTransmission(AccessControl accessControl) {
        return e -> createSQLActionException(e, accessControl::ensureMaySee);
    }

    public static RuntimeException forWireTransmission(AccessControl accessControl, Throwable e) {
        return createSQLActionException(e, accessControl::ensureMaySee);
    }

    /**
     * Create a {@link SQLActionException} out of a {@link Throwable}.
     * If concrete {@link ElasticsearchException} is found, first transform it
     * to a {@link CrateException}
     */
    public static RuntimeException createSQLActionException(Throwable e, Consumer<Throwable> maskSensitiveInformation) {
        // ideally this method would be a static factory method in SQLActionException,
        // but that would pull too many dependencies for the client
        Throwable unwrappedError = SQLExceptions.unwrap(e);
        try {
            maskSensitiveInformation.accept(e);
        } catch (Exception mpe) {
            unwrappedError = mpe;
        }
        return new RuntimeException(unwrappedError);
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
        return e.getMessage();
    }
}
