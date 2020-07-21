/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.protocols.postgres;

import io.crate.action.sql.SQLActionException;
import io.crate.exceptions.CircuitBreakingException;
import io.crate.exceptions.ConflictException;
import io.crate.exceptions.CrateException;
import io.crate.exceptions.DuplicateKeyException;
import io.crate.exceptions.InvalidRelationName;
import io.crate.exceptions.JobKilledException;
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.exceptions.ReadOnlyException;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.RepositoryUnknownException;
import io.crate.exceptions.ResourceUnknownException;
import io.crate.exceptions.SQLExceptions;
import io.crate.exceptions.SQLParseException;
import io.crate.exceptions.SnapshotAlreadyExistsException;
import io.crate.exceptions.SnapshotNameInvalidException;
import io.crate.exceptions.SnapshotUnknownException;
import io.crate.exceptions.UnauthorizedException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.PartitionName;
import io.crate.sql.parser.ParsingException;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.InvalidSnapshotNameException;
import org.elasticsearch.snapshots.SnapshotCreationException;
import org.elasticsearch.snapshots.SnapshotMissingException;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.function.Consumer;

public class PGError {

    private final Throwable t;



    public static final byte[] SEVERITY_FATAL = "FATAL".getBytes(StandardCharsets.UTF_8);
    public static final byte[] SEVERITY_ERROR = "ERROR".getBytes(StandardCharsets.UTF_8);
    public static final byte[] ERROR_CODE_INVALID_AUTHORIZATION_SPECIFICATION = "28000".getBytes(StandardCharsets.UTF_8);
    public static final byte[] ERROR_CODE_FEATURE_NOT_SUPPORTED = "0A000".getBytes(StandardCharsets.UTF_8);
    public static final byte[] ERROR_CODE_INTERNAL_ERROR = "XX000".getBytes(StandardCharsets.UTF_8);

    public static SQLActionException createSQLActionException(Throwable e, Consumer<Throwable> maskSensitiveInformation) {
        // ideally this method would be a static factory method in SQLActionException,
        // but that would pull too many dependencies for the client

        if (e instanceof SQLActionException) {
            return (SQLActionException) e;
        }
        Throwable unwrappedError = SQLExceptions.unwrap(e);
        e = esToCrateException(unwrappedError);
        try {
            maskSensitiveInformation.accept(e);
        } catch (Exception mpe) {
            e = mpe;
        }

        int errorCode = 5000;
        HttpResponseStatus httpStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        if (e instanceof CrateException) {
            CrateException crateException = (CrateException) e;
            if (e instanceof ValidationException) {
                errorCode = 4000 + crateException.errorCode();
                httpStatus = HttpResponseStatus.BAD_REQUEST;
            } else if (e instanceof UnauthorizedException) {
                errorCode = 4010 + crateException.errorCode();
                httpStatus = HttpResponseStatus.UNAUTHORIZED;
            } else if (e instanceof ReadOnlyException) {
                errorCode = 4030 + crateException.errorCode();
                httpStatus = HttpResponseStatus.FORBIDDEN;
            } else if (e instanceof ResourceUnknownException) {
                errorCode = 4040 + crateException.errorCode();
                httpStatus = HttpResponseStatus.NOT_FOUND;
            } else if (e instanceof ConflictException) {
                errorCode = 4090 + crateException.errorCode();
                httpStatus = HttpResponseStatus.CONFLICT;
            } else if (e instanceof UnhandledServerException) {
                errorCode = 5000 + crateException.errorCode();
            }
        } else if (e instanceof ParsingException) {
            errorCode = 4000;
            httpStatus = HttpResponseStatus.BAD_REQUEST;
        } else if (e instanceof MapperParsingException) {
            errorCode = 4000;
            httpStatus = HttpResponseStatus.BAD_REQUEST;
        }

        String message = e.getMessage();
        if (message == null) {
            if (e instanceof CrateException && e.getCause() != null) {
                e = e.getCause();   // use cause because it contains a more meaningful error in most cases
            }
            StackTraceElement[] stackTraceElements = e.getStackTrace();
            if (stackTraceElements.length > 0) {
                message = String.format(Locale.ENGLISH, "%s in %s", e.getClass().getSimpleName(), stackTraceElements[0]);
            } else {
                message = "Error in " + e.getClass().getSimpleName();
            }
        } else {
            message = e.getClass().getSimpleName() + ": " + message;
        }

        StackTraceElement[] usefulStacktrace =
            e instanceof MissingPrivilegeException ? e.getStackTrace() : unwrappedError.getStackTrace();
        return new SQLActionException(message, errorCode, httpStatus, usefulStacktrace);
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
        } else if (unwrappedError instanceof org.elasticsearch.common.breaker.CircuitBreakingException) {
            return new CircuitBreakingException(unwrappedError.getMessage());
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

    static byte[] errorResponse(Throwable throwable) {
        // See https://www.postgresql.org/docs/9.2/static/errcodes-appendix.html
        // need to add a throwable -> error code mapping later on
        byte[] errorCode;
        if (throwable instanceof IllegalArgumentException || throwable instanceof UnsupportedOperationException) {
            // feature_not_supported
            errorCode = PGError.ERROR_CODE_FEATURE_NOT_SUPPORTED;
        } else {
            // internal_error
            errorCode = PGError.ERROR_CODE_INTERNAL_ERROR;
        }
        return errorCode;
    }

    private static Throwable esToCrateException(Throwable error) {
        if (error instanceof  SQLParseException) {

        }
        else if (error instanceof  UnsupportedFeatureException) {

        }
        else if (error instanceof  DuplicateKeyException) {

        }

        else if (error instanceof  RelationAlreadyExists) {

        }

        else if (error instanceof  InvalidRelationName) {

        }

        else if (error instanceof  RelationUnknown) {

        }

        else if (error instanceof  CircuitBreakingException) {

        }

        else if (error instanceof  JobKilledException) {

        }

        else if (error instanceof  RepositoryUnknownException) {

        }

        else if (error instanceof  SnapshotNameInvalidException) {

        }

        else if (error instanceof  SnapshotUnknownException) {

        }

        else if (error instanceof  SnapshotAlreadyExistsException) {

        }

        else if (error instanceof IllegalArgumentException || error instanceof ParsingException) {
            return new SQLParseException(error.getMessage(), (Exception) error);
        } else if (error instanceof UnsupportedOperationException) {
            return new UnsupportedFeatureException(error.getMessage(), (Exception) error);
        } else if (SQLExceptions.isDocumentAlreadyExistsException(error)) {
            return new DuplicateKeyException(
                ((EngineException) error).getIndex().getName(),
                "A document with the same primary key exists already", error);
        } else if (error instanceof ResourceAlreadyExistsException) {
            return new RelationAlreadyExists(((ResourceAlreadyExistsException) error).getIndex().getName(), error);
        } else if ((error instanceof InvalidIndexNameException)) {
            if (error.getMessage().contains("already exists as alias")) {
                // treat an alias like a table as aliases are not officially supported
                return new RelationAlreadyExists(((InvalidIndexNameException) error).getIndex().getName(),
                                                 error);
            }
            return new InvalidRelationName(((InvalidIndexNameException) error).getIndex().getName(), error);
        } else if (error instanceof InvalidIndexTemplateException) {
            PartitionName partitionName = PartitionName.fromIndexOrTemplate(((InvalidIndexTemplateException) error).name());
            return new InvalidRelationName(partitionName.relationName().fqn(), error);
        } else if (error instanceof IndexNotFoundException) {
            return new RelationUnknown(((IndexNotFoundException) error).getIndex().getName(), error);
        } else if (error instanceof org.elasticsearch.common.breaker.CircuitBreakingException) {
            return new CircuitBreakingException(error.getMessage());
        } else if (error instanceof InterruptedException) {
            return JobKilledException.of(error.getMessage());
        } else if (error instanceof RepositoryMissingException) {
            return new RepositoryUnknownException(((RepositoryMissingException) error).repository());
        } else if (error instanceof InvalidSnapshotNameException) {
            return new SnapshotNameInvalidException(error.getMessage());
        } else if (error instanceof SnapshotMissingException) {
            SnapshotMissingException snapshotException = (SnapshotMissingException) error;
            return new SnapshotUnknownException(snapshotException.getRepositoryName(), snapshotException.getSnapshotName(), error);
        } else if (error instanceof SnapshotCreationException) {
            SnapshotCreationException creationException = (SnapshotCreationException) error;
            return new SnapshotAlreadyExistsException(creationException.getRepositoryName(), creationException.getSnapshotName());
        }
        return error;
    }
}
