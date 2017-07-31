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

package io.crate.metadata;

import com.google.common.base.Preconditions;
import io.crate.types.ArrayType;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import io.crate.types.SingleColumnTableType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Static constructors for generating signature operators and argument type matchers to be used
 * with {@link FunctionResolver#getSignature}.
 */
public class Signature {

    public static final SignatureOperator EMPTY = dataTypes -> dataTypes.isEmpty() ? dataTypes : null;
    public static final SignatureOperator SIGNATURES_SINGLE_NUMERIC = of(ArgMatcher.NUMERIC);
    public static final SignatureOperator SIGNATURES_SINGLE_ANY = numArgs(1);
    public static final SignatureOperator SIGNATURES_ALL_OF_SAME = withStrictVarArgs(ArgMatcher.ANY);

    public static final Predicate<DataType> IS_UNDEFINED = DataTypes.UNDEFINED::equals;

    /**
     * Creates an operator which takes its argument matchers from an iterator. The size of the
     * validated arguments is not checked, however it needs to be greater or equal to the
     * number of matchers returned by the iterator;
     *
     * @param matchers the argument matcher iterator
     * @return a new signature operator
     */
    public static SignatureOperator ofIterable(Iterable<ArgMatcher> matchers) {
        return new IterableArgs(matchers);
    }

    /**
     * Creates an operator which matches if the given positional argument matchers match the signature.
     *
     * @param matchers positional argument matchers
     * @return a new signature operator
     */
    public static SignatureOperator of(ArgMatcher... matchers) {
        if (matchers.length == 0) {
            return EMPTY;
        }
        return new StandardArgs(matchers);
    }

    /**
     * Returns an operator which matches and returns its input if the number of arguments in a signature are in a
     * specific range.
     *
     * @param minSize the minimum number of arguments the signature is required to have.
     * @param maxSize the maximum number of arguments the signature is required to have.
     * @return an new signature operator
     */
    public static SignatureOperator numArgs(int minSize, int maxSize) {
        Preconditions.checkArgument(minSize <= maxSize, "minSize needs to be smaller than maxSize");
        return dataTypes -> (dataTypes.size() >= minSize && dataTypes.size() <= maxSize) ? dataTypes : null;
    }

    /**
     * Returns an operator which matches and returns its input if the number of arguments matches.
     *
     * @param numArgs the number of arguments the signature is required to have.
     * @return an new signature operator
     */
    public static SignatureOperator numArgs(int numArgs) {
        Preconditions.checkArgument(numArgs >= 0, "numArgs must not be negative");
        if (numArgs == 0) {
            return EMPTY;
        }
        return in -> in.size() == numArgs ? in : null;
    }

    /**
     * Returns an operator which allows for a variable number of arguments. It matches if the given positional argument
     * matchers match the signature. Additionally the final argument matcher is allowed to be repeated.
     *
     * @param matchers positional argument matchers
     * @return a new signature operator
     */
    public static SignatureOperator withLenientVarArgs(ArgMatcher... matchers) {
        Preconditions.checkArgument(matchers.length > 0, "at least one matcher is required");
        return new StandardArgs(true, false, matchers);
    }

    /**
     * Returns an operator which allows for a variable number of arguments. It is the more strict version of
     * {@link #withLenientVarArgs(ArgMatcher...)}. It checks that the actual variable arguments are of the same type
     * and also normalizes undefined variable argument types if at least one of them is defined.
     *
     * @param matchers positional argument matchers
     * @return a new signature operator
     */
    public static SignatureOperator withStrictVarArgs(ArgMatcher... matchers) {
        Preconditions.checkArgument(matchers.length > 0, "at least one matcher is required");
        return new StandardArgs(true, true, matchers);
    }

    /**
     * Returns an operator which matches exactly the given types. If a type is undefined it will be rewritten to
     * the according type at that position.
     *
     * @param dataTypes the list of types to match
     * @return a new signature operator
     */
    public static SignatureOperator of(List<DataType> dataTypes) {
        if (dataTypes.isEmpty()) {
            return EMPTY;
        }
        ArgMatcher[] matchers = new ArgMatcher[dataTypes.size()];
        for (int i = 0; i < matchers.length; i++) {
            matchers[i] = ArgMatcher.rewriteTo(dataTypes.get(i));
        }
        return new StandardArgs(matchers);
    }

    /**
     * Convinience method for calling {@link #of(List<DataType>)} with varArgs.
     */
    public static SignatureOperator of(DataType... dataTypes) {
        return of(Arrays.asList(dataTypes));
    }

    public interface SignatureOperator extends UnaryOperator<List<DataType>> {

        /**
         * Adds a precondition before the operator which should evaluate to true in order for the operator
         * to be evaluated.
         *
         * @param predicate a predicate which represents the precondition
         * @return a new operator with the predicate prepended
         */
        default SignatureOperator preCondition(Predicate<List<DataType>> predicate) {
            return dataTypes -> predicate.test(dataTypes) ? this.apply(dataTypes) : null;
        }

        /**
         * Adds a secondary operator to this operator, which is executed with the result of this operator if a match
         * occurs.
         *
         * @param secondary the operator which will be evaluated with this operator's success result
         * @return a new operator with the secondary appended
         */
        default SignatureOperator and(SignatureOperator secondary) {
            return dataTypes -> {
                List<DataType> n = this.apply(dataTypes);
                if (n != null) {
                    return secondary.apply(n);
                }
                return null;
            };
        }

    }

    /**
     * Interface for validating and normalizing an argument type of a function signature.
     * <p>
     * This interface is a unary operator for {@link DataType} which returns a normalized type if the given input type
     * is valid. Implementations can either pass through the input or replace the type if normalization is required.
     * For example if the input type is null implementations might return a specific type if possible.
     * <p>
     * The operator returns null if the input does not match its criteria.
     */
    public interface ArgMatcher extends UnaryOperator<DataType> {

        ArgMatcher ANY = dt -> dt;
        ArgMatcher NUMERIC = of(DataTypes.NUMERIC_PRIMITIVE_TYPES::contains);

        ArgMatcher ANY_ARRAY = of(dt -> dt instanceof ArrayType);
        ArgMatcher ANY_SET = of(dt -> dt instanceof SetType);
        ArgMatcher ANY_SINGLE_COLUMN = of(dt -> dt instanceof SingleColumnTableType);
        ArgMatcher ANY_COLLECTION = of(dt -> dt instanceof CollectionType);

        ArgMatcher STRING = rewriteTo(DataTypes.STRING);
        ArgMatcher BOOLEAN = rewriteTo(DataTypes.BOOLEAN);
        ArgMatcher INTEGER = rewriteTo(DataTypes.INTEGER);
        ArgMatcher OBJECT = rewriteTo(DataTypes.OBJECT);

        /**
         * Creates a matcher witch matches any of the given types or the undefined type.
         *
         * @param allowedTypes the allowed types
         * @return a new argument matcher
         */
        static ArgMatcher of(DataType... allowedTypes) {
            return dt -> {
                for (DataType allowedType : allowedTypes) {
                    if (allowedType.equals(dt)) {
                        return dt;
                    }
                }
                return IS_UNDEFINED.test(dt) ? dt : null;
            };
        }

        /**
         * Creates a matcher which matches if any of the given input predicates matches or the input is of type undefined.
         *
         * @param predicates the predicates to match
         * @return a new argument matcher
         */
        @SafeVarargs
        static ArgMatcher of(final Predicate<DataType>... predicates) {
            return dt -> {
                for (Predicate<DataType> predicate : predicates) {
                    if (predicate.test(dt)) {
                        return dt;
                    }
                }
                return IS_UNDEFINED.test(dt) ? dt : null;
            };
        }


        /**
         * Creates a matcher which matches one given type and rewrites the undefined type to the required type.
         *
         * @param targetType the type to match agains
         * @return a new argument matcher
         */
        static ArgMatcher rewriteTo(final DataType targetType) {
            return dt -> {
                if (targetType.equals(dt) || IS_UNDEFINED.test(dt)) {
                    return targetType;
                }
                return null;
            };
        }
    }

    /**
     * Runs matchers consumed from an iterator
     */
    private static class IterableArgs implements SignatureOperator {

        private final Iterable<ArgMatcher> expected;

        private IterableArgs(Iterable<ArgMatcher> expected) {
            this.expected = expected;
        }

        @Override
        public List<DataType> apply(List<DataType> dataTypes) {
            Iterator<ArgMatcher> iter = expected.iterator();
            for (DataType dataType : dataTypes) {
                if (!iter.hasNext()) return null;
                DataType repl = iter.next().apply(dataType);
                if (repl == null) {
                    return null;
                } else if (repl != dataType) {
                    return mutatedCopy(dataTypes);
                }
            }
            return dataTypes;
        }

        private List<DataType> mutatedCopy(List<DataType> in) {
            ArrayList<DataType> out = new ArrayList<>(in.size());
            Iterator<ArgMatcher> iter = expected.iterator();
            for (DataType dataType : in) {
                if (!iter.hasNext()) return null;
                DataType repl = iter.next().apply(dataType);
                if (repl == null) {
                    return null;
                }
                out.add(repl);
            }
            return out;
        }
    }

    /**
     * A signature operator which supports standard signatures as in Java, including varargs.
     */
    private static class StandardArgs implements Signature.SignatureOperator {

        private final ArgMatcher[] matchers;
        private final boolean varArgs;
        private final boolean checkVarArgTypes;

        private StandardArgs(ArgMatcher[] matchers) {
            this(false, false, matchers);
        }

        private StandardArgs(boolean hasVarArgs, boolean strictVarArgTypes, ArgMatcher[] matchers) {
            assert matchers.length > 0 : "VarArgs requires at least one matcher";
            assert !strictVarArgTypes || hasVarArgs : "checkVarargTypes is not applicable if not hasVarArgs";
            this.matchers = matchers;
            this.varArgs = hasVarArgs;
            this.checkVarArgTypes = strictVarArgTypes;
        }

        /**
         * Checks if all non undefined types in the given list are the same, beginning @start index and returns the
         * common type. If all types are undefined, undefined is returned.
         */
        private static DataType getCommonType(List<DataType> dataTypes, int start) {
            DataType commonType = null;
            for (int i = start; i < dataTypes.size(); i++) {
                DataType dataType = dataTypes.get(i);
                if (IS_UNDEFINED.test(dataType)) {
                    continue;
                }
                if (commonType == null) {
                    commonType = dataType;
                } else if (!commonType.equals(dataType)) {
                    return null;
                }
            }
            return commonType == null ? DataTypes.UNDEFINED : commonType;
        }

        @Override
        public List<DataType> apply(List<DataType> dataTypes) {
            ArgMatcher varArgMatcher = matchers[matchers.length - 1];
            if (dataTypes.size() != matchers.length) {
                if (varArgs && dataTypes.size() > matchers.length) {
                    if (checkVarArgTypes) {
                        DataType commonType = getCommonType(dataTypes, matchers.length - 1);
                        if (commonType == null) {
                            return null;
                        } else if (!IS_UNDEFINED.test(commonType)) {
                            // enforce the common type in varArgs
                            varArgMatcher = dt -> commonType;
                        }
                    }
                } else {
                    return null;
                }
            }
            return normalize(dataTypes, varArgMatcher);
        }

        @Nullable
        private List<DataType> normalize(List<DataType> dataTypes, ArgMatcher varArgMatcher){
            for (int i = 0; i < dataTypes.size(); i++) {
                ArgMatcher matcher = i < matchers.length - 1 ? matchers[i] : varArgMatcher;
                DataType dataType = dataTypes.get(i);
                DataType repl = matcher.apply(dataType);
                if (repl == null) {
                    return null;
                } else if (repl != dataType) {
                    return mutatedCopy(dataTypes, varArgMatcher);
                }
            }
            return dataTypes;
        }

        private List<DataType> mutatedCopy(List<DataType> in, ArgMatcher varArgMatcher) {
            ArrayList<DataType> out = new ArrayList<>(in.size());
            for (int i = 0; i < in.size(); i++) {
                ArgMatcher matcher = i < matchers.length - 1 ? matchers[i] : varArgMatcher;
                DataType dt = matcher.apply(in.get(i));
                if (dt == null) {
                    return null;
                }
                out.add(dt);
            }
            return out;
        }
    }
}
