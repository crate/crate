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

package io.crate.expression.symbol;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.Nullable;

import io.crate.exceptions.ConversionException;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.scalar.cast.ExplicitCastFunction;
import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.expression.scalar.cast.TryCastFunction;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Reference;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.Expression;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public interface Symbol extends Writeable, Accountable {

    public static final Predicate<Symbol> IS_COLUMN = s -> s instanceof ScopedSymbol || s instanceof Reference;
    public static final Predicate<Symbol> IS_CORRELATED_SUBQUERY = s -> s instanceof SelectSymbol selectSymbol && selectSymbol.isCorrelated();

    public static boolean isLiteral(Symbol symbol, DataType<?> expectedType) {
        return symbol.symbolType() == SymbolType.LITERAL && symbol.valueType().equals(expectedType);
    }

    public static boolean hasLiteralValue(Symbol symbol, Object value) {
        while (symbol instanceof AliasSymbol alias) {
            symbol = alias.symbol();
        }
        return symbol instanceof Literal<?> literal && Objects.equals(literal.value(), value);
    }

    @Nullable
    public static Symbol nullableFromStream(StreamInput in) throws IOException {
        return in.readBoolean() ? fromStream(in) : null;
    }

    public static void nullableToStream(@Nullable Symbol symbol, StreamOutput out) throws IOException {
        if (symbol == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            toStream(symbol, out);
        }
    }

    public static void toStream(Symbol symbol, StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_4_2_0) && symbol instanceof AliasSymbol aliasSymbol) {
            toStream(aliasSymbol.symbol(), out);
        } else {
            int ordinal = symbol.symbolType().ordinal();
            out.writeVInt(ordinal);
            symbol.writeTo(out);
        }
    }

    public static Symbol fromStream(StreamInput in) throws IOException {
        return SymbolType.VALUES.get(in.readVInt()).newInstance(in);
    }

    SymbolType symbolType();

    <C, R> R accept(SymbolVisitor<C, R> visitor, C context);

    DataType<?> valueType();

    /**
     * Returns true if the tree is expected to return the same value given the same inputs
     */
    default boolean isDeterministic() {
        return true;
    }

    /**
     * Returns true if the tree contains a {@link Reference} or {@link ScopedSymbol}
     * column matching the argument.
     */
    default boolean hasColumn(ColumnIdent column) {
        return any(s ->
            s instanceof Reference ref && ref.column().equals(column) ||
            s instanceof ScopedSymbol field && field.column().equals(column));
    }

    /**
     * Returns true if the tree contains the given function type
     */
    default boolean hasFunctionType(FunctionType type) {
        return any(s -> s instanceof Function fn && fn.signature.getType().equals(type));
    }

    /**
     * <p>
     * Returns true if the given predicate matches on any node in the symbol tree.
     * </p>
     *
     * Does not cross relations:
     * <ul>
     * <li>Symbols within the relation of a SelectSymbol are not visited</li>
     * <li>OuterColumn is visited, but its linked symbol isn't. because it belongs to a parent relation</li>
     * </ul>
     */
    default boolean any(Predicate<? super Symbol> predicate) {
        return predicate.test(this);
    }

    /**
     * Visits all instances of clazz in a tree.
     * Does not cross relations. See {@link #any(Predicate)} for details
     */
    default <T extends Symbol> void visit(Class<T> clazz, Consumer<? super T> consumer) {
        any(node -> {
            if (clazz.isInstance(node)) {
                consumer.accept(clazz.cast(node));
            }
            return false;
        });
    }

    /**
     * Visits all nodes matching the predicate in a tree.
     * Does not cross relations. See {@link #any(Predicate)} for details
     */
    default void visit(Predicate<? super Symbol> predicate, Consumer<? super Symbol> consumer) {
        any(node -> {
            if (predicate.test(node)) {
                consumer.accept(node);
            }
            return false;
        });
    }

    /**
     * Returns a {@link ColumnIdent} that can be used to represent the Symbol.
     */
    default ColumnIdent toColumn() {
        return ColumnIdent.of(toString(Style.UNQUALIFIED));
    }

    default ColumnDefinition<Expression> toColumnDefinition() {
        return new ColumnDefinition<>(
            toColumn().sqlFqn(), // allow ObjectTypes to return col name in subscript notation
            valueType().toColumnType(ColumnPolicy.DYNAMIC, null),
            List.of()
        );
    }

    /**
     * Casts this Symbol to a new {@link DataType} by wrapping an implicit cast
     * function around it if no {@link CastMode} modes are provided.
     * <p>
     * Subclasses of this class may provide another cast methods.
     *
     * @param targetType The resulting data type after applying the cast
     * @param modes      One of the {@link CastMode} types.
     * @return An instance of {@link Function} which casts this symbol.
     */
    default Symbol cast(DataType<?> targetType, CastMode... modes) {
        if (targetType.equals(valueType())) {
            return this;
        } else if (ArrayType.unnest(targetType).equals(DataTypes.UNTYPED_OBJECT)
                   && valueType().id() == targetType.id()) {
            return this;
        } else if (ArrayType.unnest(targetType).equals(DataTypes.NUMERIC)
                   && valueType().id() == DataTypes.NUMERIC.id()) {
            // Do not cast numerics to unscaled numerics because we do not want to loose precision + scale
            return this;
        }
        return generateCastFunction(this, targetType, modes);
    }

    /**
     * If the symbol is a cast function it drops it (only on root)
     **/
    default Symbol uncast() {
        return this;
    }

    /**
     * Converts the symbol to a string format where the string output should be able to be parsed to generate the original symbol.
     * NOTE: remember to prefer quoted variants over non quoted when converting.
     */
    String toString(Style style);

    private static Symbol generateCastFunction(Symbol sourceSymbol,
                                              DataType<?> targetType,
                                              CastMode... castModes) {
        var modes = Set.of(castModes);
        assert !modes.containsAll(List.of(CastMode.EXPLICIT, CastMode.IMPLICIT))
            : "explicit and implicit cast modes are mutually exclusive";

        DataType<?> sourceType = sourceSymbol.valueType();
        if (!sourceType.isConvertableTo(targetType, modes.contains(CastMode.EXPLICIT))) {
            throw new ConversionException(sourceType, targetType);
        }

        if (modes.contains(CastMode.TRY) || modes.contains(CastMode.EXPLICIT)) {
            // Currently, it is not possible to resolve a function based on
            // its return type. For instance, it is not possible to generate
            // an object cast function with the object return type which inner
            // types have to be considered as well. Therefore, to bypass this
            // limitation we encode the return type info as the second function
            // argument.
            var signature = modes.contains(CastMode.TRY)
                ? TryCastFunction.SIGNATURE
                : ExplicitCastFunction.SIGNATURE;

            // a literal with a NULL value is passed as an argument
            // to match the method signature
            List<Symbol> arguments = List.of(sourceSymbol, Literal.of(targetType, null));
            return new Function(signature, arguments, targetType);
        } else {
            return new Function(
                ImplicitCastFunction.SIGNATURE,
                List.of(
                    sourceSymbol,
                    Literal.of(targetType.getTypeSignature().toString())
                ),
                targetType
            );
        }
    }
}
