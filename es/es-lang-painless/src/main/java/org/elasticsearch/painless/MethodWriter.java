/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;

import static org.elasticsearch.painless.WriterConstants.CHAR_TO_STRING;
import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_HANDLE;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_BOOLEAN;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_BYTE_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_BYTE_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_CHAR_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_CHAR_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_DOUBLE_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_DOUBLE_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_FLOAT_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_FLOAT_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_INT_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_INT_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_LONG_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_LONG_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_SHORT_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_SHORT_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_UTIL_TYPE;
import static org.elasticsearch.painless.WriterConstants.INDY_STRING_CONCAT_BOOTSTRAP_HANDLE;
import static org.elasticsearch.painless.WriterConstants.LAMBDA_BOOTSTRAP_HANDLE;
import static org.elasticsearch.painless.WriterConstants.MAX_INDY_STRING_CONCAT_ARGS;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_BOOLEAN;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_CHAR;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_FLOAT;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_INT;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_LONG;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_OBJECT;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_STRING;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_CONSTRUCTOR;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_TOSTRING;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_TYPE;
import static org.elasticsearch.painless.WriterConstants.STRING_TO_CHAR;
import static org.elasticsearch.painless.WriterConstants.STRING_TYPE;
import static org.elasticsearch.painless.WriterConstants.UTILITY_TYPE;

/**
 * Extension of {@link GeneratorAdapter} with some utility methods.
 * <p>
 * Set of methods used during the writing phase of compilation
 * shared by the nodes of the Painless tree.
 */
public final class MethodWriter extends GeneratorAdapter {
    private final BitSet statements;
    private final CompilerSettings settings;

    private final Deque<List<Type>> stringConcatArgs =
        (INDY_STRING_CONCAT_BOOTSTRAP_HANDLE == null) ?  null : new ArrayDeque<>();

    public MethodWriter(int access, Method method, ClassVisitor cw, BitSet statements, CompilerSettings settings) {
        super(Opcodes.ASM5, cw.visitMethod(access, method.getName(), method.getDescriptor(), null, null),
                access, method.getName(), method.getDescriptor());

        this.statements = statements;
        this.settings = settings;
    }

    /**
     * Marks a new statement boundary.
     * <p>
     * This is invoked for each statement boundary (leaf {@code S*} nodes).
     */
    public void writeStatementOffset(Location location) {
        int offset = location.getOffset();
        // ensure we don't have duplicate stuff going in here. can catch bugs
        // (e.g. nodes get assigned wrong offsets by antlr walker)
        assert statements.get(offset) == false;
        statements.set(offset);
    }

    /**
     * Encodes the offset into the line number table as {@code offset + 1}.
     * <p>
     * This is invoked before instructions that can hit exceptions.
     */
    public void writeDebugInfo(Location location) {
        // TODO: maybe track these in bitsets too? this is trickier...
        Label label = new Label();
        visitLabel(label);
        visitLineNumber(location.getOffset() + 1, label);
    }

    public void writeLoopCounter(int slot, int count, Location location) {
        assert slot != -1;
        writeDebugInfo(location);
        final Label end = new Label();

        iinc(slot, -count);
        visitVarInsn(Opcodes.ILOAD, slot);
        push(0);
        ifICmp(GeneratorAdapter.GT, end);
        throwException(PAINLESS_ERROR_TYPE, "The maximum number of statements that can be executed in a loop has been reached.");
        mark(end);
    }

    public void writeCast(PainlessCast cast) {
        if (cast != null) {
            if (cast.originalType == char.class && cast.targetType == String.class) {
                invokeStatic(UTILITY_TYPE, CHAR_TO_STRING);
            } else if (cast.originalType == String.class && cast.targetType == char.class) {
                invokeStatic(UTILITY_TYPE, STRING_TO_CHAR);
            } else if (cast.unboxOriginalType != null) {
                unbox(getType(cast.unboxOriginalType));
                writeCast(cast.originalType, cast.targetType);
            } else if (cast.unboxTargetType != null) {
                if (cast.originalType == def.class) {
                    if (cast.explicitCast) {
                        if      (cast.targetType == Boolean.class)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_BOOLEAN);
                        else if (cast.targetType == Byte.class)      invokeStatic(DEF_UTIL_TYPE, DEF_TO_BYTE_EXPLICIT);
                        else if (cast.targetType == Short.class)     invokeStatic(DEF_UTIL_TYPE, DEF_TO_SHORT_EXPLICIT);
                        else if (cast.targetType == Character.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_CHAR_EXPLICIT);
                        else if (cast.targetType == Integer.class)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_INT_EXPLICIT);
                        else if (cast.targetType == Long.class)      invokeStatic(DEF_UTIL_TYPE, DEF_TO_LONG_EXPLICIT);
                        else if (cast.targetType == Float.class)     invokeStatic(DEF_UTIL_TYPE, DEF_TO_FLOAT_EXPLICIT);
                        else if (cast.targetType == Double.class)    invokeStatic(DEF_UTIL_TYPE, DEF_TO_DOUBLE_EXPLICIT);
                        else {
                            throw new IllegalStateException("Illegal tree structure.");
                        }
                    } else {
                        if      (cast.targetType == Boolean.class)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_BOOLEAN);
                        else if (cast.targetType == Byte.class)      invokeStatic(DEF_UTIL_TYPE, DEF_TO_BYTE_IMPLICIT);
                        else if (cast.targetType == Short.class)     invokeStatic(DEF_UTIL_TYPE, DEF_TO_SHORT_IMPLICIT);
                        else if (cast.targetType == Character.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_CHAR_IMPLICIT);
                        else if (cast.targetType == Integer.class)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_INT_IMPLICIT);
                        else if (cast.targetType == Long.class)      invokeStatic(DEF_UTIL_TYPE, DEF_TO_LONG_IMPLICIT);
                        else if (cast.targetType == Float.class)     invokeStatic(DEF_UTIL_TYPE, DEF_TO_FLOAT_IMPLICIT);
                        else if (cast.targetType == Double.class)    invokeStatic(DEF_UTIL_TYPE, DEF_TO_DOUBLE_IMPLICIT);
                        else {
                            throw new IllegalStateException("Illegal tree structure.");
                        }
                    }
                } else {
                    writeCast(cast.originalType, cast.targetType);
                    unbox(getType(cast.unboxTargetType));
                }
            } else if (cast.boxOriginalType != null) {
                box(getType(cast.boxOriginalType));
                writeCast(cast.originalType, cast.targetType);
            } else if (cast.boxTargetType != null) {
                writeCast(cast.originalType, cast.targetType);
                box(getType(cast.boxTargetType));
            } else {
                writeCast(cast.originalType, cast.targetType);
            }
        }
    }

    private void writeCast(Class<?> from, Class<?> to) {
        if (from.equals(to)) {
            return;
        }

        if (from != boolean.class && from.isPrimitive() && to != boolean.class && to.isPrimitive()) {
            cast(getType(from), getType(to));
        } else {
            if (!to.isAssignableFrom(from)) {
                checkCast(getType(to));
            }
        }
    }

    /**
     * Proxy the box method to use valueOf instead to ensure that the modern boxing methods are used.
     */
    @Override
    public void box(Type type) {
        valueOf(type);
    }

    public static Type getType(Class<?> clazz) {
        if (clazz.isArray()) {
            Class<?> component = clazz.getComponentType();
            int dimensions = 1;

            while (component.isArray()) {
                component = component.getComponentType();
                ++dimensions;
            }

            if (component == def.class) {
                char[] braces = new char[dimensions];
                Arrays.fill(braces, '[');

                return Type.getType(new String(braces) + Type.getType(Object.class).getDescriptor());
            }
        } else if (clazz == def.class) {
            return Type.getType(Object.class);
        }

        return Type.getType(clazz);
    }

    /** Starts a new string concat.
     * @return the size of arguments pushed to stack (the object that does string concats, e.g. a StringBuilder)
     */
    public int writeNewStrings() {
        if (INDY_STRING_CONCAT_BOOTSTRAP_HANDLE != null) {
            // Java 9+: we just push our argument collector onto deque
            stringConcatArgs.push(new ArrayList<>());
            return 0; // nothing added to stack
        } else {
            // Java 8: create a StringBuilder in bytecode
            newInstance(STRINGBUILDER_TYPE);
            dup();
            invokeConstructor(STRINGBUILDER_TYPE, STRINGBUILDER_CONSTRUCTOR);
            return 1; // StringBuilder on stack
        }
    }

    public void writeAppendStrings(Class<?> clazz) {
        if (INDY_STRING_CONCAT_BOOTSTRAP_HANDLE != null) {
            // Java 9+: record type information
            stringConcatArgs.peek().add(getType(clazz));
            // prevent too many concat args.
            // If there are too many, do the actual concat:
            if (stringConcatArgs.peek().size() >= MAX_INDY_STRING_CONCAT_ARGS) {
                writeToStrings();
                writeNewStrings();
                // add the return value type as new first param for next concat:
                stringConcatArgs.peek().add(STRING_TYPE);
            }
        } else {
            // Java 8: push a StringBuilder append
            if      (clazz == boolean.class) invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_BOOLEAN);
            else if (clazz == char.class)    invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_CHAR);
            else if (clazz == byte.class  ||
                     clazz == short.class ||
                     clazz == int.class)     invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_INT);
            else if (clazz == long.class)    invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_LONG);
            else if (clazz == float.class)   invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_FLOAT);
            else if (clazz == double.class)  invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_DOUBLE);
            else if (clazz == String.class)  invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_STRING);
            else                             invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_OBJECT);
        }
    }

    public void writeToStrings() {
        if (INDY_STRING_CONCAT_BOOTSTRAP_HANDLE != null) {
            // Java 9+: use type information and push invokeDynamic
            final String desc = Type.getMethodDescriptor(STRING_TYPE,
                    stringConcatArgs.pop().stream().toArray(Type[]::new));
            invokeDynamic("concat", desc, INDY_STRING_CONCAT_BOOTSTRAP_HANDLE);
        } else {
            // Java 8: call toString() on StringBuilder
            invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_TOSTRING);
        }
    }

    /** Writes a dynamic binary instruction: returnType, lhs, and rhs can be different */
    public void writeDynamicBinaryInstruction(Location location, Class<?> returnType, Class<?> lhs, Class<?> rhs,
                                              Operation operation, int flags) {
        Type methodType = Type.getMethodType(getType(returnType), getType(lhs), getType(rhs));

        switch (operation) {
            case MUL:
                invokeDefCall("mul", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            case DIV:
                invokeDefCall("div", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            case REM:
                invokeDefCall("rem", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            case ADD:
                // if either side is primitive, then the + operator should always throw NPE on null,
                // so we don't need a special NPE guard.
                // otherwise, we need to allow nulls for possible string concatenation.
                boolean hasPrimitiveArg = lhs.isPrimitive() || rhs.isPrimitive();
                if (!hasPrimitiveArg) {
                    flags |= DefBootstrap.OPERATOR_ALLOWS_NULL;
                }
                invokeDefCall("add", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            case SUB:
                invokeDefCall("sub", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            case LSH:
                invokeDefCall("lsh", methodType, DefBootstrap.SHIFT_OPERATOR, flags);
                break;
            case USH:
                invokeDefCall("ush", methodType, DefBootstrap.SHIFT_OPERATOR, flags);
                break;
            case RSH:
                invokeDefCall("rsh", methodType, DefBootstrap.SHIFT_OPERATOR, flags);
                break;
            case BWAND:
                invokeDefCall("and", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            case XOR:
                invokeDefCall("xor", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            case BWOR:
                invokeDefCall("or", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            default:
                throw location.createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    /** Writes a static binary instruction */
    public void writeBinaryInstruction(Location location, Class<?> clazz, Operation operation) {
        if (    (clazz == float.class || clazz == double.class) &&
                (operation == Operation.LSH || operation == Operation.USH ||
                operation == Operation.RSH || operation == Operation.BWAND ||
                operation == Operation.XOR || operation == Operation.BWOR)) {
            throw location.createError(new IllegalStateException("Illegal tree structure."));
        }

        switch (operation) {
            case MUL:
                math(GeneratorAdapter.MUL, getType(clazz));
                break;
            case DIV:
                math(GeneratorAdapter.DIV, getType(clazz));
                break;
            case REM:
                math(GeneratorAdapter.REM, getType(clazz));
                break;
            case ADD:
                math(GeneratorAdapter.ADD, getType(clazz));
                break;
            case SUB:
                math(GeneratorAdapter.SUB, getType(clazz));
                break;
            case LSH:
                math(GeneratorAdapter.SHL, getType(clazz));
                break;
            case USH:
                math(GeneratorAdapter.USHR, getType(clazz));
                break;
            case RSH:
                math(GeneratorAdapter.SHR, getType(clazz));
                break;
            case BWAND:
                math(GeneratorAdapter.AND, getType(clazz));
                break;
            case XOR:
                math(GeneratorAdapter.XOR, getType(clazz));
                break;
            case BWOR:
                math(GeneratorAdapter.OR, getType(clazz));
                break;
            default:
                throw location.createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    public void writeDup(final int size, final int xsize) {
        if (size == 1) {
            if (xsize == 2) {
                dupX2();
            } else if (xsize == 1) {
                dupX1();
            } else {
                dup();
            }
        } else if (size == 2) {
            if (xsize == 2) {
                dup2X2();
            } else if (xsize == 1) {
                dup2X1();
            } else {
                dup2();
            }
        }
    }

    public void writePop(final int size) {
        if (size == 1) {
            pop();
        } else if (size == 2) {
            pop2();
        }
    }

    @Override
    public void endMethod() {
        if (stringConcatArgs != null && !stringConcatArgs.isEmpty()) {
            throw new IllegalStateException("String concat bytecode not completed.");
        }
        super.endMethod();
    }

    @Override
    public void visitEnd() {
        throw new AssertionError("Should never call this method on MethodWriter, use endMethod() instead");
    }

    /**
     * Writes a dynamic call for a def method.
     * @param name method name
     * @param methodType callsite signature
     * @param flavor type of call
     * @param params flavor-specific parameters
     */
    public void invokeDefCall(String name, Type methodType, int flavor, Object... params) {
        Object[] args = new Object[params.length + 2];
        args[0] = settings.getInitialCallSiteDepth();
        args[1] = flavor;
        System.arraycopy(params, 0, args, 2, params.length);
        invokeDynamic(name, methodType.getDescriptor(), DEF_BOOTSTRAP_HANDLE, args);
    }

    public void invokeMethodCall(PainlessMethod painlessMethod) {
        Type type = Type.getType(painlessMethod.javaMethod.getDeclaringClass());
        Method method = Method.getMethod(painlessMethod.javaMethod);

        if (Modifier.isStatic(painlessMethod.javaMethod.getModifiers())) {
            // invokeStatic assumes that the owner class is not an interface, so this is a
            // special case for interfaces where the interface method boolean needs to be set to
            // true to reference the appropriate class constant when calling a static interface
            // method since java 8 did not check, but java 9 and 10 do
            if (painlessMethod.javaMethod.getDeclaringClass().isInterface()) {
                visitMethodInsn(Opcodes.INVOKESTATIC, type.getInternalName(),
                        painlessMethod.javaMethod.getName(), painlessMethod.methodType.toMethodDescriptorString(), true);
            } else {
                invokeStatic(type, method);
            }
        } else if (painlessMethod.javaMethod.getDeclaringClass().isInterface()) {
            invokeInterface(type, method);
        } else {
            invokeVirtual(type, method);
        }
    }

    public void invokeLambdaCall(FunctionRef functionRef) {
        invokeDynamic(
                functionRef.interfaceMethodName,
                functionRef.factoryMethodType.toMethodDescriptorString(),
                LAMBDA_BOOTSTRAP_HANDLE,
                Type.getMethodType(functionRef.interfaceMethodType.toMethodDescriptorString()),
                functionRef.delegateClassName,
                functionRef.delegateInvokeType,
                functionRef.delegateMethodName,
                Type.getMethodType(functionRef.delegateMethodType.toMethodDescriptorString()),
                functionRef.isDelegateInterface ? 1 : 0
        );
    }
}
