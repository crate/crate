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

package io.crate.expression.operator;

import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.network.InetAddresses;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public final class CIDROperator {

    public static final String CONTAINED_WITHIN = Operator.PREFIX + "<<";

    private static final int IPV4_ADDRESS_LEN = 4;
    private static final int IPV6_ADDRESS_LEN = 16;

    public static void register(OperatorModule module) {
        module.registerDynamicOperatorFunction(CONTAINED_WITHIN, new CIDRResolver());
    }

    public static boolean containedWithin(String ipStr, String cidrStr) {
        if (null == ipStr || null == cidrStr) {
            throw new IllegalArgumentException("operands cannot be null");
        }
        if (false == cidrStr.contains("/") || ipStr.contains("/")) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "operands are incorrect, expected [ip, cidr], got [%s, %s]", ipStr, cidrStr));
        }
        try {
            BigInteger ip = new BigInteger(1, InetAddress.getByName(ipStr).getAddress());
            Tuple<BigInteger, BigInteger> cidr = extractRange(cidrStr);
            return cidr.v1().compareTo(ip) <= 0 && ip.compareTo(cidr.v2()) <= 0;
        } catch (UnknownHostException uhe) {
            throw new IllegalArgumentException(uhe);
        }
    }

    private static Tuple<BigInteger, BigInteger> extractRange(String cidr) {
        if (null == cidr || false == cidr.contains("/")) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH, "operand [%s] must conform with CIDR notation", cidr));
        }
        Tuple<InetAddress, Integer> tup = InetAddresses.parseCidr(cidr);
        InetAddress inetAddress = tup.v1();
        BigInteger base = new BigInteger(1, inetAddress.getAddress());
        BigInteger mask = createMask(inetAddress.getAddress().length, tup.v2());
        BigInteger start = base.and(mask);
        BigInteger end = start.add(mask.not());
        return new Tuple<>(start, end);
    }

    private static BigInteger createMask(int addressSizeInBytes, int prefixLength) {
        ByteBuffer maskBuffer = ByteBuffer.allocate(addressSizeInBytes);
        if (IPV4_ADDRESS_LEN == addressSizeInBytes) {
            maskBuffer = maskBuffer.putInt(-1);
        } else {
            maskBuffer = maskBuffer.putLong(-1L).putLong(-1L);
        }
        return new BigInteger(1, maskBuffer.array()).not().shiftRight(prefixLength);
    }

    private static class CIDRResolver extends BaseFunctionResolver {

        private static final FunctionInfo INFO = new FunctionInfo(
            new FunctionIdent(CONTAINED_WITHIN, Arrays.asList(DataTypes.IP, DataTypes.STRING)),
            DataTypes.BOOLEAN);

        private CIDRResolver() {
            super(FuncParams.builder(Param.of(DataTypes.IP), Param.of(DataTypes.STRING)).build());
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return new ContainedWithinOperator();
        }

        private static class ContainedWithinOperator extends Scalar<Boolean, Object> {

            @Override
            public Boolean evaluate(TransactionContext txnCtx, Input<Object>... args) {
                assert args.length == 2 : "number of args must be 2";
                String left = (String) args[0].value();
                if (null == left) {
                    return null;
                }
                String right = (String) args[1].value();
                if (null == right) {
                    return null;
                }
                return containedWithin(left, right);
            }

            @Override
            public FunctionInfo info() {
                return INFO;
            }
        }
    }
}
