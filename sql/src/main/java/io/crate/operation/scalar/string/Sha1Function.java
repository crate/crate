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

package io.crate.operation.scalar.string;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;

import com.google.common.collect.ImmutableList;

import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataTypes;

public class Sha1Function extends Scalar<BytesRef, BytesRef>{
	
	public final static String NAME = "sha1";
	private final FunctionInfo info;
	private MessageDigest messageDigest = null;

	public static void register(ScalarFunctionModule module) {
        module.register(new Sha1Function(new FunctionInfo(
                new FunctionIdent(NAME, ImmutableList.of(DataTypes.STRING)),
                DataTypes.STRING)
        ));
    }
	
	public Sha1Function(FunctionInfo info) {
        this.info = info;
    }
	
	@Override
	public FunctionInfo info() {
		return this.info;
	}

	@Override
	public BytesRef evaluate(Input<BytesRef>... args) {
		BytesRef stringValue = args[0].value();
        if (stringValue == null) {
            return null;
        }
                
        //sha function executed to arg0 using apache commons
        if( messageDigest == null ) {	
	        try {
	        	messageDigest = MessageDigest.getInstance("SHA");
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
        }
        
        messageDigest.update(stringValue.bytes, 0, stringValue.length);  
        char[] ref = Hex.encodeHex( messageDigest.digest() );
        
        byte[] res = new byte[UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * ref.length];
        int len = UnicodeUtil.UTF16toUTF8(ref, 0, ref.length, res);
        return new BytesRef(res, 0, len);
	}

}
