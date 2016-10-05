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

//import java.io.ByteArrayInputStream;
//import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
//import org.apache.commons.codec.digest.DigestUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.google.common.collect.ImmutableList;

import io.crate.action.sql.SQLOperations;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class Sha1Function extends Scalar<BytesRef, BytesRef>{
	
	public final static String NAME = "sha1";
	private final static ESLogger LOGGER = Loggers.getLogger(SQLOperations.class);
	private final FunctionInfo info;

	public static void register(ScalarFunctionModule module) {
        module.register(new Sha1Function(new FunctionInfo(
                new FunctionIdent(NAME, ImmutableList.<DataType>of(DataTypes.STRING)),
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
        
        //MessageDigest has to be initialised each time since it is not thread save
        MessageDigest messageDigest = null;	
        try {
            messageDigest = MessageDigest.getInstance("SHA");
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("Message", e);
        }
        
        messageDigest.update(stringValue.bytes, 0, stringValue.length);  
        char[] ref = Hex.encodeHex( messageDigest.digest() );
        
        byte[] res = new byte[UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * ref.length];
        int len = UnicodeUtil.UTF16toUTF8(ref, 0, ref.length, res);
        return new BytesRef(res, 0, len);
    }



	
//Alternative implementations that have been tested for performance:
//  Using DigestUtils library + array copy necessary since possibly not all bytes in the array 
//  are valid (BytesRef is filling the array with '0' at the end)
//     @Override
//     public BytesRef evaluate(Input<BytesRef>... args) {
//        BytesRef stringValue = args[0].value();
//        if (stringValue == null) {
//            return null;
//        }
//                
//        //sha function executed to arg0 using apache commons
//        byte[] bytes = new byte[stringValue.length];
//        System.arraycopy(stringValue.bytes, 0, bytes, 0, stringValue.length );
//        
//        char[] ref = Hex.encodeHex( DigestUtils.sha1(bytes) );
//        
//        byte[] res = new byte[UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * ref.length];
//        int len = UnicodeUtil.UTF16toUTF8(ref, 0, ref.length, res);
//        return new BytesRef(res, 0, len);
//    }

//    Using DigestUtils library + no array creation and copy but the usage of ByteArrayInputStream 
//    @Override
//    public BytesRef evaluate(Input<BytesRef>... args) {
//        BytesRef stringValue = args[0].value();
//        if (stringValue == null) {
//            return null;
//        }
//                
//        //sha function executed to arg0 using apache commons  
//        ByteArrayInputStream inputSha1 = new ByteArrayInputStream( 
//        	      stringValue.bytes, 0, stringValue.length);
//        char[] ref = null;
//        try {
//            ref = Hex.encodeHex( DigestUtils.sha1( inputSha1 ) );
//        } catch (IOException e) {
//            LOGGER.error("InputStream could not be processed", e);
//        }
//        
//        byte[] res = new byte[UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * ref.length];
//        int len = UnicodeUtil.UTF16toUTF8(ref, 0, ref.length, res);
//        return new BytesRef(res, 0, len);
//    }
}
