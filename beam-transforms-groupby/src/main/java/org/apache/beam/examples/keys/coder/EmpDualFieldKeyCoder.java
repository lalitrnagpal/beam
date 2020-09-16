package org.apache.beam.examples.keys.coder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.beam.examples.keys.EmpDualFieldKey;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;

public class EmpDualFieldKeyCoder extends Coder<EmpDualFieldKey> {

	@Override
	public void encode(EmpDualFieldKey value, OutputStream outStream) throws CoderException, IOException {
	    if (value == null) {
	        throw new CoderException("cannot encode a null Double");
	      }
	      new DataOutputStream(outStream).writeUTF(((EmpDualFieldKey)value).getKey1()+":"+((EmpDualFieldKey)value).getKey2());		
	}

	@Override
	public EmpDualFieldKey decode(InputStream inStream) throws CoderException, IOException {
	    try {
	    	String value = new DataInputStream(inStream).readUTF();
	        StringTokenizer tokenizer = new StringTokenizer(value, ":");
	        return new EmpDualFieldKey(tokenizer.nextToken(), tokenizer.nextToken());
	      } catch (EOFException | UTFDataFormatException exn) {
	        // These exceptions correspond to decoding problems, so change
	        // what kind of exception they're branded as.
	        throw new CoderException(exn);
	      }	
	}

	@Override
	public List<? extends Coder<?>> getCoderArguments() {
		 return Collections.emptyList();
	}

	@Override
	public void verifyDeterministic() throws NonDeterministicException {
		
		ByteArrayCoder.of().verifyDeterministic();
		
	}
	
}
