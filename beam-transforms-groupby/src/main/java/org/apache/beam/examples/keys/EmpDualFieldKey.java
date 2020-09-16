package org.apache.beam.examples.keys;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class EmpDualFieldKey {

	private String key1;
	private String key2;
	
	@Override
	public String toString() {
		return key1+":"+key2;
	}
	
	@Override
	public boolean equals(Object obj) {
		EmpDualFieldKey eDualKey = (EmpDualFieldKey)obj; 
		return this.key1.equalsIgnoreCase(eDualKey.getKey1()) && this.getKey2().equalsIgnoreCase(eDualKey.getKey2());
	}
	
}
