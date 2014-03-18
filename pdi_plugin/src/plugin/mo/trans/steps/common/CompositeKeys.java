/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Copyright (c) 2014 Martin Ouellet
 *
 */
package plugin.mo.trans.steps.common;

import java.util.Arrays;

/**
 * A simple object representing a number of key value(s).  
 * Designed to simplify equality comparison for composite keys 
 * 
 * @author mouellet
 * 
 */
public class CompositeKeys {
	private static String EMPTY_STR = "";
	
	private final Object[] values;
	// immutable hash pre-calculated in Constructor
	private int hashValue = 7;
	
	/**
	 * Constructor with all values interpreted as composite key 
	 * @param keyvalues 
	 * 		complete row is used 
	 */
	public CompositeKeys(Object[] keyvalues) {
		values = new Object[keyvalues.length];
		for (int i = 0; i < keyvalues.length; i++) {
			if (keyvalues[i] == null || keyvalues[i].equals(EMPTY_STR)){
				throw new IllegalStateException("CompositeKey(s) cannot have null or empty value");
			}
			values[i] = keyvalues[i];
			hashValue += keyvalues[i].hashCode();
		}
	}

	/**
	 * Used to only consider key value(s) at specified position
	 * @param fullrow
	 * @param keyvaluesIdx 
	 * 		position of key values to use
	 */
	public CompositeKeys(Object[] fullrow, int[] keyvaluesIdx) {
		values = new Object[keyvaluesIdx.length];
		for (int i = 0; i < keyvaluesIdx.length; i++) {
			if (fullrow[keyvaluesIdx[i]] == null || fullrow[keyvaluesIdx[i]].equals(EMPTY_STR)){
				throw new IllegalStateException("CompositeKey(s) cannot have null or empty key value");
			}
			values[i] = fullrow[keyvaluesIdx[i]];
			hashValue += values[i].hashCode();
		}
	}

	
	/**
	 * Used to only consider the first n values starting at @from index.  
	 * Convenient with ResultSet often returning Array with empty 
	 * columns appended
	 * @param fullrow
	 * @param from 
	 * 		starting index (0-based)
	 * @param n 
	 * 		number of values to read
	 * 
	 */
	public CompositeKeys(Object[] fullrow, int from, int n) {
		values = new Object[n];
		for (int i = from; i < from+n; i++) {
			if (fullrow[i] == null || fullrow[i].equals(EMPTY_STR)){
				throw new IllegalStateException("CompositeKey(s) cannot have null or empty key value");
			}
			values[i-from] = fullrow[i];
			hashValue += values[i-from].hashCode();
		}
	}

		
	public int getNumberOfKey() {
		return values.length;	
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;

		if ((obj == null) || (getClass() != obj.getClass()))
			return false;
		
		CompositeKeys other = (CompositeKeys) obj;
		return (Arrays.equals(values, other.values));	
	}

	@Override
	public int hashCode() {
		return hashValue;
	}

	public Object[] getValues() {
		return values;
	}


	@Override
	public String toString() {
		return "CompositeKeys [keys=" + Arrays.toString(values) + "]";
	}

	
	
}
