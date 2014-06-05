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
 * A simple object representing a Satellite record composed of an 
 * arbitrary number of field values among which there are
 * <ul>
 * <li>1- techKey (Hub's PK) 
 * <li>2- fromDate time point (for temporal sat, for static = null)
 * </ul>
 * 
 * These two values are used for equality and natural ordering. 
 * <p>
 * The fromDate value is represented using <number of milliseconds> (since 1970...), 
 * aligned with getTime() methods of java.util.Date, java.sql.Date and 
 * java.sql.timestamp.  This is done to avoid comparison issues when mixing Timestamp 
 * and Date object class (sometimes happen when between values in input stream and values 
 * returned by the jdbc driver from DB).
 * <p>
 * All values that equal the empty string ("") are replaced by null 
 * to align with PDI logic.
 * 
 *  
 * 
 * @author mouellet
 * 
 */
public class SatRecord implements Comparable<SatRecord>{
	private static String EMPTY_STR = "";
	
	private final Object[] values;
	// Sat record immutable so hash pre-calculated
	private int hashValue;

	private Long techkeyValue;
	//FromDateValue stores in milliseconds time (as getTime())
	private long fromDateValue = Long.MIN_VALUE;
	private int fromDateIdx = -1;
	//flag indicating if record is persisted or not in DB
	private boolean persisted = false;
	
	// Meta attributes do not participate in sat record lifecycle 
	// change of values will be ignored for existing sat record
	private Object[] metaAtts;

	/**
	 * Construct a SatRow where techKey and FromDate are of type 
	 * Long and any subtype of java.util.Date, respectively.  
	 * 
	 * This ensures values are correctly ordered in Sorted Collection. 
	 * Satellite attributes may be null but not its techkeyValue 
	 * 
	 * Important:  values that equals the empty string ("") 
     * are replaced by null to align with PDI logic.
	 *   
	 * @param row
	 * 		input row
	 * @param indexpos 
	 * 		position of values to use
	 * @param techkeyIdx
	 * 		surrogate key index
	 * @param rowFromDateIdx
	 * 		"FromDate" time point index in @row.  This index must also 
	 * 	               be present in indexpos[].  static = -1.
	 */
	public SatRecord(Object[] row, int[] indexpos, int techkeyIdx, int rowFromDateIdx){
		if (row[techkeyIdx] == null){
			throw new IllegalStateException("techKey cannot be null");
		} 
		if (!(row[techkeyIdx].getClass() == Long.class)){
			throw new IllegalStateException("techKey must be of type Long, and is " + row[techkeyIdx].getClass());
				
		} 
		techkeyValue = (Long) row[techkeyIdx];

		values = new Object[indexpos.length];
		for (int i = 0; i < indexpos.length; i++) {
			if (row[indexpos[i]] != null && row[indexpos[i]].equals(EMPTY_STR)){
				values[i] = null;
			} else {
				values[i] = row[indexpos[i]];	
			}
			if (rowFromDateIdx != -1 && rowFromDateIdx == indexpos[i]){
				this.fromDateIdx = i;
				if (values[fromDateIdx] instanceof java.util.Date){
					fromDateValue = ((java.util.Date) values[fromDateIdx]).getTime();
				} else {
					throw new IllegalStateException("fromDateValue must be of type java.util.Date (or some subtype), "
							+ "but is " + values[fromDateIdx].getClass());
				}
			}
		}
		//hashCode must be consistent with Equals 
		hashValue = techkeyValue.hashCode();			
	}

	/**
	 * Convenient constructor that only uses values given
	 * by from and n.
	 * 
	 * @param row
	 * 		input row
	 * @param from 
	 * 		starting index (0-based) to read
	 * @param n 
	 * 		number of values to read
	 * @param techkeyIdx
	 * 		surrogate key index
	 * @param rowFromDateIdx
	 * 		"FromDate" time point index (considered immutable when = -1)
	 */
	public SatRecord(Object[] row, int from, int n, int techkeyIdx, int rowFromDateIdx){
		if (row[techkeyIdx] == null){
			throw new IllegalStateException("techKey cannot be null");
		} 
		if (!(row[techkeyIdx].getClass() == Long.class)){
			throw new IllegalStateException("techKey must be of type Long, and is " + row[techkeyIdx].getClass());
		} 
		techkeyValue = (Long) row[techkeyIdx];

		values = new Object[n];
		for (int i = from; i < from+n; i++) {
			if (row[i] != null && row[i].equals(EMPTY_STR)){
				values[i-from] = null;
			} else {
				values[i-from] = row[i];
			}
			if (rowFromDateIdx != -1 && rowFromDateIdx == i){
				fromDateIdx = i - from;
				if (values[fromDateIdx] instanceof java.util.Date){
					fromDateValue = ((java.util.Date) values[fromDateIdx]).getTime();	
				} else {
					throw new IllegalStateException("fromDateValue must be of type java.util.Date (or some subtype), "
							+ "but is " + values[fromDateIdx].getClass());
				}
			}
		}
		//hashCode must be consistent with Equals 
		hashValue = techkeyValue.hashCode();			
	}

	/**
	 * Convenient constructor that uses all row values 
	 *   
	 * @param allrow
	 * 		input row
	 * @param techkeyIdx
	 * 		surrogate key index
	 * @param fromDateIdx
	 * 		"FromDate" time point index (considered static when = -1)
	 * 
	 */
	public SatRecord(Object[] allrow, int techkeyIdx, int fromDateIdx){
		this(allrow, 0, allrow.length, techkeyIdx, fromDateIdx);
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;

		if ((obj == null) || (getClass() != obj.getClass()))
			return false;

		SatRecord other = (SatRecord) obj;

		if (!techkeyValue.equals(other.techkeyValue)){
			return false;
		} else {
			return fromDateValue == other.fromDateValue;
		}
	}

	@Override
	public int hashCode() {
		return hashValue;
	}

	/**
	 * Check all values are equal irrespective of "fromDate". 
	 * Useful to determine identical sat records (Idempotent)
	 * 
	 * @param other
	 * @return
	 */
	public boolean equalsNoCheckOnDate(SatRecord other) {
		
		for (int i = 0; i < values.length; i++) {
			if (i != fromDateIdx ){
				//require the other sat value to be also null
				if (values[i] == null){
					if (other.values[i] == null){
						continue;
					} else {
						return false;
					}
				} 
			
				if (!values[i].equals(other.values[i])){
					//nasty issue with mixing Date/Timestamp used as normal field!
					if ((values[i] instanceof java.util.Date) && 
							(other.values[i] instanceof java.util.Date)){
						long v = ((java.util.Date) values[i]).getTime();
						long o = ((java.util.Date) other.values[i]).getTime();
						if (v != o)
							return false;
					} else {
						return false;	
					}
				} 
			}
		}
		return true;
	}

	
	/*
	 * Used to guarantee proper equality/sorting of satellite records.
	 * 
	 * Both techkeyValue/fromDateValue are considered so it is 
	 * consistent with Equality comparison  
	 */
	@Override
	public int compareTo(SatRecord o) {
		if (techkeyValue == null){
			throw new IllegalStateException("Cannot sort object with null techkey value");
		}

		//static sat 
		if (fromDateIdx == -1){
			return techkeyValue.compareTo(o.getTechkeyValue());
		}
		
		//normal sat  
		if (techkeyValue.compareTo(o.getTechkeyValue()) == 0){
			return this.fromDateValue < o.fromDateValue ? -1 : 
				(this.fromDateValue == o.fromDateValue ? 0 : 1);
		} else {
			return techkeyValue.compareTo(o.getTechkeyValue());
		}
	}

	
	public Long getTechkeyValue() {
		return techkeyValue;
	}

	public Object[] getValues() {
		return values;
	}

	public long getFromDateTime() {
		return fromDateValue;
	}

	public boolean isPersisted() {
		return persisted;
	}

	public void setAsPersisted() {
		this.persisted = true;
	}

	public Object[] getMetaAtts() {
		return metaAtts;
	}

	public void setMetaAtts(Object[] atts, int[] idx) {
		metaAtts = new Object[idx.length];
		for (int i = 0; i < idx.length; i++ ){
			metaAtts[i] = atts[idx[i]];
		}
	}

	@Override
	public String toString() {
		return "SatRecord=" + Arrays.toString(values) + ", pkeyValue=" + techkeyValue
				+ ", fromDateValue=" + fromDateValue + "]";
	}
	
}
