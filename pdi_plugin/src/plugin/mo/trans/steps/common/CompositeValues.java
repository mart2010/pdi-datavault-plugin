package plugin.mo.trans.steps.common;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;

import javax.naming.ldap.HasControls;

/**
 * A simple object composed of an arbitrary number of field value(s).  
 * Designed to simplify equality comparison when used with 
 * a set of natural keys (key values), or used to simplify sorting/comparing satellite rows.
 * 
 * By default, all values are used for Equality comparison (key values).
 * 
 * Optionally when used as satellite rows, only two values are used for equality 
 * and natural ordering :
 * 	1- surrKey 
 *  2- fromDate time point
 * 
 */
public class CompositeValues implements Comparable<CompositeValues>{

	private final Object[] values;
	// immutable hash pre-calculated in Constructor
	private int hashValue = 7;
	// corresponding to row PKey (null, when not used as satellite)
	private Comparable<Object> pkeyValue;

	//corresponding to "fromDate" time point
	// null, when not used as satellite rows or satellite is immutable
	private Comparable<Object> fromDateValue;
	
	//flag indicating if object is or not persisted in DB
	private boolean persisted = false;
	
	/**
	 * Constructor that interpret all values as one natural key (composite)
	 * @param keyvalues 
	 * 		complete row is used 
	 */
	public CompositeValues(Object[] keyvalues) {
		values = new Object[keyvalues.length];
		for (int i = 0; i < keyvalues.length; i++) {
			values[i] = keyvalues[i];
			hashValue += keyvalues[i].hashCode();
		}
	}

	/**
	 * Used to only consider value(s) at specified position
	 * @param fullrow
	 * @param keyvaluesIdx 
	 * 		position of key values to use
	 */
	public CompositeValues(Object[] fullrow, int[] keyvaluesIdx) {
		values = new Object[keyvaluesIdx.length];
		for (int i = 0; i < keyvaluesIdx.length; i++) {
			values[i] = fullrow[keyvaluesIdx[i]];
			hashValue += values[i].hashCode();
		}
	}

	
	/**
	 * Used to only consider the first n values in row  
	 * starting at some index.  Convenient when ResultSet 
	 * returns Array with empty columns appended at end
	 * @param fullrow
	 * @param from 
	 * 		starting index (0-based)
	 * @param n 
	 * 		number of values to read
	 * 
	 */
	public CompositeValues(Object[] fullrow, int from, int n) {
		values = new Object[n];
		for (int i = from; i < from+n; i++) {
			values[i-from] = fullrow[i];
			hashValue += values[i-from].hashCode();
		}
	}


	/**
	 * Construct a SatRow where surKey and FromDate are of type Comparable.  
	 * This ensures values are correctly sorted inside Sorted Collection.  
	 *   
	 * @param fullrow
	 * 		input row
	 * @param indexpos 
	 * 		position of values to use
	 * @param surkeyIdx
	 * 		surrogate key index
	 * @param fromDateIdx
	 * 		"FromDate" time point index (considered immutable when = -1)
	 * 
	 */
	@SuppressWarnings("unchecked")
	public CompositeValues(Object[] row, int[] indexpos, int surkeyIdx, int fromDateIdx){
		this(row,indexpos);
	
		try {
			pkeyValue = (Comparable<Object>) row[surkeyIdx];
			//hashCode must be consistent with Equals
			hashValue = pkeyValue.hashCode();			
			if (fromDateIdx != -1){
				fromDateValue = (Comparable<Object>) row[fromDateIdx];
				hashValue = hashValue + fromDateValue.hashCode();
		 	} 
		} catch (ClassCastException ex){
			throw new IllegalArgumentException("SurrKey and Timepoint must be of type Comparable");
		}
	}

	/**
	 * Construct a SatRow where surKey and FromDate are of type Comparable.  
	 * This ensures values are correctly sorted inside Sorted Collection.  
	 *   
	 * @param fullrow
	 * 		input row
	 * @param from 
	 * 		starting index (0-based) to read
	 * @param n 
	 * 		number of values to read
	 * @param surkeyIdx
	 * 		surrogate key index
	 * @param fromDateIdx
	 * 		"FromDate" time point index (considered immutable when = -1)
	 * 
	 */
	@SuppressWarnings("unchecked")
	public CompositeValues(Object[] row, int from, int n, int surkeyIdx, int fromDateIdx){
		this(row,from,n);
	
		try {
			pkeyValue = (Comparable<Object>) row[surkeyIdx];
			//hashCode must be consistent with Equals
			hashValue = pkeyValue.hashCode();			
			if (fromDateIdx != -1){
				fromDateValue = (Comparable<Object>) row[fromDateIdx];
				hashValue = hashValue + fromDateValue.hashCode();
		 	} 
		} catch (ClassCastException ex){
			throw new IllegalArgumentException("SurrKey and Timepoint must be of type Comparable");
		}
		
	}

	
	
	public int getNumberOfKey() {
		if (pkeyValue == null){
			return values.length;	
		} else {
			//sat row has always 1 or 2 keys
			return (fromDateValue != null)? 2 : 1;
		}
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;

		if ((obj == null) || (getClass() != obj.getClass()))
			return false;

		CompositeValues other = (CompositeValues) obj;
		//normal 
		if (pkeyValue == null) {
			return (Arrays.equals(values, other.values));	
		} 
		//immutable sat row
		if (fromDateValue == null){
			return pkeyValue.equals(other.pkeyValue);
		}
    	//normal sat row 
		return   pkeyValue.equals(other.pkeyValue)
					&& fromDateValue.equals(other.fromDateValue) ; 
	}

	@Override
	public int hashCode() {
		return hashValue;
	}

	/**
	 * Compare all values irrespective of their "fromDate". 
	 * Useful to determine identical records 
	 * @param other
	 * @return
	 */
	public boolean equalsIgnoreFromDate(CompositeValues other) {
		
		for (int i = 0; i < values.length; i++) {
			if (values[i] != fromDateValue){
				if (!values[i].equals(other.values[i])){
					return false;
				}
			}
		}
		return true;
	}

	
	/*
	 * Used to guarantee proper equality/sorting of satellite rows values.
	 * 
	 * Both surrkeyValue/fromDateValue must be equal to be 
	 * considered identical
	 */
	@Override
	public int compareTo(CompositeValues o) {
		if (pkeyValue == null){
			throw new IllegalStateException("Cannot sort object without valid surrkey value");
		}

		//immutable sat row
		if (fromDateValue == null){
			return pkeyValue.compareTo(o.getPkeyValue());
		}
		
		//normal sat row 
		if (pkeyValue.compareTo(o.getPkeyValue()) == 0){
			return fromDateValue.compareTo(o.getFromDateValue());
		} else {
			return pkeyValue.compareTo(o.getPkeyValue());
		}
	}

	
	public Comparable<Object> getPkeyValue() {
		return pkeyValue;
	}

	public Object[] getValues() {
		return values;
	}

	public Comparable<Object> getFromDateValue() {
		return fromDateValue;
	}

	public boolean isPersisted() {
		return persisted;
	}

	public void setAsPersisted() {
		this.persisted = true;
	}

	@Override
	public String toString() {
		return "CompositeValues [values=" + Arrays.toString(values) + ", surrkeyValue=" + pkeyValue
				+ ", fromDateValue=" + fromDateValue + "]";
	}

	
	
}
