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
	// corresponding to surrKey
	// null, when not used as satellite rows
	private Comparable<Object> surrkeyValue;

	//corresponding to "fromDate" time point
	// null, when not used as satellite rows or satellite is immutable
	private Comparable<Object> fromDateValue;
	
	//flag indicating if object is or not persisted in DB
	private boolean persisted = false;
	
	/**
	 * Constructor that read all key values as forming a single 
	 * composite key
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
	 * Convenient to only consider key value(s) at specified position
	 * @param fullrow
	 * @param indexpos 
	 * 		position of key values to use
	 */
	public CompositeValues(Object[] fullrow, int[] indexpos) {
		values = new Object[indexpos.length];
		for (int i = 0; i < indexpos.length; i++) {
			values[i] = fullrow[indexpos[i]];
			hashValue += values[i].hashCode();
		}
	}

	
	/**
	 * Convenient constructor to only consider the first n values in row  
	 * starting at some index.  Useful when ResultSet returns Object[] with 
	 * empty columns appended at end
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
			surrkeyValue = (Comparable<Object>) row[surkeyIdx];
			//hashCode must be consistent with Equals
			hashValue = surrkeyValue.hashCode();			
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
			surrkeyValue = (Comparable<Object>) row[surkeyIdx];
			//hashCode must be consistent with Equals
			hashValue = surrkeyValue.hashCode();			
			if (fromDateIdx != -1){
				fromDateValue = (Comparable<Object>) row[fromDateIdx];
				hashValue = hashValue + fromDateValue.hashCode();
		 	} 
		} catch (ClassCastException ex){
			throw new IllegalArgumentException("SurrKey and Timepoint must be of type Comparable");
		}
		
	}

	
	
	public int getNumberOfKey() {
		if (surrkeyValue == null){
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
		if (surrkeyValue == null) {
			return (Arrays.equals(values, other.values));	
		} 
		//immutable sat row
		if (fromDateValue == null){
			return surrkeyValue.equals(other.surrkeyValue);
		}
    	//normal sat row 
		return   surrkeyValue.equals(other.surrkeyValue)
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
		if (surrkeyValue == null){
			throw new IllegalStateException("Cannot sort object without valid surrkey value");
		}

		//immutable sat row
		if (fromDateValue == null){
			return surrkeyValue.compareTo(o.getSurrkeyValue());
		}
		
		//normal sat row 
		if (surrkeyValue.compareTo(o.getSurrkeyValue()) == 0){
			return fromDateValue.compareTo(o.getFromDateValue());
		} else {
			return surrkeyValue.compareTo(o.getSurrkeyValue());
		}
	}

	
	public Comparable<Object> getSurrkeyValue() {
		return surrkeyValue;
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
		return "CompositeValues [values=" + Arrays.toString(values) + ", surrkeyValue=" + surrkeyValue
				+ ", fromDateValue=" + fromDateValue + "]";
	}

	
	
}
