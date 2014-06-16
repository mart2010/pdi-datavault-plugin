/*
 * 
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
package plugin.dvloader.trans.steps.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;
import java.util.Date;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

public class SatRecordTest {

	@Test
	public void testSatConstructor() {

		Object[] o1 = new Object[] {"vvv", new Long(23),"att1",new Integer(0),new Timestamp(100000), "rrrr"};
		
		SatRecord s1 = new SatRecord(o1, 1, 4);
		SatRecord s2 = new SatRecord(o1,new int[] {0,1,2,3,4,5}, 1, 4);
		SatRecord s3 = new SatRecord(o1,0,6,1,4);
		
		assertTrue(s1.equals(s2));
		assertTrue(s1.hashCode() == s2.hashCode());
		assertTrue(s1.equals(s3));
		assertTrue(s1.hashCode() == s3.hashCode());
		assertTrue(s1.compareTo(s2)==00);
		assertTrue(s1.compareTo(s3)==00);
		
		assertTrue(s1.equalsNoCheckOnDate(s2));
		assertTrue(s1.equalsNoCheckOnDate(s3));
		
		Object[] o2 = new Object[] {"zzz", new Long(23),"att2",new Integer(2),new Timestamp(100000), "xxxx"};
		
		SatRecord t1 = new SatRecord(o2, 1, 4);
		SatRecord t2 = new SatRecord(o2,new int[] {0,1,2,3,4,5}, 1, 4);
		SatRecord t3 = new SatRecord(o2,0,5,1, 4);
		
		assertTrue(s1.equals(t1));
		assertTrue(s1.equals(t2));
		assertTrue(s1.equals(t3));
		assertTrue(s1.compareTo(t1)==00);
		assertTrue(s1.compareTo(t2)==00);
		assertTrue(s1.compareTo(t3)==00);
		
		assertFalse(s1.equalsNoCheckOnDate(t1));
		assertFalse(s1.equalsNoCheckOnDate(t2));
		assertFalse(s1.equalsNoCheckOnDate(t3));
		
		Object[] sameIdo2 = new Object[] {"zzz", "att2", new Long(23), new Timestamp(100000), new Integer(2), "xxxx"};
		
		SatRecord ts1 = new SatRecord(sameIdo2, 2, 3);
		SatRecord ts2 = new SatRecord(sameIdo2,new int[] {0,1,2,3,4,5}, 2, 3);
		SatRecord ts3 = new SatRecord(sameIdo2,0,5,2,3);
		
		assertTrue(t1.equals(ts1));
		assertTrue(t1.equals(ts2));
		assertTrue(t1.equals(ts3));
		assertTrue(t1.compareTo(ts1)==00);
		assertTrue(t1.compareTo(ts2)==00);
		assertTrue(t1.compareTo(ts3)==00);
		
		assertFalse(t1.equalsNoCheckOnDate(ts1));
		assertFalse(t1.equalsNoCheckOnDate(ts2));
		assertFalse(t1.equalsNoCheckOnDate(ts3));
		
		
		
	}
	
	
	
	@Test
	public void testSatCompValues() {

		//Test SAT ROW with diff att values

		SatRecord cv1 = new SatRecord(new Object[] {new Long(23),"att1",new Integer(0),new Timestamp(100000)},0,4,0,3);
		SatRecord cv2 = new SatRecord(new Object[] {new Long(23),"att2",new Integer(10),new Timestamp(100000)},0,4,0,3);	
		assertTrue(cv1.equals(cv2));
		assertTrue(cv1.hashCode() == cv2.hashCode());
		assertTrue(cv1.compareTo(cv2) == 0);
		assertFalse(cv1.equalsNoCheckOnDate(cv2));
		
		SatRecord cv3 = new SatRecord(new Object[] {new Long(23),"att1",new Integer(0),new Timestamp(100001)},0,4,0,3);
		assertFalse(cv1.equals(cv3));
		assertTrue(cv1.compareTo(cv3)<0);
		assertTrue(cv3.compareTo(cv1)>0);
		assertTrue(cv1.equalsNoCheckOnDate(cv3));

		SatRecord cv4 = new SatRecord(new Object[] {new Long(24),"att1",new Integer(0),new Timestamp(100000)},0,4,0,3);
		assertFalse(cv1.equals(cv4));
		assertTrue(cv1.compareTo(cv4)<0);
		assertTrue(cv4.compareTo(cv1)>0);
		assertFalse(cv1.equalsNoCheckOnDate(cv4));
	
	
		//Test SAT ROW with null and empty string
		SatRecord cnull1 = new SatRecord(new Object[] {new Long(23),"v","",null,new Timestamp(100000)},0,4);
		SatRecord cnull2 = new SatRecord(new Object[] {new Long(23),"v",null,null,new Timestamp(100000)},0,4);	
		assertTrue(cnull1.equals(cnull2));
		assertTrue(cnull1.compareTo(cnull2)==00);
		assertEquals(cnull1.hashCode(), cnull2.hashCode());
		assertTrue(cnull1.equalsNoCheckOnDate(cnull2));
		
		
		//check values with Integer / Long etc..
		SatRecord cint = new SatRecord(new Object[] {new Long(1),new Date(88888888),new Integer(100)},0,1);
		SatRecord clong = new SatRecord(new Object[] {new Long(1),new Date(88888888),new Long(100)},0,1);	
		
		assertTrue(cint.equals(clong));
		assertTrue(cint.compareTo(clong) == 0);
		//Important: Long & Integer even if same values are not equal as not the same type
		assertFalse(cint.equalsNoCheckOnDate(clong));
	
		try {
			SatRecord cIdint = new SatRecord(new Object[] {new Integer(1),new Date(88888888),new Integer(100)},0,1);
			assertTrue(false);
		} catch (IllegalStateException ex){
			assertTrue(true);
		}   
		
	}
	
	@Test
	public void testSortedSetValues() {
		NavigableSet<SatRecord> s = new TreeSet<SatRecord>();		
		
		SatRecord c1 = new SatRecord(new Object[] {new Long(10), "a1","a2", new Timestamp(88888888)}
									,new int[] {0, 1, 3}, 0, 3);
		assertTrue(s.add(c1));
		
		SatRecord samec1 = new SatRecord(new Object[] {new Long(10), "eee","eee", new Timestamp(88888888)}
									,new int[] {0, 1, 3}, 0, 3);
		assertFalse(s.add(samec1));
		
		SatRecord afterc1 = new SatRecord(new Object[] {new Long(10), "ccc","ccc", new Timestamp(88888889)}
									,new int[] {0, 1, 3}, 0, 3);
		assertTrue(s.add(afterc1));
		
		SatRecord c2 = new SatRecord(new Object[] {new Long(11), "a1","a2", new Timestamp(88888888)}
									,new int[] {0, 1, 3}, 0, 3);
		assertTrue(s.add(c2));
		
		assertTrue(s.contains(c1));
		assertTrue(s.contains(samec1));
		assertTrue(s.contains(afterc1));
		assertTrue(s.contains(c2));

		SatRecord c21 = new SatRecord(new Object[] {new Long(11), "ccc","ccc", new Timestamp(88888890)}
									,new int[] {0, 1, 3}, 0, 3);

		SatRecord c22 = new SatRecord(new Object[] {new Long(11), "ccc","ccc", new Timestamp(88888891)}
									,new int[] {0, 1, 3}, 0, 3);

		SatRecord c23 = new SatRecord(new Object[] {new Long(11), "ccc","ccc", new Timestamp(88888892)}
									,new int[] {0, 1, 3}, 0, 3);

		assertTrue(s.add(c21));
		assertTrue(s.add(c22));
		assertTrue(s.add(c23));
		
		assertTrue(s.lower(c23) == c22);
		assertTrue(s.lower(c22) == c21);
		assertTrue(s.lower(c21) == c2);
		assertTrue(s.lower(c2) == afterc1);
		assertTrue(s.lower(afterc1) == c1);
		
		assertTrue(s.lower(c1) == null);
		assertTrue(s.higher(c23) == null);
		
		assertTrue(s.higher(c21) == c22);
		assertTrue(s.higher(afterc1) == c2);
		
	}

	
	@Test
	public void testSortedSetImmutValues() {

		SortedSet<SatRecord> s = new TreeSet<SatRecord>();		
		
		SatRecord c1 = new SatRecord(new Object[] {new Long(10), "a1","a2", new Timestamp(88888888)}
									,new int[] {0, 1}, 0, -1);
		assertTrue(s.add(c1));
		
		SatRecord samec1 = new SatRecord(new Object[] {new Long(10), "eee","eee", new Timestamp(88888888)}
									,new int[] {0, 1}, 0, -1);
		assertFalse(s.add(samec1));

		SatRecord diffc1 = new SatRecord(new Object[] {new Long(11), "eee","eee", new Timestamp(88888888)}
									,new int[] {0, 1}, 0, -1);
		assertTrue(s.add(diffc1));
		
		assertTrue(s.contains(c1));
		assertTrue(s.contains(samec1));
		assertTrue(s.contains(diffc1));
		
		
	}

	@Test
	public void testSortedSetMixingDateType() {
		//the issues arise when mixing different Date type, for ex:
		Date d = new Date(100);
		Timestamp t = new Timestamp(100);
		
		assertFalse(t.equals(d));
		//THE REVERSE IS NOT TRUE!! because Timestamp, a subclass of Date, has a nanoseconds that is used in comparison with Date
		//so not reflexive  
		assertTrue(d.equals(t));

		//but to make things more complicated CONTRADICTS equality!!!!
		assertTrue(t.compareTo(d) == 0);
		assertFalse(d.compareTo(t) == 0);

		java.sql.Date sqld = new java.sql.Date(100);
		assertFalse(t.equals(sqld));
		assertTrue(sqld.equals(t));
		assertTrue(t.compareTo(sqld) == 0);
		assertFalse(sqld.compareTo(t) == 0);

		//But both Date behaves similarly as java.sql.Date 
		assertTrue(d.equals(sqld));
		assertTrue(sqld.equals(sqld));
		assertTrue(d.compareTo(sqld) == 0);
		assertTrue(sqld.compareTo(d) == 0);
		
		
		//Because of these quirks, decided to convert all Date-related to millisecond getTime() equivalent in SatRecord!!!
		
		
		SatRecord c1Timestamp = new SatRecord(new Object[] {new Long(10), "a1", new Timestamp(88), new Timestamp(88)}
									, 0, 3);		
		SatRecord samec1ButWithDate 
						   = new SatRecord(new Object[] {new Long(10), "a1",new Date(88), new Date(88)}
									, 0, 3);
		
		//mixing Date and Timestamp  
		assertTrue(c1Timestamp.equals(samec1ButWithDate));
		assertTrue(samec1ButWithDate.equals(c1Timestamp));
		assertTrue(c1Timestamp.compareTo(samec1ButWithDate) == 0);
		assertTrue(samec1ButWithDate.compareTo(c1Timestamp) == 0);
		
		SatRecord samec1ButWithSQLDate 
							= new SatRecord(new Object[] {new Long(10), "a1",new java.sql.Date(88), new java.sql.Date(88)}
									, 0, 3);
		assertTrue(c1Timestamp.equals(samec1ButWithSQLDate));
		assertTrue(samec1ButWithSQLDate.equals(c1Timestamp));
		assertTrue(c1Timestamp.compareTo(samec1ButWithSQLDate) == 0);
		assertTrue(samec1ButWithSQLDate.compareTo(c1Timestamp) == 0);
		assertTrue(samec1ButWithDate.equals(samec1ButWithSQLDate));
		assertTrue(samec1ButWithSQLDate.equals(samec1ButWithDate));

		//check inserting behave also coherently...
		NavigableSet<SatRecord> s = new TreeSet<SatRecord>();		
		assertTrue(s.add(c1Timestamp));
		assertFalse(s.add(samec1ButWithDate));
		assertFalse(s.add(samec1ButWithSQLDate));
		
		SatRecord diffc1	= new SatRecord(new Object[] {new Long(10), "a1",new Date(88), new Date(89)}
		   							, 0, 3);
		assertTrue(s.add(diffc1));
		
		
		//Now checking Idempotent (with mix of Date in values) 
		assertTrue(c1Timestamp.equalsNoCheckOnDate(samec1ButWithDate));
		assertTrue(samec1ButWithDate.equalsNoCheckOnDate(c1Timestamp));
		assertTrue(samec1ButWithSQLDate.equalsNoCheckOnDate(samec1ButWithDate));
		assertTrue(samec1ButWithDate.equalsNoCheckOnDate(samec1ButWithSQLDate));
		
		assertTrue(c1Timestamp.equalsNoCheckOnDate(diffc1));
		assertTrue(diffc1.equalsNoCheckOnDate(c1Timestamp));
		
		
	}
	
	
}
