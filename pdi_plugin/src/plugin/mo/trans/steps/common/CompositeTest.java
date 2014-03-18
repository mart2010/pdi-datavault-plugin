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
package plugin.mo.trans.steps.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;
import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

public class CompositeTest {

	@Test
	public void testIdenticalValues() {
		
		CompositeValues c1 = new CompositeValues(new Object[] {"v1","v2",new Timestamp(100000)});
		CompositeValues c2 = new CompositeValues(new Object[] {"v1","v2",new Timestamp(100000)});	
		assertTrue(c1.equals(c2));
		assertEquals(c1.hashCode(), c2.hashCode());
		
		CompositeValues c3 = new CompositeValues(new Object[] {"v1","v2",new Timestamp(100001)});
		assertFalse(c2.equals(c3));
		
		CompositeValues c4 = new CompositeValues(new Object[] {"v1","V",new Timestamp(100000)});
		assertFalse(c1.equals(c4));

		CompositeValues c5 = new CompositeValues(new Object[] {"v1","v2 ",new Timestamp(100000)});
		assertFalse(c1.equals(c4));
		
		
		CompositeValues c6 = new CompositeValues(new Object[] {"v1","v2",new Timestamp(100000)}, new int[] {0,2});		
		CompositeValues c7 = new CompositeValues(new Object[] {"v1",new Timestamp(100000)});		
		assertTrue(c6.equals(c7));
		assertEquals(c6.hashCode(),c7.hashCode());
		//assertTrue(!c5.equals(c3));
		
		
		
		CompositeValues c8 = new CompositeValues(new Object[] {"v1","v2",new Timestamp(100000)}, new int[] {0,1});		
		CompositeValues c9 = new CompositeValues(new Object[] {"v1","v2",new Timestamp(100000)}, 0, 2);		
		assertTrue(c8.equals(c9));
		assertEquals(c8.hashCode(),c9.hashCode());
		
		CompositeValues c10 = new CompositeValues(new Object[] {"v1",new Integer(10)});		
		CompositeValues c11 = new CompositeValues(new Object[] {"v1",new Long(10)});		
		assertFalse(c10.equals(c11));
		//HERE, they will have same Hashcode()
		assertEquals(c10.hashCode(),c11.hashCode());
		
		
		CompositeValues d1 = new CompositeValues(new Object[] {"v1","v2",new Timestamp(100000)},0,2);
		CompositeValues d2 = new CompositeValues(new Object[] {"v1","v2",new Timestamp(1)},0,2);	
		assertTrue(d1.equals(d2));

		CompositeValues e1 = new CompositeValues(new Object[] {"v1","v2",new Timestamp(100000)},1,2);
		CompositeValues e2 = new CompositeValues(new Object[] {"v1","v2",new Timestamp(1)},1,2);	
		assertFalse(e1.equals(e2));
		
	}

	@Test
	public void testDateValues() {
		//TODO:  check about these Dates......!!!!!
		//IMPORTANT: it seems even if these do not come from same base class, they are equivalent!!! 
		CompositeValues c1 = new CompositeValues(new Object[] {"v1",new Date(10)});
		CompositeValues c2 = new CompositeValues(new Object[] {"v1",new Timestamp(10)});	
		assertTrue(c1.equals(c2));
		
		CompositeValues c3 = new CompositeValues(new Object[] {"v1",new java.sql.Date(10)});	
		assertTrue(c1.equals(c3));
			
		
	}
	
	
	@Test
	public void testSatCompValues() {

		//Test SAT ROW with null
		CompositeValues cnull1 = new CompositeValues(new Object[] {"v1","att1",null,new Timestamp(100000)},0,4,0,3);
		CompositeValues cnull2 = new CompositeValues(new Object[] {"v1","att1",null,new Timestamp(100000)},0,4,0,3);	
		assertTrue(cnull1.equals(cnull2));
		assertEquals(cnull1.hashCode(), cnull2.hashCode());

		assertTrue(cnull1.equalsValuesExceptFromDate(cnull2));
		
		CompositeValues cnull3 = new CompositeValues(new Object[] {"v1","att1",null,new Timestamp(100001)},0,4,0,3);
		assertTrue(!cnull1.equals(cnull3));
		
		assertTrue(cnull1.equalsValuesExceptFromDate(cnull3));
		
		//check out the empty string is same as null
		CompositeValues cnull4 = new CompositeValues(new Object[] {"v1","att1","",new Timestamp(100001)},0,4,0,3);
		assertTrue(cnull3.equalsValuesExceptFromDate(cnull4));
		assertTrue(cnull4.equalsValuesExceptFromDate(cnull3));
		
		CompositeValues cnull5 = new CompositeValues(new Object[] {"v1","att2","",new Timestamp(100001)},0,4,0,3);
		assertTrue(cnull4.equals(cnull5));
		assertTrue(!cnull4.equalsValuesExceptFromDate(cnull5));
		
		
		/*
		CompositeValues c1 = new CompositeValues(new Object[] {"v1",new Timestamp(88888888)},1);
		CompositeValues c2 = new CompositeValues(new Object[] {"vdddd",new Timestamp(88888888)},1);	
		
		assertFalse(c1.equals(c2));	
		assertEquals(c1.compareTo(c2),0);

		CompositeValues c3 = new CompositeValues(new Object[] {"vdddd",new Timestamp(88888889)},1);	

		assertTrue(c1.compareTo(c3) < 0);
		assertTrue(c3.compareTo(c1) > 0);

		c1 = new CompositeValues(new Object[] {"v1",new Timestamp(88888888)},0);
		c2 = new CompositeValues(new Object[] {"v1",new Timestamp(88888889)},0);	
		
		assertEquals(c1.compareTo(c2),0);

		*/		
	}
	
	@Test
	public void testSortedSetValues() {

		SortedSet<CompositeValues> s = new TreeSet<CompositeValues>();		
		
		CompositeValues c1 = new CompositeValues(new Object[] {new Long(10), "a1","a2", new Timestamp(88888888)}
									,new int[] {0, 1, 3}, 0, 3);
		assertTrue(s.add(c1));
		
		CompositeValues samec1 = new CompositeValues(new Object[] {new Long(10), "eee","eee", new Timestamp(88888888)}
									,new int[] {0, 1, 3}, 0, 3);
		assertFalse(s.add(samec1));
		
		CompositeValues diffc1 = new CompositeValues(new Object[] {new Long(10), "eee","eee", new Timestamp(88888889)}
									,new int[] {0, 1, 3}, 0, 3);
		assertTrue(s.add(diffc1));
		
		CompositeValues c2 = new CompositeValues(new Object[] {new Long(11), "a1","a2", new Timestamp(88888888)}
									,new int[] {0, 1, 3}, 0, 3);
		assertTrue(s.add(c2));
		
		assertTrue(s.contains(c1));
		assertTrue(s.contains(samec1));
		assertTrue(s.contains(samec1));
		assertTrue(s.contains(c2));
		
		s.clear();
		c1 = new CompositeValues(new Object[] {new Long(10), "a1","a2", new Timestamp(88888888)}
									,0,4, 0, 3);
		assertTrue(s.add(c1));

		samec1 = new CompositeValues(new Object[] {new Long(10), "eee","eee", new Timestamp(88888888)}
									,0,4, 0, 3);
		assertFalse(s.add(samec1));

		diffc1 = new CompositeValues(new Object[] {new Long(10), "eee","eee", new Timestamp(88888889)}
									,0,4, 0, 3);
		assertTrue(s.add(diffc1));

		c2 = new CompositeValues(new Object[] {new Long(11), "a1","a2", new Timestamp(88888888)}
									,0,4, 0, 3);
		assertTrue(s.add(c2));
		
		
	}

	@Test
	public void testSortedSetImmutValues() {

		SortedSet<CompositeValues> s = new TreeSet<CompositeValues>();		
		
		CompositeValues c1 = new CompositeValues(new Object[] {new Long(10), "a1","a2", new Timestamp(88888888)}
									,new int[] {0, 1}, 0, -1);
		assertTrue(s.add(c1));
		
		CompositeValues samec1 = new CompositeValues(new Object[] {new Long(10), "eee","eee", new Timestamp(88888888)}
									,new int[] {0, 1}, 0, -1);
		assertFalse(s.add(samec1));

		CompositeValues diffc1 = new CompositeValues(new Object[] {new Long(11), "eee","eee", new Timestamp(88888888)}
									,new int[] {0, 1}, 0, -1);
		assertTrue(s.add(diffc1));
		
		assertTrue(s.contains(c1));
		assertTrue(s.contains(samec1));
		assertTrue(s.contains(diffc1));
		
		
	}

	@Test
	public void testSortedSetMixingDateType() {
		CompositeValues c1Timestamp = new CompositeValues(new Object[] {new Long(10), "a1","a2", new Timestamp(88)}
									,new int[] {0, 1, 3}, 0, 3);		
		CompositeValues samec1ButWithDate 
						   = new CompositeValues(new Object[] {new Long(10), "a1","a2", new Date(88)}
									,new int[] {0, 1, 3}, 0, 3);
		
		//mixing Date and Timestamp  
		//c1 has nanosecond so will never be equal to any Date
		assertFalse(c1Timestamp.equals(samec1ButWithDate));
		// but the reverse is ok as Date does not have nano and ignore this nano sec of Timestamp!
		assertTrue(samec1ButWithDate.equals(c1Timestamp));

		//But these two types represent the same day!! 
		//Reversing the comparison of compareTo behaves also the same  
		assertTrue(c1Timestamp.compareTo(samec1ButWithDate) == 0);
		assertFalse(samec1ButWithDate.compareTo(c1Timestamp) == 0);
		
		//this is explained by:
		Date d = new Date(100);
		Timestamp t = new Timestamp(100);
		assertTrue(t.compareTo(d) == 0);
		//BUT THE REVERSE IS NOT TRUE!! I guess because Timestamp is a subclass of Date (and not the ther way around) 
		assertFalse(d.compareTo(t)==0);
		
		//So inserting these into the Set will behave differently depending on the order...
		SortedSet<CompositeValues> s = new TreeSet<CompositeValues>();		
		s.add(samec1ButWithDate);
		//Explaining the eratic behavior of satHistMap when mixing Date & Timestamp
		assertFalse(s.add(c1Timestamp));
		assertTrue(s.remove(samec1ButWithDate));
		s.add(c1Timestamp);
		assertTrue(s.add(samec1ButWithDate));
		
	}
	
	
	
}
