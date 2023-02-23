/*
 * This file is part of RDF Federator.
 * Copyright 2010 Olaf Goerlitz
 * 
 * RDF Federator is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * RDF Federator is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with RDF Federator.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * RDF Federator uses libraries from the OpenRDF Sesame Project licensed 
 * under the Aduna BSD-style license. 
 */
package de.uni_koblenz.west.splendid.statistics.model;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A map containing statistical data about elements which have sub elements of
 * different types (classes).<br><br>
 * 
 * Example:<br><ul>
 *   <li>'id' -> (String) 'D6GHQP'</li>
 *   <li>'id' -> (Integer) '12345'</li>
 *   <li>'age' -> (Integer) 24</li>
 *   <li>'size' -> (Double) 3.65</li></ul>
 * 
 * @author Olaf Goerlitz
 *
 * @param <K> the map's key type.
 */
public class StatMap<K> {
	
	protected Map<K, ClassMap> map = new HashMap<K, ClassMap>();
	
	/**
	 * Returns the {@link ClassMap} entry for the supplied key.
	 * 
	 * @param key the key to look up.
	 * @return the {@link ClassMap} entry for the key.
	 */
	protected ClassMap getClassMap(K key) {
		ClassMap classMap = map.get(key);
		if (classMap == null) {
			classMap = new ClassMap();
			map.put(key, classMap);
		}
		return classMap;
	}
	
	/**
	 * Adds a comparable value to the map.
	 * 
	 * @param <V> the value's type/class.
	 * @param key the associated key.
	 * @param t the value's class instance.
	 * @param value the values to add.
	 */
	public <V extends Comparable<V>> void add(K key, Class<V> t, V value) {
		StatEntry<V> statItem = getClassMap(key).get(t);
		if (statItem == null) {
			getClassMap(key).put(t, new ComparableStatEntry<V>(value));
		} else {
			statItem.add(value);
		}
	}
	
	/**
	 * Adds a incomparable value to the map, supplying a suitable comparator.
	 * 
	 * @param <V> the value's type/class.
	 * @param key the associated key.
	 * @param t the value's class instance.
	 * @param value the values to add.
	 * @param comparator the comparator used for comparing values of this type.
	 */
	public <V> void add(K key, Class<V> t, V value, Comparator<? super V> comparator) {
		StatEntry<V> statItem = getClassMap(key).get(t);
		if (statItem == null) {
			getClassMap(key).put(t, new StatEntry<V>(value));
		} else {
			statItem.add(value, comparator);
		}
	}
	
	/**
	 * Returns the {@link StatEntry} for the supplied key and type/class.
	 * 
	 * @param <V> the value's type/class.
	 * @param key the associated key.
	 * @param t the value's class instance.
	 * @return
	 */
	public <V> StatEntry<V> get(K key, Class<V> t) {
		return getClassMap(key).get(t);
	}
	
	/**
	 * Returns the set of classes contained in this map.
	 * 
	 * @return the set of classes.
	 */
	public Set<Class<?>> getClasses() {
		Set<Class<?>> classes = new HashSet<Class<?>>();
		for (K key : map.keySet()) {
			classes.addAll(map.get(key).keySet());
		}
		return classes;
	}
	
	/**
	 * Returns the set of classed contained in this map for the supplied key.
	 * 
	 * @param key the key to look up.
	 * @return the set of classes.
	 */
	public Set<Class<?>> getClasses(K key) {
		return this.map.get(key).keySet();
	}
	
	/**
	 * Returns the element count for the supplied key.
	 * 
	 * @param key the key.
	 * @return the element count for the key.
	 */
	public long getCount(K key) {
		return this.map.get(key).size();
	}
	
	/**
	 * Returns all key:{@link StatEntry} mappings contained in this map for
	 * the supplied type/class.
	 * 
	 * @param <V> the value's type/class.
	 * @param t the value's class instance.
	 * @return the mappings.
	 */
	public <V> Map<K, StatEntry<V>> getStats(Class<V> t) {
		Map<K, StatEntry<V>> statsMap = new HashMap<K, StatEntry<V>>();
		for (K key : map.keySet()) {
			StatEntry<V> statItem = getClassMap(key).get(t);
			if (statItem != null)
				statsMap.put(key, statItem);
		}
		return statsMap;
	}
	
	/**
	 * Returns the set of keys contained in this map.
	 * 
	 * @return the set of keys.
	 */
	public Set<K> keySet() {
		return this.map.keySet();
	}
	
	// -------------------------------------------------------------------------

	/**
	 * A mapping of classes to StatItems of that class type.
	 * Ensures that only objects of the right class are put in the map.
	 */
	class ClassMap {
		
		private Map<Class<?>, StatEntry<?>> map = new HashMap<Class<?>, StatEntry<?>>();
		
		@SuppressWarnings("unchecked")
		public <V> StatEntry<V> get(Class<V> clazz) {
			return (StatEntry<V>) map.get(clazz);
		}
		
		public <V> void put(Class<V> clazz, StatEntry<V> statItem) {
			map.put(clazz, statItem);
		}
		
		public Set<Class<?>> keySet() {
			return this.map.keySet();
		}

		/**
		 * Returns the overall number of elements in this map.
		 * 
		 * @return the number of elements.
		 */
		public long size() {
			long count = 0;
			for (Class<?> key : keySet()) {
				count += map.get(key).getCount();
			}
			return count;
		}
	}
	
	/**
	 * A {@link StatEntry} for comparable elements which makes min/max
	 * computations simpler.
	 * 
	 * @param <V> the type of the element values.
	 */
	protected class ComparableStatEntry<V extends Comparable<V>> extends StatEntry<V> {
		
		/**
		 * Creates a statistics entry with the supplied initial element.
		 * 
		 * @param element the initial element.
		 */
		public ComparableStatEntry(V element) {
			super(element);
		}
		
		/**
		 * Add a comparable element to the statistics entry.
		 * 
		 * @param element the comparable element to add.
		 */
		@Override
		public void add(V element) {
			if (element == null)
				throw new IllegalArgumentException("element must not be null.");
			if (element.compareTo(min) < 0)
				min = element;
			else if (element.compareTo(max) > 0)
				max = element;
			count++;
		}
		
	}
}

