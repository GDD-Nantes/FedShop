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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A map which counts the frequency of occurrence of its elements.
 * 
 * @author Olaf Goerlitz
 * 
 * @param <K> the map's key type.
 */
public class CountMap<K> {
	
	protected Map<K, Long> map = new HashMap<K, Long>();
	
	/**
	 * Returns the number of key-value mappings in this map.
	 * If the map contains more than Integer.MAX_VALUE elements,
	 * returns Integer.MAX_VALUE.
	 * 
	 * @return the number of key-value mapping in this map.
	 */
	public int size() {
		return this.map.size();
	}
	
	/**
	 * Adds an element to the map.
	 * 
	 * @param element the element to add
	 */
	public void add(K element) {
		Long count = map.get(element);
		if (count == null)
			map.put(element, 1l);
		else
			map.put(element, count + 1);
	}
	
	/**
	 * Returns the set of elements in this map.
	 * 
	 * @return the set of elements.
	 */
	public Set<K> keySet() {
		return map.keySet();
	}
	
	/**
	 * Retuns the count for a specific element.
	 * 
	 * @param key the element of interest.
	 * @return the count of the element.
	 */
	public long getCount(K key) {
		return map.get(key);
	}

}
