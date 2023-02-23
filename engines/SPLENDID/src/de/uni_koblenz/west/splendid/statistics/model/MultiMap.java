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
 * Utility class providing a map with two keys, i.e. a 2-dimensional map.
 * 
 * @author Olaf Goerlitz
 *
 * @param <K> the type of the first key.
 * @param <S> the type of the second key.
 * @param <V> the type of the value.
 */
public class MultiMap<K,S,V> {
	
	private Map<K, Map<S,V>> map = new HashMap<K, Map<S,V>>();
	
	private Map<S,V> get(K majorKey) {
		Map<S, V> subMap = map.get(majorKey);
		if (subMap == null) {
			subMap = new HashMap<S, V>();
			map.put(majorKey, subMap);
		}
		return subMap;
	}
	
	public V get(K majorKey, S minorKey) {
		return get(majorKey).get(minorKey);
	}
	
	public void put(K majorKey, S minorKey, V value) {
		get(majorKey).put(minorKey, value);
	}
	
	public Set<K> keySet() {
		return this.map.keySet();
	}
	
	public Set<S> keySet(K majorKey) {
		return get(majorKey).keySet();
	}
	
}
