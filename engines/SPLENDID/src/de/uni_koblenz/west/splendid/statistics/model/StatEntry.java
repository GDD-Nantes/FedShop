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

/**
 * Basic statistics entry for counting element and obtaining the min/max values
 * for he element's range.
 *  
 * @author Olaf Goerlitz
 *
 * @param <T> the type of the element values.
 */
public class StatEntry<T> {
	
	protected long count;
	protected T min;
	protected T max;
	
	/**
	 * Creates a statistics entry with the supplied initial element.
	 * 
	 * @param element the initial element.
	 */
	public StatEntry(T element) {
		if (element == null)
			throw new IllegalArgumentException("element must not be null.");
		this.min = element;
		this.max = element;
		this.count = 1;
	}
	
	/**
	 * Adds an element to the statistics entry.
	 * Needs to be implemented by a subclass for comparable elements to
	 * obtain min/max values.
	 * 
	 * @param element the element to add.
	 */
	public void add(T element) {
		throw new IllegalArgumentException("comparable element required");
	}
	
	/**
	 * Add a comparable element to the statistics entry.
	 * 
	 * @param element the element to add.
	 * @param comparator the comparator to compare elements of this type.
	 */
	public void add(T element, Comparator<? super T> comparator) {
		if (element == null)
			throw new IllegalArgumentException("element must not be null.");
		if (comparator.compare(element, min) < 0)
			min = element;
		else if (comparator.compare(element, max) > 0)
			max = element;
		count++;
	}
	
	/**
	 * Returns the entries count value.
	 * 
	 * @return the count.
	 */
	public long getCount() {
		return this.count;
	}
	
	@Override
	public String toString() {
		return "count: " + count + ", min=" + min + ", max=" + max;
	}
}
