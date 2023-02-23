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
package de.uni_koblenz.west.splendid.index;

import java.util.HashSet;
import java.util.Set;


/**
 * Statistics about RDF graphs and their data.
 * 
 * @author Olaf Goerlitz
 */
public class GraphCatalog {
	
	public static enum SPO {
		SUBJECT,
		PREDICATE,
		OBJECT
	}
	
	// TODO dummy values
	Graph g = new Graph("defaultgraph");
	HashSet<Graph> list = new HashSet<Graph>();
	
	{
		list.add(g);
	}

//	/**
//	 * Returns the graphs which contain the specified value
//	 * as subject, predicate, or object.
//	 * 
//	 * @param element position in triple (subject, predicate, or object).
//	 * @param value the value
//	 * @return the set of graphs which contains the value.
//	 */
//	public Set<Graph> getGraphs(SPO element, String value) {
//		return list;
//	}

}
