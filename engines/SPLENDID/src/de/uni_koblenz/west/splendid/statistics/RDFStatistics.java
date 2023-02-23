/*
 * This file is part of RDF Federator.
 * Copyright 2011 Olaf Goerlitz
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
package de.uni_koblenz.west.splendid.statistics;

import java.util.Set;

import de.uni_koblenz.west.splendid.index.Graph;

/**
 * Interface for all RDF statistics provider.
 * 
 * @author Olaf Goerlitz
 */
public interface RDFStatistics {
	
	/**
	 * Returns a set of data sources which can potentially return results for the supplied s, p, o values.
	 * 
	 * @param sValue subject value.
	 * @param pValue predicate value.
	 * @param oValue object value.
	 * @param handleType defines whether rdf:type definitions should be evaluated. TODO: should not be method parameter.
	 * @return the set of matched data sources.
	 */
	public Set<Graph> findSources(String sValue, String pValue, String oValue, boolean handleType);
	
	/**
	 * Returns the number of triples in a data source.
	 * 
	 * @param g the data source.
	 * @return the number of triples.
	 */
	public long getTripleCount(Graph g);
	
	/**
	 * Returns the number of triples with the specified predicate in a data source.
	 * 
	 * @param g the data source.
	 * @param predicate the predicate.
	 * @return the number of triples.
	 */
	public long getPredicateCount(Graph g, String predicate);
	
	/**
	 * Returns the number of triples with rdf:type definition of the specified type in a data source.
	 * 
	 * @param g the data source.
	 * @param type the desired type.
	 * @return the number of triples.
	 */
	public long getTypeCount(Graph g, String type);
	
	/**
	 * Returns the number of distinct predicates in a data source.
	 * 
	 * @param g the data source.
	 * @return the number of distinct predicates.
	 */
	public long getDistinctPredicates(Graph g);
	
	/**
	 * Returns the number of distinct subjects in a data source.
	 * 
	 * @param g the data source.
	 * @return the number of distinct subjects.
	 */
	public long getDistinctSubjects(Graph g);
	
	/**
	 * Returns the number of distinct subjects in a data source
	 * which occur in triples with the specified predicate.
	 * 
	 * @param g the data source.
	 * @param predicate the predicate which occurs with the subjects.
	 * @return the number of distinct subjects.
	 */
	public long getDistinctSubjects(Graph g, String predicate);
	
	/**
	 * Returns the number of distinct objects in a data source.
	 * 
	 * @param g the data source.
	 * @return the number of distinct objects.
	 */
	public long getDistinctObjects(Graph g);

	/**
	 * Returns the number of distinct subjects in a data source
	 * which occur in triples with the specified predicate.
	 * 
	 * @param g the data source.
	 * @param predicate the predicate which occurs with the subjects.
	 * @return the number of distinct subjects.
	 */
	public long getDistinctObjects(Graph g, String predicate);
	
}
