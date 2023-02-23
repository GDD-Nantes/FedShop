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
package de.uni_koblenz.west.splendid.estimation;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import de.uni_koblenz.west.splendid.index.Graph;
import de.uni_koblenz.west.splendid.model.MappedStatementPattern;
import de.uni_koblenz.west.splendid.statistics.RDFStatistics;

/**
 * @author OLaf Goerlitz
 */
public class DARQCardinalityEstimator extends VoidCardinalityEstimator {
	
	public DARQCardinalityEstimator(RDFStatistics stats) {
		super(stats);
	}
	
	@Override
	public String getName() {
		return "DARQCard";
	}
	
	@Override
	protected Number getPatternCard(MappedStatementPattern pattern, Graph source) {
		
		Value s = pattern.getSubjectVar().getValue();
		Value p = pattern.getPredicateVar().getValue();
		Value o = pattern.getObjectVar().getValue();
		
		// predicate must be bound
		if (p == null)
			throw new IllegalArgumentException("DARQ requires bound predicate: " + pattern);
		
		// handle rdf:type
		if (RDF.TYPE.equals(p) && o != null) {
			return stats.getTypeCount(source, o.stringValue());
		}
		
		// Subject is bound
		if (s != null) {
			
		}

		// use triple count containing the predicate
		return stats.getPredicateCount(source, p.stringValue());
	}

}
