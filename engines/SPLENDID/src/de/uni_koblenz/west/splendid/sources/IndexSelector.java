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
package de.uni_koblenz.west.splendid.sources;

import java.util.Set;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

import de.uni_koblenz.west.splendid.index.Graph;

import de.uni_koblenz.west.splendid.test.config.Configuration;

/**
 * Source selection for the supplied basic graph patterns.
 * Pattern subsets are built for the same set of matched sources. 
 * 
 * @author Olaf Goerlitz
 */
public class IndexSelector extends SourceSelectorBase {
	
	private boolean useTypeStats = true; 
	
	/**
	 * Creates a source finder using the supplied statistics.
	 * 
	 * @param stats the statistics to use.
	 */
	public IndexSelector(boolean useTypeStats) {
		this.useTypeStats = useTypeStats;
	}

	/**
	 * Find matching sources for the supplied pattern.
	 * 
	 * @param patterns the pattern that need to matched to sources.
	 * @return the set of source which can contribute results for the pattern.
	 */
	@Override
	protected Set<Graph> getSources(StatementPattern pattern, Configuration config) {
		Value s = pattern.getSubjectVar().getValue();
		Value p = pattern.getPredicateVar().getValue();
		Value o = pattern.getObjectVar().getValue();
		
		return stats.findSources(s == null ? null : s.stringValue(),
								 p == null ? null : p.stringValue(),
								 o == null ? null : o.stringValue(), useTypeStats);
	}
	
}
