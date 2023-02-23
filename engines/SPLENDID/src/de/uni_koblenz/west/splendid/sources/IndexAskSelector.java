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

import org.eclipse.rdf4j.query.algebra.StatementPattern;

import de.uni_koblenz.west.splendid.index.Graph;
import de.uni_koblenz.west.splendid.statistics.RDFStatistics;
import de.uni_koblenz.west.splendid.test.config.Configuration;

/**
 * A source selector which first uses the index to find data sources which can
 * return results and then contacts the SPARQL Endpoints asking them if they
 * can really return results for a triple pattern. 
 * 
 * @author Olaf Goerlitz
 */
public class IndexAskSelector extends AskSelector {
	
	private IndexSelector indexSel;
	
	/**
	 * Creates a source finder using the supplied statistics and sources.
	 * 
	 * @param stats the statistics to use.
	 */
	public IndexAskSelector(boolean useTypeStats) {
		this.indexSel = new IndexSelector(useTypeStats);
	}
	
	@Override
	public void setStatistics(RDFStatistics stats) {
		super.setStatistics(stats);
		this.indexSel.setStatistics(stats);
	}
	
	@Override
	protected Set<Graph> getSources(StatementPattern pattern, Configuration config) {
		Set<Graph> sources = this.indexSel.getSources(pattern, config);
		return getSources(pattern, sources, config);
	}
	
}
