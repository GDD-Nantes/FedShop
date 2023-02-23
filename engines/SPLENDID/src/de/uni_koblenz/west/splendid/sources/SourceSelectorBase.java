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
package de.uni_koblenz.west.splendid.sources;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uni_koblenz.west.splendid.helpers.OperatorTreePrinter;
import de.uni_koblenz.west.splendid.index.Graph;
import de.uni_koblenz.west.splendid.model.MappedStatementPattern;
import de.uni_koblenz.west.splendid.statistics.RDFStatistics;

import de.uni_koblenz.west.splendid.test.config.Configuration;

/**
 * Basic behavior of a source selector.
 * Aggregates query patterns in groups with same constant values but different
 * variables before the source selection is done.
 * 
 * @author Olaf Goerlitz
 */
public abstract class SourceSelectorBase implements SourceSelector {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SourceSelectorBase.class);
	
	protected RDFStatistics stats;

	/**
	 * Return all sources for the supplied pattern.
	 * 
	 * @param pattern the statement pattern to process.
	 * @return a set of sources.
	 */
	protected abstract Set<Graph> getSources(StatementPattern pattern, Configuration config);
	
	// --------------------------------------------------------------
	
	@Override
	public void init() throws SailException {
		if (this.stats == null)
			throw new SailException("need statistics for source selection");
	}
	
	public void setStatistics(RDFStatistics stats) {
		if (stats == null)
			throw new IllegalArgumentException("statistics must not be null");
		this.stats = stats;
	}
	
	/**
	 * Assigns data sources to query patterns.
	 * 
	 * @param patterns the list of patterns to be processed.
	 * @return the list of patterns with data source mappings.
	 */
	public List<MappedStatementPattern> mapSources(List<StatementPattern> patterns, Configuration config) {
		
		List<MappedStatementPattern> pMap = new ArrayList<MappedStatementPattern>();
		
		// group patterns with same constant values but different variables
		TriplePatternIndex pso = new TriplePatternIndex(patterns);
		
		// determine sources for all distinct pattern groups
		for (List<StatementPattern> patternGroup : pso.getDistinctPatterns()) {
			
			// get sources for the first pattern in group (with same constants)
			StatementPattern firstPattern = patternGroup.get(0);
			Set<Graph> sources = getSources(firstPattern, config);
			
			// print warning if no sources were found
			if (sources.size() == 0) {
				LOGGER.warn("cannot find any source for: " + OperatorTreePrinter.print(firstPattern));
			}
			
			for (StatementPattern pattern : patternGroup) {
				pMap.add(new MappedStatementPattern(pattern, sources));
			}
		}
		return pMap;
	}
	
}
