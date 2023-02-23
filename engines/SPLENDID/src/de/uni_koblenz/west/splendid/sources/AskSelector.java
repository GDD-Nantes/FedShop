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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uni_koblenz.west.splendid.helpers.OperatorTreePrinter;
import de.uni_koblenz.west.splendid.helpers.QueryExecutor;
import de.uni_koblenz.west.splendid.index.Graph;
import de.uni_koblenz.west.splendid.statistics.VoidStatistics;
import de.uni_koblenz.west.splendid.test.config.Configuration;

/**
 * A source selector which contacts SPARQL Endpoints asking them whether
 * they can return results for a triple pattern or not. 
 * 
 * @author Olaf Goerlitz
 */
public class AskSelector extends SourceSelectorBase {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AskSelector.class);
	
	private List<Graph> sourceList;
	
	@Override
	public void init() throws SailException {
		super.init();
		this.sourceList = ((VoidStatistics) stats).getEndpoints();
		
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("initialized ask selecector with: " + this.sourceList);
	}

	@Override
	protected Set<Graph> getSources(StatementPattern pattern, Configuration config) {
		return getSources(pattern, this.sourceList, config);
	}
	
	protected Set<Graph> getSources(StatementPattern pattern, Collection<Graph> sources, Configuration config) {
		Set<Graph> selectedSources = new HashSet<Graph>();
		
		if (LOGGER.isDebugEnabled())
			LOGGER.debug(debugAskRequest(pattern));
		
		String sparqlPattern = OperatorTreePrinter.print(pattern);
		
		// ask each source for current pattern
		for (Graph source : sources) {
			if (QueryExecutor.ask(source.toString(), sparqlPattern, config))
				selectedSources.add(source);
		}
		return selectedSources;
	}
	
	private String debugAskRequest(StatementPattern pattern) {
		StringBuffer buffer = new StringBuffer("ASK {");
		buffer.append(OperatorTreePrinter.print(pattern));
		buffer.append("} @[");
		for (Graph source : sourceList) {
			buffer.append(source.getNamespaceURL()).append(", ");
		}
		buffer.setLength(buffer.length()-2);
		buffer.append("]");
		return buffer.toString();
	}

}
