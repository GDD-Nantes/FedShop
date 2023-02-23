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

import java.util.List;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.sail.SailException;

import de.uni_koblenz.west.splendid.model.MappedStatementPattern;
import de.uni_koblenz.west.splendid.statistics.RDFStatistics;

import de.uni_koblenz.west.splendid.test.config.Configuration;

/**
 * Interface for source selection strategies.
 * 
 * @author Olaf Goerlitz
 */
public interface SourceSelector {
	
	public void init() throws SailException;
	
	public void setStatistics(RDFStatistics stats);
	
	/**
	 * Maps triple patterns to data sources which can contribute results.
	 * 
	 * @param patterns the SPARQL triple patterns which need to be mapped.
	 * @return a list triple patterns with mappings to sources.
	 */
	public List<MappedStatementPattern> mapSources(List<StatementPattern> patterns, Configuration config);

}
