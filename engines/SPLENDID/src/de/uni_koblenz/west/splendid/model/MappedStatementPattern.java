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
package de.uni_koblenz.west.splendid.model;

import java.util.HashSet;
import java.util.Set;

import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

import de.uni_koblenz.west.splendid.helpers.OperatorTreePrinter;
import de.uni_koblenz.west.splendid.index.Graph;

/**
 * A StatementPattern with assigned data source mappings.
 * 
 * @author Olaf Goerlitz
 */
public class MappedStatementPattern extends StatementPattern {
	
	private Set<Graph> sources = new HashSet<Graph>();
	
	public MappedStatementPattern(StatementPattern pattern, Set<Graph> sources) {
		super(pattern.getScope(), pattern.getSubjectVar(), pattern.getPredicateVar(), pattern.getObjectVar(), pattern.getContextVar());
		this.setSources(sources);
	}

	public Set<Graph> getSources() {
		return sources;
	}

	public void setSources(Set<Graph> sources) {
		if (sources == null)
			throw new IllegalArgumentException("source set is null");
		this.sources = sources;
	}
	
	public void addSource(Graph source) {
		this.sources.add(source);
	}
	
	public boolean removeSource(Graph source) {
		return this.sources.remove(source);
	}
	
	// -------------------------------------------------------------------------
	
	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor)
			throws X {
		visitor.meet(this);
	}
	
	@Override
	public String toString() {
		return OperatorTreePrinter.print(this);
	}

}
