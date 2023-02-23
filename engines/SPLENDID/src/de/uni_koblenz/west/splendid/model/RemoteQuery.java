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

import java.util.Set;

import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;

import de.uni_koblenz.west.splendid.index.Graph;

/**
 * Query Model Node which marks a sub tree to be a group of StatementPatterns which are executed class which defines that all child arguments should be executed
 * in one block on a SPARQL endpoint.
 * 
 * @author Olaf Goerlitz
 */
@SuppressWarnings({"deprecation","removal"})
public class RemoteQuery extends UnaryTupleOperator {
	
	public RemoteQuery(TupleExpr expr) {
		super(expr);
	}

	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor) throws X {
		visitor.meetOther(this);
	}
	
	public Set<Graph> getSources() {
		StatementPattern p = StatementPatternCollector.process(this).get(0);
		if (p instanceof MappedStatementPattern)
			return ((MappedStatementPattern) p).getSources();
		else
			return null;
	}
	
}
