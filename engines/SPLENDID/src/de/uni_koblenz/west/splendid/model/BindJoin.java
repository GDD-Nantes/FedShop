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

import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

/**
 * @author Olaf Goerlitz
 */
public class BindJoin extends Join {
	
	public BindJoin(TupleExpr leftArg, TupleExpr rightArg) {
		super(leftArg, rightArg);
	}
	
	@Override
	public boolean equals(Object other) {
		return other instanceof BindJoin && super.equals(other);
	}

	@Override
	public int hashCode() {
		return super.hashCode() ^ "BindJoin".hashCode();
	}

	@Override
	public BindJoin clone() {
		return (BindJoin)super.clone();
	}

}
