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
package de.uni_koblenz.west.splendid.vocabulary;

/**
 * RDF vocabulary definition.
 * 
 * @author goerlitz@uni-koblenz.de
 */
public enum RDF {
	
	Property,
	Statement,
	Bag,
	Seq,
	Alt,
	List,
	
	type,
	subject,
	predicate,
	object,
	value,
	first,
	rest;
	
	public static final String NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	
	private final String uri;
	
	private RDF() {
		this.uri = NAMESPACE + super.toString();
	}
	
	public String toString() {
		return this.uri;
	}
	
}
