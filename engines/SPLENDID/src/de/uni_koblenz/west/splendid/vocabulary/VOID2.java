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
 * VOID 2 (draft) vocabulary definition.
 * 
 * @author goerlitz@uni-koblenz.de
 */
public enum VOID2 {
	
	// concepts
	Dataset,
	Linkset,
	EquiWidthHist,
	EuqiDepthHist,
	
	// predicates
	vocabulary,
	sparqlEndpoint,
	distinctSubjects,
	distinctObjects,
	triples,
	classes,
	entities,
	properties,
	classPartition,
	clazz("class"),
	propertyPartition,
	property,
	target,
	linkPredicate,
	
	histogram,
	minValue,
	maxValue,
	bucketLoad,
	buckets,
	bucketDef;
	
	public static final String NAMESPACE = "http://rdfs.org/ns/void#";
	
	private final String uri;
	
	private VOID2(String name) {
		this.uri = NAMESPACE + name;
	}
	
	private VOID2() {
		this.uri = NAMESPACE + super.toString();
	}
	
	public String toString() {
		return this.uri;
	}
	
}