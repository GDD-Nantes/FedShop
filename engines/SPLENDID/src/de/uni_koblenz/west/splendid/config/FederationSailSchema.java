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
package de.uni_koblenz.west.splendid.config;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * RDF Schema used by the federation configuration.
 * 
 * @author Olaf Goerlitz
 */
public class FederationSailSchema {
	
	private static final ValueFactory vf = SimpleValueFactory.getInstance();
	
	/** The SailRepository schema namespace 
	 * (<tt>http://west.uni-koblenz.de/config/federation/sail#</tt>). */
	public static final String NAMESPACE = "http://west.uni-koblenz.de/config/federation/sail#";
	
	public static final IRI MEMBER    = vf.createIRI(NAMESPACE + "member");
	public static final IRI QUERY_OPT = vf.createIRI(NAMESPACE + "queryOptimization");
	public static final IRI OPT_TYPE  = vf.createIRI(NAMESPACE + "optimizerType");
	public static final IRI SRC_SELECTION = vf.createIRI(NAMESPACE + "sourceSelection");
	public static final IRI SELECTOR_TYPE = vf.createIRI(NAMESPACE + "selectorType");
	public static final IRI USE_TYPE_STATS = vf.createIRI(NAMESPACE + "useTypeStats");
	public static final IRI GROUP_BY_SAMEAS = vf.createIRI(NAMESPACE + "groupBySameAs");
	public static final IRI GROUP_BY_SOURCE = vf.createIRI(NAMESPACE + "groupBySource");
	public static final IRI USE_BIND_JOIN = vf.createIRI(NAMESPACE + "useBindJoin");
	public static final IRI USE_HASH_JOIN = vf.createIRI(NAMESPACE + "useHashJoin");
	public static final IRI ESTIMATOR = vf.createIRI(NAMESPACE + "cardEstimator");
	public static final IRI STATISTIC = vf.createIRI(NAMESPACE + "statistic");
	public static final IRI VOID_IRI  = vf.createIRI(NAMESPACE + "voidDescription");
	public static final IRI EVAL_STRATEGY  = vf.createIRI(NAMESPACE + "evalStrategy");

}
