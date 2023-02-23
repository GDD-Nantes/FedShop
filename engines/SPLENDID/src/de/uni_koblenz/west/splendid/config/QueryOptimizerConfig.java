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

import static de.uni_koblenz.west.splendid.config.FederationSailSchema.ESTIMATOR;
import static de.uni_koblenz.west.splendid.config.FederationSailSchema.EVAL_STRATEGY;
import static de.uni_koblenz.west.splendid.config.FederationSailSchema.GROUP_BY_SAMEAS;
import static de.uni_koblenz.west.splendid.config.FederationSailSchema.GROUP_BY_SOURCE;
import static de.uni_koblenz.west.splendid.config.FederationSailSchema.OPT_TYPE;
import static de.uni_koblenz.west.splendid.config.FederationSailSchema.USE_BIND_JOIN;
import static de.uni_koblenz.west.splendid.config.FederationSailSchema.USE_HASH_JOIN;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.sail.config.SailConfigException;

/**
 * Configuration settings for the query optimizer.
 * 
 * @author Olaf Goerlitz
 */
public class QueryOptimizerConfig extends AbstractSailConfig {
	
	private static final String DEFAULT_ESTIMATOR_TYPE = "INDEX_ASK";
	
	private String estimatorType = DEFAULT_ESTIMATOR_TYPE;
	
	private boolean groupBySameAs = false;
	private boolean groupBySource = true;
	
	private boolean useBindJoin = true;
	private boolean useHashJoin = true;
	
	private EvaluationStrategy evalStrategy;
	
	protected QueryOptimizerConfig() {
		super(OPT_TYPE);
	}
	
	protected QueryOptimizerConfig(String type) {
		this.setType(type);
	}
	
	public static QueryOptimizerConfig create(Model model, Resource implNode) throws SailConfigException {
		QueryOptimizerConfig config = new QueryOptimizerConfig();
		config.parse(model, implNode);
		return config;
	}
	
	public String getEstimatorType() {
		return this.estimatorType;
	}
	
	public EvaluationStrategy getEvalStrategy() {
		return this.evalStrategy;
	}
	
	public boolean isGroupBySameAs() {
		return this.groupBySameAs;
	}

	public boolean isGroupBySource() {
		return this.groupBySource;
	}

	
	public boolean isUseBindJoin() {
		return this.useBindJoin;
	}
	
	public boolean isUseHashJoin() {
		return this.useHashJoin;
	}

	@Override
	public Resource export(Model model) {
		ValueFactory vf = SimpleValueFactory.getInstance();
		
		Resource self = super.export(model);
		
		model.add(self, ESTIMATOR, vf.createLiteral(this.estimatorType));
		
		model.add(self, GROUP_BY_SAMEAS, vf.createLiteral(this.groupBySameAs));
		model.add(self, GROUP_BY_SOURCE, vf.createLiteral(this.groupBySource));
		
		model.add(self, USE_BIND_JOIN, vf.createLiteral(this.useBindJoin));
		model.add(self, USE_HASH_JOIN, vf.createLiteral(this.useHashJoin));
		
		model.add(self, EVAL_STRATEGY, vf.createLiteral(this.evalStrategy.getClass().getName()));
		
		return self;
	}

	@Override
	public void parse(Model model, Resource implNode) throws SailConfigException {
		super.parse(model, implNode);
		
		Literal estimator = getObjectLiteral(model, implNode, ESTIMATOR);
		if (estimator != null) {
			this.estimatorType = estimator.getLabel();
		} else {
			this.estimatorType = DEFAULT_ESTIMATOR_TYPE;
		}
		
		this.groupBySameAs = getObjectBoolean(model, implNode, GROUP_BY_SAMEAS, this.groupBySameAs);
		this.groupBySource = getObjectBoolean(model, implNode, GROUP_BY_SOURCE, this.groupBySource);
		
		this.useBindJoin = getObjectBoolean(model, implNode, USE_BIND_JOIN, this.useBindJoin);
		this.useHashJoin = getObjectBoolean(model, implNode, USE_HASH_JOIN, this.useHashJoin);
		
		Literal className = getObjectLiteral(model, implNode, EVAL_STRATEGY);
		if (className != null) {
			try {
				this.evalStrategy = (EvaluationStrategy) Class.forName(className.stringValue()).newInstance();
			} catch (ClassNotFoundException e) {
				throw new SailConfigException("unknown evaluation strategy impl: " + className);
			} catch (InstantiationException e) {
				throw new SailConfigException("failed to create evaluation strategy impl: " + className, e);
			} catch (IllegalAccessException e) {
				throw new SailConfigException("failed to create evaluation strategy impl: " + className, e);
			} catch (ClassCastException e) {
				throw new SailConfigException(className + " is not an EvaluationStrategy", e);
			}
		}
	}

	@Override
	public void validate() throws SailConfigException {
		super.validate();
		
		if (this.estimatorType == null)
			throw new SailConfigException("no cardinality estimator specified: use " + ESTIMATOR);
		
		if (this.useHashJoin == false && this.useBindJoin == false)
			throw new SailConfigException("cannot create joins: all physical join types are set to false");
		
		// TODO: check for valid estimator settings
	}

}
