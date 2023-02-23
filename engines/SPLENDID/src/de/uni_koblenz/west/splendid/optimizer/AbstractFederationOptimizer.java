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
package de.uni_koblenz.west.splendid.optimizer;

import java.util.List;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uni_koblenz.west.splendid.estimation.AbstractCostEstimator;
import de.uni_koblenz.west.splendid.estimation.ModelEvaluator;
import de.uni_koblenz.west.splendid.helpers.AnnotatingTreePrinter;
import de.uni_koblenz.west.splendid.helpers.FilterConditionCollector;
import de.uni_koblenz.west.splendid.model.BasicGraphPatternExtractor;
import de.uni_koblenz.west.splendid.model.MappedStatementPattern;
import de.uni_koblenz.west.splendid.model.SubQueryBuilder;
import de.uni_koblenz.west.splendid.sources.SourceSelector;

/**
 * Base functionality for federated query optimizers
 * 
 * @author Olaf Goerlitz
 */
@SuppressWarnings({"deprecation","removal"})
public abstract class AbstractFederationOptimizer implements QueryOptimizer {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFederationOptimizer.class);
	
	protected SourceSelector sourceSelector;
	protected SubQueryBuilder queryBuilder;
	protected AbstractCostEstimator costEstimator;
	protected ModelEvaluator modelEvaluator;
	
	/**
	 * To be implemented by sub classes.
	 * 
	 * @param query the Query to optimize.
	 */
	public abstract TupleExpr optimizeBGP(TupleExpr query);
	
	// -------------------------------------------------------------------------
	
	public SourceSelector getSelector() {
		return sourceSelector;
	}

	public void setSelector(SourceSelector sourceSelector) {
		if (sourceSelector == null)
			throw new IllegalArgumentException("source selector must not be null");
		this.sourceSelector = sourceSelector;
	}

	public SubQueryBuilder getBuilder() {
		return queryBuilder;
	}

	public void setBuilder(SubQueryBuilder queryBuilder) {
		if (queryBuilder == null)
			throw new IllegalArgumentException("pattern group builder must not be null");
		this.queryBuilder = queryBuilder;
	}

	public AbstractCostEstimator getCostEstimator() {
		return costEstimator;
	}

	public void setCostEstimator(AbstractCostEstimator costEstimator) {
		if (costEstimator == null)
			throw new IllegalArgumentException("cost estimator must not be null");
		this.costEstimator = costEstimator;
	}
	
	public ModelEvaluator getModelEvaluator() {
		return modelEvaluator;
	}

	public void setModelEvaluator(ModelEvaluator modelEvaluator) {
		this.modelEvaluator = modelEvaluator;
	}
	
	// -------------------------------------------------------------------------
	
	protected List<TupleExpr> getBaseExpressions(TupleExpr expr) {
		
		// get patterns and filter conditions from query model
		List<StatementPattern> patterns = StatementPatternCollector.process(expr);
		List<ValueExpr> conditions = FilterConditionCollector.process(expr);
		
		// create patterns with source mappings
		List<MappedStatementPattern> mappedPatterns = this.sourceSelector.mapSources(patterns, null);
		
		return this.queryBuilder.createSubQueries(mappedPatterns, conditions);
	}
	
	@Override
	public void optimize(TupleExpr query, Dataset dataset, BindingSet bindings) {  // Sesame 2
		
		// collect all basic graph patterns
		for (TupleExpr bgp : BasicGraphPatternExtractor.process(query)) {
			
//			// a single statement pattern needs no optimization
//			// TODO: need sources first
//			if (bgp instanceof StatementPattern)
//				continue;
			
			if (LOGGER.isTraceEnabled())
				LOGGER.trace("BGP before optimization:\n" + AnnotatingTreePrinter.print(bgp));

			bgp = optimizeBGP(bgp);
			
			if (LOGGER.isTraceEnabled() && modelEvaluator != null)
				LOGGER.trace("BGP after optimization:\n" + AnnotatingTreePrinter.print(bgp, modelEvaluator));
		}
		
	}
	
}
