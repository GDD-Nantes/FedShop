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
package de.uni_koblenz.west.splendid.estimation;

import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.uni_koblenz.west.splendid.helpers.QueryExecutor;

/**
 * @author Olaf Goerlitz
 */
public class TrueCardinalityEstimator extends AbstractCardinalityEstimator {

	private static final Logger LOGGER = LoggerFactory.getLogger(TrueCardinalityEstimator.class);
	
	private EvaluationStrategy evalStrategy;
	
	public TrueCardinalityEstimator(EvaluationStrategy evalStrategy) {
		this.evalStrategy = evalStrategy;
	}
	
	@Override
	public String getName() {
		return "TrueCard";
	}
	
	@Override
	protected void meetNode(QueryModelNode node) throws RuntimeException {
		if (node instanceof TupleExpr) {
			TupleExpr expr = (TupleExpr) node;
			
			// check cardinality index first
			if (getIndexCard(expr) != null)
				return;
			
			try {
				int card = QueryExecutor.getSize(evalStrategy.evaluate(expr, EmptyBindingSet.getInstance()));
				
				// add cardinality to index
				setIndexCard(expr, (double) card);
				
			} catch (QueryEvaluationException e) {
				throw new RuntimeException("query evaluation failed", e);
			}
			
		} else {
			throw new IllegalArgumentException();
		}
	}
}
