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

import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.QueryModelVisitorBase;

/**
 * @author Olaf Goerlitz
 */
public abstract class AbstractCostEstimator extends QueryModelVisitorBase<RuntimeException> implements ModelEvaluator {
	
	protected double cost;
	
	protected AbstractCardinalityEstimator cardEst;
	
	public AbstractCardinalityEstimator getCardinalityEstimator() {
		return cardEst;
	}

	public void setCardinalityEstimator(AbstractCardinalityEstimator cardEst) {
		this.cardEst = cardEst;
	}

	public Double getCost(TupleExpr expr) {
		cost = 0;
		expr.visit(this);
		return cost;
	}
	
	@Override
	public Double process(TupleExpr expr) {
		synchronized (this) {
			return getCost(expr);
		}
	}
	
}
