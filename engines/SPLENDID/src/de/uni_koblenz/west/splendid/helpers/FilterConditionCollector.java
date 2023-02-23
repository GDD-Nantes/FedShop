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
package de.uni_koblenz.west.splendid.helpers;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.helpers.QueryModelVisitorBase;

/**
 * A QueryModelVisitor that collects filter conditions from a query model.
 * 
 * @author Olaf Goerlitz
 */
public class FilterConditionCollector extends QueryModelVisitorBase<RuntimeException> {

	public static List<ValueExpr> process(QueryModelNode node) {
		FilterConditionCollector collector = new FilterConditionCollector();
		node.visit(collector);
		return collector.conditions;
	}

	private List<ValueExpr> conditions = new ArrayList<ValueExpr>();

	@Override
	public void meet(Filter node)
	{
		node.getArg().visit(this);
		conditions.add(node.getCondition());
	}

}
