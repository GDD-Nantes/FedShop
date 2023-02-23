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

import static de.uni_koblenz.west.splendid.config.FederationSailSchema.SELECTOR_TYPE;
import static de.uni_koblenz.west.splendid.config.FederationSailSchema.USE_TYPE_STATS;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.ValueFactoryImpl;
import org.eclipse.rdf4j.sail.config.SailConfigException;

/**
 * Configuration settings for the sources selector.
 * 
 * @author Olaf Goerlitz
 */
@SuppressWarnings({"deprecation","removal","dep-ann"})
public class SourceSelectorConfig extends AbstractSailConfig {
	
	/** @deprecated */
	private boolean useTypeStats;
	
	protected SourceSelectorConfig() {
		super(SELECTOR_TYPE);
	}
	
	protected SourceSelectorConfig(String type) {
		this.setType(type);
	}
	
	public static SourceSelectorConfig create(Model model, Resource implNode) throws SailConfigException {
		SourceSelectorConfig config = new SourceSelectorConfig();
		config.parse(model, implNode);
		return config;
	}
	
	/** @deprecated */
	public boolean isUseTypeStats() {
		return this.useTypeStats;
	}
	
	@Override
	public Resource export(Model model) {
		ValueFactory vf = ValueFactoryImpl.getInstance();
		
		Resource self = super.export(model);
		model.add(self, USE_TYPE_STATS, vf.createLiteral(this.useTypeStats));
		
		return self;
	}

	@Override
	public void parse(Model model, Resource implNode) throws SailConfigException {
		super.parse(model, implNode);
		
		this.useTypeStats = getObjectBoolean(model, implNode, USE_TYPE_STATS, true);
	}

}
