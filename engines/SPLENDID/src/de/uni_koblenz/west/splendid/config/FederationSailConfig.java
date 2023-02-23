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

import static de.uni_koblenz.west.splendid.config.FederationSailSchema.MEMBER;
import static de.uni_koblenz.west.splendid.config.FederationSailSchema.QUERY_OPT;
import static de.uni_koblenz.west.splendid.config.FederationSailSchema.SRC_SELECTION;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfigBase;
import org.eclipse.rdf4j.sail.config.SailConfigException;

/**
 * Configuration details for federation setup including member descriptions.
 * 
 * @author Olaf Goerlitz
 */
public class FederationSailConfig extends AbstractSailConfig {
	
	private static final String DEFAULT_SOURCE_SELECTION = "INDEX_ASK";
	private static final String DEFAULT_OPTIMIZER_STRATEGY = "DYNAMIC_PROGRAMMING";
	
	private final List<RepositoryImplConfig> memberConfig = new ArrayList<RepositoryImplConfig>();
	private SourceSelectorConfig selectorConfig;
	private QueryOptimizerConfig optimizerConfig;
	
	/**
	 * Returns the configuration settings of the federation members.
	 * 
	 * @return the member repository configuration settings.
	 */
	public List<RepositoryImplConfig> getMemberConfigs() {
		return this.memberConfig;
	}
	
	/**
	 * Returns the configuration settings of the source selector.
	 * 
	 * @return the source selection configuration settings.
	 */
	public SourceSelectorConfig getSelectorConfig() {
		return this.selectorConfig;
	}
	
	public QueryOptimizerConfig getOptimizerConfig() {
		return this.optimizerConfig;
	}
	
	// -------------------------------------------------------------------------

	/**
	 * Adds all Sail configuration settings to a configuration model.
	 * 
	 * @param model the configuration model to be filled.
	 * @return the resource representing this Sail configuration.
	 */
	@Override
	public Resource export(Model model) {
		
		Resource self = super.export(model);
		
		for (RepositoryImplConfig member : this.memberConfig) {
			model.add(self, MEMBER, member.export(model));
		}
		
		model.add(self, SRC_SELECTION, this.selectorConfig.export(model));
		model.add(self, QUERY_OPT, this.optimizerConfig.export(model));
		
		return self;
	}

	/**
	 * Parses the configuration model.
	 * 
	 * @param model the configuration model.
	 * @param implNode the resource representing this federation sail.
	 */
	@Override
	public void parse(Model model, Resource implNode) throws SailConfigException {
		super.parse(model, implNode);
		
		// extract the repository settings for all defined federation members
		for (Value member : filter(model, implNode, MEMBER)) {
			if (member instanceof Resource) {
				try {
					this.memberConfig.add(RepositoryImplConfigBase.create(model, (Resource) member));
				} catch (RepositoryConfigException e) {
					throw new SailConfigException(e);
				}
			}
			else {
				throw new SailConfigException("Found literal for federation member node, expected a resource");
			}
		}
		
		// get source selection strategy
		Resource sourceSelection = getObjectResource(model, implNode, SRC_SELECTION);
		if (sourceSelection == null) {
			// using default setting
			selectorConfig = new SourceSelectorConfig(DEFAULT_SOURCE_SELECTION);
		} else {
			selectorConfig = SourceSelectorConfig.create(model, sourceSelection);
		}
		
		// get query optimization strategy
		Resource queryOptimization = getObjectResource(model, implNode, QUERY_OPT);
		if (queryOptimization == null) {
			// using default setting
			optimizerConfig = new QueryOptimizerConfig(DEFAULT_OPTIMIZER_STRATEGY);
		} else {
			optimizerConfig = QueryOptimizerConfig.create(model, queryOptimization);
			if (optimizerConfig.getType() == null)
				optimizerConfig.setType(DEFAULT_OPTIMIZER_STRATEGY);
		}
	}

	/**
	 * Validates this configuration. If the configuration is invalid a
	 * {@link StoreConfigException} is thrown including the reason why the
	 * configuration is invalid.
	 * 
	 * @throws StoreConfigException
	 *             If the configuration is invalid.
	 */
	@Override
	public void validate() throws SailConfigException {
		super.validate();
		if (memberConfig.size() == 0) {
			throw new SailConfigException("No federation members specified");
		}
		
		// validate all member repositories
		for (RepositoryImplConfig cfg : memberConfig) {
			try {
				cfg.validate();
			} catch (RepositoryConfigException e) {
				throw new SailConfigException(e);
			}
		}
		
		this.selectorConfig.validate();
		this.optimizerConfig.validate();
	}
	
}
