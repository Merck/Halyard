/*
 * Copyright 2016 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.msd.gin.halyard.strategy;

import org.eclipse.rdf4j.query.parser.sparql.SPARQLUpdateTest;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;

/**
 *
 * @author Adam Sotona (MSD)
 */
public class HalyardSPARQLUpdateTest  extends SPARQLUpdateTest {

	@Override
	protected Repository newRepository()
		throws Exception
	{
		return new SailRepository(new MemoryStoreWithHalyardStrategy());
	}

}