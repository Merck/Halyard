/*******************************************************************************
 * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *******************************************************************************/
package com.msd.gin.halyard.spin.function;

import org.eclipse.rdf4j.query.algebra.evaluation.function.TupleFunction;

/**
 * Tagging interface for {@link TupleFunction}s that have some sort of lifespan.
 */
public interface TransientTupleFunction extends TupleFunction {
}