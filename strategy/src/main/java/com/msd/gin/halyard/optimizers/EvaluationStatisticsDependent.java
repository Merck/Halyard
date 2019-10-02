package com.msd.gin.halyard.optimizers;

import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;

public interface EvaluationStatisticsDependent {

	void setEvaluationStatistics(EvaluationStatistics evaluationStatistics);
}
