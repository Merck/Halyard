package com.msd.gin.halyard.strategy;

public final class StrategyConfig {

	static final String HASH_JOIN_LIMIT = "halyard.evaluation.hashJoin.limit";
	static final String HASH_JOIN_COST_RATIO = "halyard.evaluation.hashJoin.costRatio";
	static final String MEMORY_THRESHOLD = "halyard.evaluation.collections.memoryThreshold";
	static final String HALYARD_EVALUATION_POLL_TIMEOUT_MILLIS = "halyard.evaluation.pollTimeoutMillis";
	static final String HALYARD_EVALUATION_MAX_QUEUE_SIZE = "halyard.evaluation.maxQueueSize";
	static final String HALYARD_EVALUATION_MAX_THREADS = "halyard.evaluation.maxThreads";
	static final String HALYARD_EVALUATION_THREAD_GAIN = "halyard.evaluation.threadGain";
	static final String HALYARD_EVALUATION_RETRY_LIMIT = "halyard.evaluation.retryLimit";
	static final String HALYARD_EVALUATION_MAX_RETRIES = "halyard.evaluation.maxRetries";
	static final String HALYARD_EVALUATION_THREADS = "halyard.evaluation.threads";

	static final int DEFAULT_HASH_JOIN_LIMIT = 50000;
	static final int DEFAULT_MEMORY_THRESHOLD = 100000;
}
