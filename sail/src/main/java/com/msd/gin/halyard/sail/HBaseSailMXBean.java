package com.msd.gin.halyard.sail;

import com.msd.gin.halyard.sail.HBaseSail.QueryInfo;
import com.msd.gin.halyard.sail.HBaseSail.ScanSettings;

public interface HBaseSailMXBean {
	boolean isPushStrategyEnabled();
	int getEvaluationTimeout();
	ElasticSettings getElasticSettings();
	int getValueIdentifierSize();
	String getValueIdentifierAlgorithm();
	ScanSettings getScanSettings();

	boolean isTrackResultSize();
	void setTrackResultSize(boolean f);

	boolean isTrackResultTime();
	void setTrackResultTime(boolean f);

	QueryInfo[] getRecentQueries();
}
