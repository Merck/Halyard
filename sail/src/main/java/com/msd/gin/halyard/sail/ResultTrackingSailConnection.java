package com.msd.gin.halyard.sail;

import org.eclipse.rdf4j.sail.SailConnection;

public interface ResultTrackingSailConnection extends SailConnection {
	boolean isTrackResultSize();

	void setTrackResultSize(boolean f);

	boolean isTrackResultTime();

	void setTrackResultTime(boolean f);
}
