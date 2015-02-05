package org.elasticsearch.river.tibcoas;

public enum Status 
{
	UNKNOWN,
    /** River should be started */
    START_PENDING,
    /** River is actually starting up */
    STARTING,
    START_FAILED,
    RUNNING,
    STOPPED,
    IMPORT_FAILED,
    INITIAL_IMPORT_FAILED,
}
