/**
 *
 */
package info.bunji.mongodb.synces;

/**
 *
 * @author Fumiharu Kinoshita
 */
public enum Status {
	UNKNOWN,
    STARTING,
    START_FAILED,
    RUNNING,
    STOPPED,
    IMPORT_FAILED,
    INITIAL_IMPORTING,
    INITIAL_IMPORT_FAILED,
}
