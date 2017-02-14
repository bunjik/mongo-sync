/**
 *
 */
package info.bunji.mongodb.synces;

/**
 * Sync Status
 * @author Fumiharu Kinoshita
 */
public enum Status {
	UNKNOWN,
    STARTING,	// delete
    START_FAILED,
    RUNNING,
    STOPPED,
//    IMPORT_FAILED,
    INITIAL_IMPORTING,
    INITIAL_IMPORT_FAILED;

    public static final Status fromString(Object status) {
		Status ret = Status.UNKNOWN;
		for (Status s : values()) {
			if (s.toString().equals(status)) {
				ret = s;
				break;
			}
		}
		return ret;
	}
}
