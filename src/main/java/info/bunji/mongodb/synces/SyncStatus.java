/**
 * 
 */
package info.bunji.mongodb.synces;

import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.bson.BsonTimestamp;

/**
 ************************************************
 * 
 * @author Fumiharu Kinoshita
 ************************************************
 */
public final class SyncStatus {
	/** sync status */
	private Status status;
	/** last sync oplog timestamp */
	private BsonTimestamp lastOpTime;
	/** last document sync timestamp */
	private BsonTimestamp lastSyncTime;
	/**  */
	private String lastError;

	@SuppressWarnings("unchecked")
	public SyncStatus(Map<String, Object> map) {
		this.status = Status.fromString(map.get("status"));
		Map<String, Number> ts = (Map<String, Number>) map.get("lastOpTime");
		if (ts != null) {
			int sec = ts.get("seconds").intValue();
			int inc = ts.get("inc").intValue();
			this.lastOpTime = new BsonTimestamp(sec, inc);
		}
		ts = (Map<String, Number>) map.get("lastSyncTime");
		if (ts != null) {
			int sec = ts.get("seconds").intValue();
			int inc = ts.get("inc").intValue();
			this.lastSyncTime = new BsonTimestamp(sec, inc);
		}
	}

	public Status getStatus() {
		return status;
	}

	public BsonTimestamp getLastOpTime() {
		return lastOpTime;
	}

	public BsonTimestamp getLastSyncTime() {
		return lastSyncTime;
	}

	public String getLastError() {
		return lastError;
	}

	public void setLastSyncTime(BsonTimestamp lastSyncTime) {
		this.lastSyncTime = lastSyncTime;
	}

	public void setLastError(String lastError) {
		this.lastError = lastError;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
