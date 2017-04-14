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
	private Status status;
	private BsonTimestamp lastOpTime;
	private String lastError;

	public SyncStatus(Map<String, Object> map) {
		this.status = Status.fromString(map.get("status"));
		@SuppressWarnings("unchecked")
		Map<String, Number> ts = (Map<String, Number>) map.get("lastOpTime");
		if (ts != null) {
			int sec = ts.get("seconds").intValue();
			int inc = ts.get("inc").intValue();
			this.lastOpTime = new BsonTimestamp(sec, inc);
		}
	}

	public Status getStatus() {
		return status;
	}

	public BsonTimestamp getLastOpTime() {
		return lastOpTime;
	}

//	public String toJson() {
//		return EsUtils.makeStatusJson(status, null, lastOpTime);
//	}

	public String getLastError() {
		return lastError;
	}

	public void setLastError(String lastError) {
		this.lastError = lastError;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
