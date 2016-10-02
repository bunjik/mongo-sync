package info.bunji.mongodb.synces;

public enum Operation {
	INSERT("i"),
	UPDATE("u"),
	DELETE("d"),
	DROP_COLLECTION("dc"),
	DROP_DATABASE("dd"),
	COMMAND("c"),
	UNKNOWN(null);

	private String value;

	private Operation(String value) {
	    this.value = value;
	}

	public String getValue() {
		return value;
	}

	public static Operation valueOf(Object value) {
		Operation ret = UNKNOWN;
		if (value != null) {
			String v = value.toString();
			for (Operation op : Operation.values()) {
				if (v.equalsIgnoreCase(op.getValue())) {
					return op;
				}
			}
		}
		return ret;
	}
/*
	@Deprecated
    public static Operation fromString(String value) {
        if (value != null) {
            for (Operation operation : Operation.values()) {
                if (value.equalsIgnoreCase(operation.getValue())) {
                    return operation;
                }
            }
//            if (MongoDBRiver.OPLOG_UPDATE_ROW_OPERATION.equalsIgnoreCase(value)) {
//                return Operation.UPDATE;
//            }
        }
        return Operation.UNKNOWN;
    }
*/
}
