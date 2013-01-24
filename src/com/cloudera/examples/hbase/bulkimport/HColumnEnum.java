package com.cloudera.examples.hbase.bulkimport;

/**
 * HBase table columns for the 'srv' column family
 */
public enum HColumnEnum {
	SRV_COL_B("c1".getBytes()),
	SRV_COL_C("c2"
			.getBytes());

	private final byte[] columnName;

	HColumnEnum(byte[] column) {
		this.columnName = column;
	}

	public byte[] getColumnName() {
		return this.columnName;
	}
}
