
package com.apple.tool;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {
	private final VerifiableProperties props;

	public SimplePartitioner(final VerifiableProperties props) {
		this.props = props;
	}

	public VerifiableProperties getProps() {
		return props;
	}

	@Override
	public int partition(Object key, int numPartitions) {
		int partition = 0;
		final int offset = ((String) key).lastIndexOf('.');
		if (offset > 0) {
			partition = Integer.parseInt(((String) key).substring(offset + 1)) % numPartitions;
		}
		return partition;
	}

}
