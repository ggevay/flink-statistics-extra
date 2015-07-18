/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.extras.streaming;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.FieldAccessor;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Statistics extends org.apache.flink.contrib.streaming.DataStreamUtils.Statistics {

	/**
	 * Counts the distinct values in a data stream.
	 *
	 * Warning: Uses unbounded memory!
	 *
	 * @param stream
	 *            The {@link org.apache.flink.streaming.api.datastream.DataStream} to work with
	 * @return The transformed {@link org.apache.flink.streaming.api.datastream.DataStream}.
	 */
	public static <T> DataStream<Long> countDistinct(DataStream<T> stream) {
		return stream.map(new CountDistinctMapFunction<T>(new CountDistinctExact<T>())).setParallelism(1);
	}

	/**
	 * Counts the distinct values in a data stream at the specified field
	 * position, and writes the result to another (or back to the same) field.
	 *
	 * The output field must be of type Integer.
	 *
	 * Warning: Uses unbounded memory!
	 *
	 * @param inPos
	 *            The input position in the tuple/array
	 * @param outPos
	 *            The output position in the tuple/array
	 * @param stream
	 *            The {@link DataStream} to work with
	 * @return The transformed {@link DataStream}.
	 */
	public static <T> DataStream<T> countDistinct(int inPos, int outPos,
												  DataStream<T> stream) {
		return stream.map(new CountDistinctFieldMapFunction<T, Object>(
				FieldAccessor.create(inPos, stream.getType(), stream.getExecutionConfig()),
				FieldAccessor.<T, Long>create(outPos, stream.getType(), stream.getExecutionConfig()),
				new CountDistinctExact<Object>()
		)).setParallelism(1);
	}

	/**
	 * Counts the distinct values in a data stream at the specified field,
	 * and writes the result to another (or back to the same) field.
	 *
	 * The output field must be of type Integer.
	 *
	 * Warning: Uses unbounded memory!
	 *
	 * The fields can be specified by a field expression that can be either
	 * the name of a public field or a getter method with parentheses of the
	 * stream's underlying type. A dot can be used to drill down into objects,
	 * as in {@code "field1.getInnerField2()" }.
	 *
	 * @param inField
	 *            The input position in the tuple/array
	 * @param outField
	 *            The output position in the tuple/array
	 * @param stream
	 *            The {@link DataStream} to work with
	 * @return The transformed {@link DataStream}.
	 */
	public static <T> DataStream<T> countDistinct(String inField, String outField,
												  DataStream<T> stream) {
		return stream.map(new CountDistinctFieldMapFunction<T, Object>(
				FieldAccessor.create(inField, stream.getType(), stream.getExecutionConfig()),
				FieldAccessor.<T, Long>create(outField, stream.getType(), stream.getExecutionConfig()),
				new CountDistinctExact<Object>()
		)).setParallelism(1);
	}

	private static class CountDistinctExact<T> implements CountDistinct<T>, Serializable {

		private static final long serialVersionUID = 1L;

		Set<T> seen = new HashSet<T>();
		long result = 0;

		@Override
		public Long offer(T elem) {
			if (!seen.contains(elem)) {
				seen.add(elem);
				result++;
			}
			return result;
		}
	}
}
