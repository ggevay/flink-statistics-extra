/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.extras.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.DataStreamUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

/**
 * This test verifies the behavior of DataStreamUtils.collect.
 */
public class CountDistinctITCase {

	final Integer[] inputData = new Integer[]{1,2,3,2,4,5,6,5,3,7,8,9,9,10,9,8,10,11};
	final int[] referenceOutput = new int[]{1,2,3,3,4,5,6,6,6,7,8,9,9,10,10,10,10,11};

	@Test
	public void testCountDistinctExact() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		DataStream<Integer> input = env.fromArray(inputData);

		DataStream<Tuple2<Integer, Long>> result = Statistics.countDistinct(0, 1,
				input.map(new MapFunction<Integer, Tuple2<Integer, Long>>() {
					@Override
					public Tuple2<Integer, Long> map(Integer value) throws Exception {
						return new Tuple2<Integer, Long>(value, -1l);
					}
				})
		);

		int i = 0;
		for(Iterator<Tuple2<Integer, Long>> it = DataStreamUtils.collect(result); it.hasNext(); ) {
			Tuple2<Integer, Long> x = it.next();
			if(x.f1 != referenceOutput[i]) {
				Assert.fail(String.format("Should have got %d, got %d instead.", referenceOutput[i], x.f1));
			}
			i++;
		}
		if(i != referenceOutput.length) {
			Assert.fail(String.format("Should have collected %d numbers, got %d instead.", referenceOutput.length, i));
		}
	}
}
