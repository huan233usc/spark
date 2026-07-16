/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class LocalRelationLeftAntiJoinSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, "")

  test("multi-row LocalRelation left anti join collects without a Spark job") {
    val left = Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")).toDF("id", "value")
    val right = Seq(2, 4).toDF("id")
    val result = left.join(right, Seq("id"), "left_anti")

    assert(result.queryExecution.optimizedPlan.isInstanceOf[LocalRelation])
    assert(result.queryExecution.executedPlan.isInstanceOf[LocalTableScanExec])

    val jobCount = new AtomicInteger(0)
    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        jobCount.incrementAndGet()
      }
    }

    spark.sparkContext.listenerBus.waitUntilEmpty()
    spark.sparkContext.addSparkListener(listener)
    try {
      assert(result.collect().toSeq === Seq(Row(1, "a"), Row(3, "c")))
      spark.sparkContext.listenerBus.waitUntilEmpty()
      assert(jobCount.get() === 0)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }
}
