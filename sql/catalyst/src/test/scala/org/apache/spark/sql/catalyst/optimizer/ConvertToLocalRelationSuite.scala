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

package org.apache.spark.sql.catalyst.optimizer

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualNullSafe, EqualTo, Expression,
  GenericInternalRow, IsNull, LessThan, Literal, Not, Or, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.plans.{LeftAnti, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, JoinHint, LeafNode, LocalRelation,
  LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.unsafe.types.UTF8String


class ConvertToLocalRelationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("LocalRelation", FixedPoint(100),
        ConvertToLocalRelation) :: Nil
  }

  test("Project on LocalRelation should be turned into a single LocalRelation") {
    val testRelation = LocalRelation(
      LocalRelation($"a".int, $"b".int).output,
      InternalRow(1, 2) :: InternalRow(4, 5) :: Nil)

    val correctAnswer = LocalRelation(
      LocalRelation($"a1".int, $"b1".int).output,
      InternalRow(1, 3) :: InternalRow(4, 6) :: Nil)

    val projectOnLocal = testRelation.select(
      UnresolvedAttribute("a").as("a1"),
      (UnresolvedAttribute("b") + 1).as("b1"))

    val optimized = Optimize.execute(projectOnLocal.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("Filter on LocalRelation should be turned into a single LocalRelation") {
    val testRelation = LocalRelation(
      LocalRelation($"a".int, $"b".int).output,
      InternalRow(1, 2) :: InternalRow(4, 5) :: Nil)

    val correctAnswer = LocalRelation(
      LocalRelation($"a1".int, $"b1".int).output,
      InternalRow(1, 3) :: Nil)

    val filterAndProjectOnLocal = testRelation
      .select(UnresolvedAttribute("a").as("a1"), (UnresolvedAttribute("b") + 1).as("b1"))
      .where(LessThan(UnresolvedAttribute("b1"), Literal.create(6)))

    val optimized = Optimize.execute(filterAndProjectOnLocal.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-27798: Expression reusing output shouldn't override values in local relation") {
    val testRelation = LocalRelation(
      LocalRelation($"a".int).output,
      InternalRow(1) :: InternalRow(2) :: Nil)

    val correctAnswer = LocalRelation(
      LocalRelation($"a".struct($"a1".int)).output,
      InternalRow(InternalRow(1)) :: InternalRow(InternalRow(2)) :: Nil)

    val projected = testRelation.select(ExprReuseOutput(UnresolvedAttribute("a")).as("a"))
    val optimized = Optimize.execute(projected.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("left anti join with a single-row LocalRelation should be turned into a Filter") {
    val left = NonLocalRelation(Seq($"a".int))
    val right = LocalRelation(Seq($"b".int), Seq(InternalRow(2)))
    val condition = EqualTo(left.output.head, right.output.head)

    val optimized = Optimize.execute(
      Join(left, right, LeftAnti, Some(condition), JoinHint.NONE))
    val correctAnswer = Filter(
      Not(EqualNullSafe(EqualTo(left.output.head, Literal(2)), Literal.TrueLiteral)),
      left)

    comparePlans(optimized, correctAnswer)
  }

  test("left anti join between multi-row LocalRelations should be evaluated locally") {
    val left = LocalRelation(
      Seq($"a".int),
      Seq(InternalRow(1), InternalRow(2), InternalRow(3), InternalRow(4), InternalRow(null)))
    val right = LocalRelation(
      Seq($"b".int),
      Seq(InternalRow(2), InternalRow(4)))
    val condition = EqualTo(left.output.head, right.output.head)

    val optimized = Optimize.execute(
      Join(left, right, LeftAnti, Some(condition), JoinHint.NONE))
    val correctAnswer = LocalRelation(
      left.output,
      Seq(InternalRow(1), InternalRow(3), InternalRow(null)))

    comparePlans(optimized, correctAnswer)
  }

  test("left anti join without a condition should be evaluated locally") {
    val left = LocalRelation(Seq($"a".int), Seq(InternalRow(1), InternalRow(2)))
    val right = LocalRelation(Seq($"b".int), Seq(InternalRow(3)))
    val join = Join(left, right, LeftAnti, None, JoinHint.NONE)

    comparePlans(Optimize.execute(join), LocalRelation(left.output))
  }

  test("left anti join conversion should preserve null semantics") {
    val left = LocalRelation(
      Seq($"a".int),
      Seq(InternalRow(1), InternalRow(2), InternalRow(null)))
    val right = LocalRelation(Seq($"b".int), Seq(InternalRow(2)))
    val condition = EqualTo(left.output.head, right.output.head)

    val optimized = Optimize.execute(
      Join(left, right, LeftAnti, Some(condition), JoinHint.NONE))
    val correctAnswer = LocalRelation(
      left.output,
      Seq(InternalRow(1), InternalRow(null)))

    comparePlans(optimized, correctAnswer)
  }

  test("left anti join conversion should preserve a null value on the right side") {
    val left = LocalRelation(
      Seq($"a".int),
      Seq(InternalRow(1), InternalRow(null)))
    val right = LocalRelation(Seq($"b".int), Seq(InternalRow(null)))
    val condition = EqualTo(left.output.head, right.output.head)

    val optimized = Optimize.execute(
      Join(left, right, LeftAnti, Some(condition), JoinHint.NONE))

    comparePlans(optimized, left)
  }

  test("left anti join conversion should preserve null-aware anti join semantics") {
    val left = LocalRelation(
      Seq($"a".int),
      Seq(InternalRow(1), InternalRow(null)))
    val right = LocalRelation(Seq($"b".int), Seq(InternalRow(null)))
    val equality = EqualTo(left.output.head, right.output.head)
    val nullAwareCondition = Or(equality, IsNull(equality))

    val optimized = Optimize.execute(
      Join(left, right, LeftAnti, Some(nullAwareCondition), JoinHint.NONE))
    val correctAnswer = LocalRelation(left.output, Seq.empty)

    comparePlans(optimized, correctAnswer)
  }

  test("left anti equi join should build and probe a local hash index") {
    val left = LocalRelation(
      Seq($"a".int),
      (0 until 20).map(InternalRow(_)))
    val right = LocalRelation(
      Seq($"b".int),
      (10 until 30).map(InternalRow(_)))
    CountingKey.evaluations.set(0)

    val condition = EqualTo(CountingKey(left.output.head), CountingKey(right.output.head))
    val optimized = Optimize.execute(
      Join(left, right, LeftAnti, Some(condition), JoinHint.NONE))
    val correctAnswer = LocalRelation(
      left.output,
      (0 until 10).map(InternalRow(_)))

    comparePlans(optimized, correctAnswer)
    assert(CountingKey.evaluations.get() == left.data.length + right.data.length)
  }

  test("left anti equi join should hash binary-stable non-integral keys") {
    def row(value: String): InternalRow = InternalRow(UTF8String.fromString(value))

    val left = LocalRelation(
      Seq($"a".string),
      (0 until 10).map(value => row(s"key-$value")))
    val right = LocalRelation(
      Seq($"b".string),
      (5 until 15).map(value => row(s"key-$value")))
    val condition = EqualTo(left.output.head, right.output.head)

    val optimized = Optimize.execute(
      Join(left, right, LeftAnti, Some(condition), JoinHint.NONE))
    val correctAnswer = LocalRelation(
      left.output,
      (0 until 5).map(value => row(s"key-$value")))

    comparePlans(optimized, correctAnswer)
  }

  test("left anti equi join should hash sparse integral keys") {
    val leftValues = Seq(Long.MinValue, 0L, 1L, Long.MaxValue)
    val rightValues = Seq(Long.MinValue, Long.MaxValue) ++ (1L to 20L).map(_ * 1000000L)
    val left = LocalRelation(Seq($"a".long), leftValues.map(InternalRow(_)))
    val right = LocalRelation(Seq($"b".long), rightValues.map(InternalRow(_)))
    val condition = EqualTo(left.output.head, right.output.head)

    val optimized = Optimize.execute(
      Join(left, right, LeftAnti, Some(condition), JoinHint.NONE))
    val correctAnswer = LocalRelation(left.output, Seq(InternalRow(0L), InternalRow(1L)))

    comparePlans(optimized, correctAnswer)
  }

  test("left anti equi join should evaluate residuals within matching hash buckets") {
    val left = LocalRelation(
      Seq($"leftKey".int, $"leftValue".int),
      (0 until 10).map(key => InternalRow(key, 5)))
    val right = LocalRelation(
      Seq($"rightKey".int, $"rightValue".int),
      (0 until 10).map(key => InternalRow(key, key)))
    val condition = And(
      EqualTo(left.output.head, right.output.head),
      LessThan(left.output(1), right.output(1)))

    val optimized = Optimize.execute(
      Join(left, right, LeftAnti, Some(condition), JoinHint.NONE))
    val correctAnswer = LocalRelation(
      left.output,
      (0 to 5).map(key => InternalRow(key, 5)))

    comparePlans(optimized, correctAnswer)
  }

  test("left anti equi join hash keys should normalize NaN and negative zero") {
    val leftNaN = java.lang.Double.longBitsToDouble(0x7ff0000000000001L)
    val rightNaN = java.lang.Double.longBitsToDouble(0x7ff8000000000002L)
    val retainedValues = (1 to 8).map(_.toDouble)
    val left = LocalRelation(
      Seq($"a".double),
      (Seq(0.0, leftNaN) ++ retainedValues).map(InternalRow(_)))
    val right = LocalRelation(
      Seq($"b".double),
      (Seq(-0.0, rightNaN) ++ (101 to 108).map(_.toDouble)).map(InternalRow(_)))
    val condition = EqualTo(left.output.head, right.output.head)

    val optimized = Optimize.execute(
      Join(left, right, LeftAnti, Some(condition), JoinHint.NONE))
    val correctAnswer = LocalRelation(left.output, retainedValues.map(InternalRow(_)))

    comparePlans(optimized, correctAnswer)
  }

  test("left anti equal-null-safe join should hash null keys") {
    val left = LocalRelation(
      Seq($"a".int),
      Seq(InternalRow(null)) ++ (0 until 9).map(InternalRow(_)))
    val right = LocalRelation(
      Seq($"b".int),
      Seq(InternalRow(null), InternalRow(0)) ++ (100 until 107).map(InternalRow(_)))
    val condition = EqualNullSafe(left.output.head, right.output.head)

    val optimized = Optimize.execute(
      Join(left, right, LeftAnti, Some(condition), JoinHint.NONE))
    val correctAnswer = LocalRelation(
      left.output,
      (1 until 9).map(InternalRow(_)))

    comparePlans(optimized, correctAnswer)
  }

  test("left anti join with a multi-row LocalRelation should not be converted") {
    val left = NonLocalRelation(Seq($"a".int))
    val right = LocalRelation(
      Seq($"b".int),
      Seq(InternalRow(1), InternalRow(2)))
    val join = Join(
      left,
      right,
      LeftAnti,
      Some(EqualTo(left.output.head, right.output.head)),
      JoinHint.NONE)

    comparePlans(Optimize.execute(join), join)
  }
}

case class NonLocalRelation(output: Seq[Attribute]) extends LeafNode


object CountingKey {
  val evaluations = new AtomicInteger()
}


case class CountingKey(child: Expression) extends UnaryExpression with CodegenFallback {
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable

  override protected def nullSafeEval(input: Any): Any = {
    CountingKey.evaluations.incrementAndGet()
    input
  }

  override protected def withNewChildInternal(newChild: Expression): CountingKey = copy(newChild)
}


// Dummy expression used for testing. It reuses output row. Assumes child expr outputs an integer.
case class ExprReuseOutput(child: Expression) extends UnaryExpression {
  override def dataType: DataType = StructType.fromDDL("a1 int")
  override def nullable: Boolean = true

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException("Should not trigger codegen")

  private val row: InternalRow = new GenericInternalRow(1)

  override def eval(input: InternalRow): Any = {
    row.update(0, child.eval(input))
    row
  }

  override protected def withNewChildInternal(newChild: Expression): ExprReuseOutput =
    copy(child = newChild)
}
