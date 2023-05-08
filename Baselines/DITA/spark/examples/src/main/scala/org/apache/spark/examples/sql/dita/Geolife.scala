
package org.apache.spark.examples.sql.dita

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.dita.TrajectorySimilarityFunction
import org.apache.spark.sql.catalyst.expressions.dita.common.shape.Point
import org.apache.spark.sql.catalyst.expressions.dita.common.trajectory.Trajectory

object Geolife {
  case class TrajectoryRecord(id: Long, traj: Array[Array[Double]])

  private def getTrajectory(line: (String, Long)): TrajectoryRecord = {
    val points = line._1.split(";").map(_.split(","))
      .map(x => x.map(_.toDouble))
    TrajectoryRecord(line._2, points)
  }

  private def splitTrajectory(t: TrajectoryRecord, from: Int, until: Int): TrajectoryRecord = {
    TrajectoryRecord(t.id, t.traj.slice(from, until))
  }

  def main(args: Array[String]): Unit = {
    bench(K = 10, windowSize = Integer.parseInt(args(0)), Integer.parseInt(args(1)), Integer.parseInt(args(2)))
  }

  def bench(K: Int, windowSize: Int, i: Int, step: Int): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    import spark.implicits._

    val trajs = spark.sparkContext
      .textFile(
        "/home/liontao/work/stream-distance/notebooks/preprocess/geolife-dita-long-small.txt")
      .zipWithIndex().map(getTrajectory)
      //      .filter(_.traj.length >= DITAConfigConstants.TRAJECTORY_MIN_LENGTH)
      .map(splitTrajectory(_, i * 4 * step, (i + windowSize) * 4 * step))
    val df1 = trajs.toDF()
    df1.createOrReplaceTempView("traj1")
    df1.createTrieIndex(df1("traj"), "traj_index1")

    val queryTrajectory = Trajectory(trajs.filter(t => t.id == 1).take(1).head.traj.map(Point))
    df1.trajectorySimilarityWithKNNSearch(queryTrajectory, df1("traj"),
      TrajectorySimilarityFunction.DTW, K).show()
    spark.stop()
  }
}
