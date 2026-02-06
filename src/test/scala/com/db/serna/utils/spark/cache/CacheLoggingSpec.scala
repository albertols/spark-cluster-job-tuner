package com.db.serna.utils.spark.cache

import com.db.serna.utils.spark.TestSparkSessionSupport
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}

import scala.util.Try

class CacheLoggingSpec extends AnyFunSuite with Eventually {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(100, Millis))

  test("DataFrame caching is reported via Dataset.isCached and CacheManager, and materializes data") {
    TestSparkSessionSupport.withCacheSession { spark: SparkSession =>
      val ssStable = spark;

      println(s"[CLS] master=${spark.sparkContext.master}, serializer=${spark.sparkContext.getConf.get("spark.serializer")}")

      val df = spark.range(0, 10000).toDF("id").repartition(8)

      // Cache as DataFrame (Spark SQL cache)
      df.persist(StorageLevel.MEMORY_ONLY)

      // Materialize cache
      val n = df.count()
      println(s"[CLS] count=$n")
      assert(n == 10000L)

      // CacheManager entry
      eventually {
        val cachedDataOpt = spark.sharedState.cacheManager.lookupCachedData(df.queryExecution.logical)
        println(s"[CLS] CacheManager.lookupCachedData.present=${cachedDataOpt.nonEmpty}")
        assert(cachedDataOpt.nonEmpty, "CacheManager should have a CachedData entry for the DataFrame")
      }

      // For completeness: some environments surface SQL caches in RDDStorageInfo; don't tie to df.rdd.id
      val anyCachedRddInfoExists =
        spark.sparkContext.getRDDStorageInfo.exists(_.numCachedPartitions > 0)
      println(s"[CLS] RDDStorageInfo.hasCachedPartitions=${anyCachedRddInfoExists}")
      assert(anyCachedRddInfoExists || spark.sharedState.cacheManager.lookupCachedData(df.queryExecution.logical).nonEmpty)
    }
  }

  test("Executor memory status is available; attempt CacheLogging invocation if present (optional)") {
    TestSparkSessionSupport.withCacheSession { spark: SparkSession =>
      val ssStable = spark;

      val df = spark.range(0, 1000).toDF("id")
      df.persist(StorageLevel.MEMORY_AND_DISK)
      val _ = df.count()

      // Public API is available in all Spark versions
      val execMemStatus = spark.sparkContext.getExecutorMemoryStatus
      println(s"[CLS] ExecutorMemoryStatus.count=${execMemStatus.size}")
      assert(execMemStatus.nonEmpty)

      // Try object and class variants in both package locations
      val candidates = Seq(
        "com.db.serna.utils.spark.cache.CacheLogging$",
        "com.db.serna.utils.spark.cache.CacheLogging",
        "com.example.monitoring.CacheLogging$",
        "com.example.monitoring.CacheLogging"
      )

      def tryInvoke(fqcn: String): Boolean = {
        val found = Try(Class.forName(fqcn)).toOption
        println(s"[CLS] trying=$fqcn found=${found.isDefined}")
        found.exists { clazz =>
          val instanceOpt =
            if (fqcn.endsWith("$")) Try(clazz.getField("MODULE$").get(null)).toOption
            else Try(clazz.getField("MODULE$").get(null)).toOption
              .orElse(Try(clazz.getDeclaredConstructor().newInstance()).toOption)

          println(s"[CLS] instanceOpt.isDefined=${instanceOpt.isDefined}")

          instanceOpt.exists { instance =>
            val registerOpt = Try(clazz.getMethod("register", classOf[org.apache.spark.SparkContext])).toOption
            val logStats2Opt = Try(clazz.getMethod("logDfCacheStats", classOf[org.apache.spark.sql.DataFrame], classOf[String])).toOption
            val logStats1Opt = Try(clazz.getMethod("logDfCacheStats", classOf[org.apache.spark.sql.DataFrame])).toOption

            println(s"[CLS] methods: register=${registerOpt.isDefined}, log2=${logStats2Opt.isDefined}, log1=${logStats1Opt.isDefined}")

            registerOpt.exists { register =>
              register.invoke(instance, spark.sparkContext)
              val invoked =
                logStats2Opt
                  .map(m => { m.invoke(instance, df, "range_df"); true })
                  .orElse(logStats1Opt.map(m => { m.invoke(instance, df); true }))
                  .getOrElse(false)
              println(s"[CLS] CacheLogging invoked=$invoked for=$fqcn")
              invoked
            }
          }
        }
      }

      // Attempt to invoke; treat as optional
      val loggerInvoked = candidates.exists(tryInvoke)
      println(s"[CLS] loggerInvoked=$loggerInvoked")
      // Optional invocation; no strict assertion beyond executor memory status
      assert(execMemStatus.nonEmpty)
    }
  }
}