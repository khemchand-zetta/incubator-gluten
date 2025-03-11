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
package org.apache.gluten.execution

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import org.apache.gluten.GlutenConfig
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._

/**
 * Cache for Velox broadcast join hash tables.
 * This cache stores native hash tables to avoid redundant hash table construction
 * for broadcast joins when multiple tasks run on the same executor.
 */
object VeloxBroadcastBuildSideCache {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  // Cache for storing build hash tables
  private val cache: Cache[Long, VeloxBuildSideTable] = Caffeine.newBuilder()
    .expireAfterAccess(
      GlutenConfig.getConf.getConfString(
        "spark.gluten.sql.columnar.velox.broadcast.expire.seconds",
        "3600").toInt,
      TimeUnit.SECONDS)
    .removalListener((key: Long, value: VeloxBuildSideTable, cause) => {
      if (value != null) {
        logger.info(s"Removing hash table for broadcast ID $key, cause: $cause")
        value.close()
      }
    })
    .build()

  private val tableIdGenerator = new AtomicLong(0)

  // Get or build a hash table for the broadcast relation
  def getOrBuildHashTable(buildSideRelation: BuildSideRelation): VeloxBuildSideTable = {
    if (!GlutenConfig.getConf.getConfBoolean(
        "spark.gluten.sql.columnar.velox.broadcastJoin.hashTableSharing", true)) {
      // If sharing is disabled, always build a new hash table
      return buildHashTable(buildSideRelation)
    }

    val broadcastId = buildSideRelation.relationHashCode()
    val result = cache.getIfPresent(broadcastId)
    if (result != null) {
      logger.debug(s"Cache hit for broadcast ID $broadcastId")
      result.incrementReferenceCount()
      result
    } else {
      logger.debug(s"Cache miss for broadcast ID $broadcastId, building new hash table")
      val newTable = buildHashTable(buildSideRelation)
      cache.put(broadcastId, newTable)
      newTable
    }
  }

  // Build a hash table from a broadcast relation
  private def buildHashTable(buildSideRelation: BuildSideRelation): VeloxBuildSideTable = {
    val tableId = tableIdGenerator.getAndIncrement()
    val relation = buildSideRelation.asReadOnlyCopy()

    // Create C++ native hash table through JNI
    val nativePtr = VeloxJniWrapper.createBroadcastHashTable(tableId)

    // Populate the hash table with the relation data
    val batches = relation.deserialized
    try {
      while (batches.hasNext) {
        val batch = batches.next()
        VeloxJniWrapper.addBatchToBroadcastHashTable(nativePtr, batch)
      }
      new VeloxBuildSideTable(tableId, nativePtr)
    } finally {
      batches.asInstanceOf[AutoCloseable].close()
    }
  }

  // Get cache stats for monitoring
  def getCacheStats(): Map[String, Long] = {
    Map(
      "estimatedSize" -> cache.estimatedSize(),
      "hitCount" -> cache.stats().hitCount(),
      "missCount" -> cache.stats().missCount()
    )
  }

  // Remove a hash table from the cache
  def invalidate(broadcastId: Long): Unit = {
    val table = cache.getIfPresent(broadcastId)
    if (table != null) {
      logger.debug(s"Explicitly invalidating hash table for broadcast ID $broadcastId")
      cache.invalidate(broadcastId)
    }
  }

  // Clear all entries from the cache
  def clear(): Unit = {
    logger.info("Clearing all broadcast hash tables from cache")
    cache.invalidateAll()
  }
}

/**
 * Represents a Velox build side hash table with reference counting.
 */
class VeloxBuildSideTable(val tableId: Long, private val nativePtr: Long) {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  private val referenceCount = new AtomicLong(1)

  def incrementReferenceCount(): Long = {
    val newCount = referenceCount.incrementAndGet()
    logger.debug(s"Incremented reference count to $newCount for hash table $tableId")
    newCount
  }

  def decrementReferenceCount(): Long = {
    val count = referenceCount.decrementAndGet()
    logger.debug(s"Decremented reference count to $count for hash table $tableId")
    if (count == 0) {
      close()
    }
    count
  }

  def iterator: Iterator[ColumnarBatch] = {
    VeloxJniWrapper.getBroadcastHashTableIterator(nativePtr)
  }

  def close(): Unit = {
    if (nativePtr != 0) {
      logger.debug(s"Releasing native resources for hash table $tableId")
      VeloxJniWrapper.releaseBroadcastHashTable(nativePtr)
    }
  }
}
