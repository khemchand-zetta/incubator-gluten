package org.apache.gluten.execution

import org.apache.spark.sql.vectorized.ColumnarBatch

object VeloxJniWrapper {
  // Load native library
  System.loadLibrary("veloxjni")

  // Create a new broadcast hash table
  @native def createBroadcastHashTable(tableId: Long): Long

  // Add a batch to the hash table
  @native def addBatchToBroadcastHashTable(nativePtr: Long, batch: ColumnarBatch): Unit

  // Get an iterator over the hash table
  @native def getBroadcastHashTableIterator(nativePtr: Long): Iterator[ColumnarBatch]

  // Release the hash table
  @native def releaseBroadcastHashTable(nativePtr: Long): Unit
}
