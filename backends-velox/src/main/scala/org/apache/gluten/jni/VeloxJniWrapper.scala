/**
 * Create a new broadcast hash table in native memory.
 *
 * @param tableId unique identifier for the hash table
 * @return pointer to the native hash table
 */
@native def createBroadcastHashTable(tableId: Long): Long

/**
 * Add a batch to the existing hash table.
 *
 * @param nativePtr pointer to the native hash table
 * @param batch columnar batch to add to the hash table
 */
@native def addBatchToBroadcastHashTable(nativePtr: Long, batch: ColumnarBatch): Unit

/**
 * Get an iterator over the hash table contents.
 *
 * @param nativePtr pointer to the native hash table
 * @return iterator of columnar batches
 */
@native def getBroadcastHashTableIterator(nativePtr: Long): Iterator[ColumnarBatch]

/**
 * Release resources associated with the hash table.
 *
 * @param nativePtr pointer to the native hash table
 */
@native def releaseBroadcastHashTable(nativePtr: Long): Unit