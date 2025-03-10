// velox/jni/VeloxBroadcastHashTable.h
#pragma once

#include <memory>
#include <unordered_map>
#include <mutex>

#include "velox/exec/HashTable.h"
#include "velox/vector/VectorStream.h"
#include "VeloxColumnarBatch.h"

namespace gluten {

class VeloxBroadcastHashTable {
public:
  VeloxBroadcastHashTable(int64_t tableId);
  ~VeloxBroadcastHashTable();

  // Add a batch to the hash table
  void addBatch(VeloxColumnarBatch& batch);

  // Create an iterator that returns batches from this table
  std::unique_ptr<VeloxColumnarBatchIterator> createIterator();

  // Get the table ID
  int64_t tableId() const { return tableId_; }

private:
  int64_t tableId_;
  std::unique_ptr<facebook::velox::exec::HashTable> hashTable_;
  std::mutex mutex_;
  std:: static int currentIndex_

  // Store the schema and data in a format suitable for iteration
  facebook::velox::memory::MemoryPool* pool_;
  std::vector<facebook::velox::RowVectorPtr> tableData_;
  facebook::velox::RowTypePtr rowType_;
};

// Global registry to store all broadcast hash tables
class BroadcastHashTableRegistry {
public:
  static BroadcastHashTableRegistry& instance();

  // Register a new hash table
  void registerTable(std::shared_ptr<VeloxBroadcastHashTable> table);

  // Get an existing hash table by pointer
  std::shared_ptr<VeloxBroadcastHashTable> getTable(int64_t nativePtr);

  // Remove a hash table
  void removeTable(int64_t nativePtr);

private:
  BroadcastHashTableRegistry() = default;

  std::mutex mutex_;
  std::unordered_map<int64_t, std::shared_ptr<VeloxBroadcastHashTable>> tables_;
};

} // namespace gluten
