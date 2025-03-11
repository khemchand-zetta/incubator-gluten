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
#pragma once

#include <memory>
#include <unordered_map>
#include <mutex>

#include "velox/exec/HashTable.h"
#include "velox/vector/VectorStream.h"
#include "VeloxColumnarBatch.h"

namespace gluten {

// Forward declarations
class VeloxColumnarBatchIterator;

/**
 * A class to store a Velox hash table for broadcast joins.
 * This is used for sharing hash tables between tasks on the same executor.
 */
class VeloxBroadcastHashTable {
public:
  /**
   * Constructor.
   *
   * @param tableId unique identifier for the hash table
   */
  VeloxBroadcastHashTable(int64_t tableId);

  /**
   * Destructor.
   */
  ~VeloxBroadcastHashTable();

  /**
   * Add a batch to the hash table.
   *
   * @param batch the columnar batch to add
   */
  void addBatch(VeloxColumnarBatch& batch);

  /**
   * Create an iterator that returns batches from this table.
   *
   * @return iterator over the hash table contents
   */
  std::unique_ptr<VeloxColumnarBatchIterator> createIterator();

  /**
   * Get the table ID.
   *
   * @return the table ID
   */
  int64_t tableId() const { return tableId_; }

private:
  int64_t tableId_;
  std::shared_ptr<facebook::velox::core::JoinBridge> joinBridge_;
  std::mutex mutex_;

  // Store the schema and data in a format suitable for iteration
  facebook::velox::memory::MemoryPool* pool_;
  std::vector<facebook::velox::RowVectorPtr> tableData_;
  facebook::velox::RowTypePtr rowType_;
};

/**
 * Global registry to store all broadcast hash tables.
 * This ensures we can lookup tables by pointer in JNI calls.
 */
class BroadcastHashTableRegistry {
public:
  /**
   * Get the singleton instance.
   *
   * @return the registry instance
   */
  static BroadcastHashTableRegistry& instance();

  /**
   * Register a new hash table.
   *
   * @param table the hash table to register
   * @return native pointer (cast of the shared_ptr address)
   */
  int64_t registerTable(std::shared_ptr<VeloxBroadcastHashTable> table);

  /**
   * Get an existing hash table by pointer.
   *
   * @param nativePtr pointer to the hash table
   * @return the hash table or nullptr if not found
   */
  std::shared_ptr<VeloxBroadcastHashTable> getTable(int64_t nativePtr);

  /**
   * Remove a hash table.
   *
   * @param nativePtr pointer to the hash table
   */
  void removeTable(int64_t nativePtr);

private:
  BroadcastHashTableRegistry() = default;

  std::mutex mutex_;
  std::unordered_map<int64_t, std::shared_ptr<VeloxBroadcastHashTable>> tables_;
};

} // namespace gluten
