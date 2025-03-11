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

 #include "VeloxBroadcastHashTable.h"

 #include "velox/common/base/Exceptions.h"
 #include "velox/exec/HashBuild.h"
 #include "velox/exec/JoinBridge.h"
 #include "VeloxColumnarBatch.h"
 #include "VeloxColumnarBatchIterator.h"
 
 namespace gluten {
 
 VeloxBroadcastHashTable::VeloxBroadcastHashTable(int64_t tableId)
     : tableId_(tableId),
       joinBridge_(std::make_shared<facebook::velox::core::JoinBridge>()),
       pool_(facebook::velox::memory::getProcessDefaultMemoryManager()
                 .getRoot()
                 ->addLeafPool("broadcast_hash_table")) {
   // Initialize the hash table
 }
 
 VeloxBroadcastHashTable::~VeloxBroadcastHashTable() {
   // Release all resources
   if (pool_) {
     facebook::velox::memory::getProcessDefaultMemoryManager().getRoot()->removeLeafPool(pool_);
   }
 }
 
 void VeloxBroadcastHashTable::addBatch(VeloxColumnarBatch& batch) {
   std::lock_guard<std::mutex> lock(mutex_);
 
   auto rowVector = batch.toRowVector();
 
   if (!rowType_) {
     // First batch, initialize the schema and hash table
     rowType_ = std::dynamic_pointer_cast<const facebook::velox::RowType>(rowVector->type());
   }
 
   // Store for iteration
   tableData_.push_back(rowVector);
 }
 
 std::unique_ptr<VeloxColumnarBatchIterator> VeloxBroadcastHashTable::createIterator() {
   // Create an iterator that can return batches from tableData_
   std::vector<facebook::velox::VectorPtr> vectors;
   for (const auto& rowVector : tableData_) {
     vectors.push_back(rowVector);
   }
 
   // Create and return a new iterator over the stored vectors
   return std::make_unique<VeloxColumnarBatchIterator>(vectors);
 }
 
 // BroadcastHashTableRegistry implementation
 BroadcastHashTableRegistry& BroadcastHashTableRegistry::instance() {
   static BroadcastHashTableRegistry instance;
   return instance;
 }
 
 int64_t BroadcastHashTableRegistry::registerTable(std::shared_ptr<VeloxBroadcastHashTable> table) {
   std::lock_guard<std::mutex> lock(mutex_);
   int64_t ptr = reinterpret_cast<int64_t>(table.get());
   tables_[ptr] = table;
   return ptr;
 }
 
 std::shared_ptr<VeloxBroadcastHashTable> BroadcastHashTableRegistry::getTable(int64_t nativePtr) {
   std::lock_guard<std::mutex> lock(mutex_);
   auto it = tables_.find(nativePtr);
   if (it == tables_.end()) {
     return nullptr;
   }
   return it->second;
 }
 
 void BroadcastHashTableRegistry::removeTable(int64_t nativePtr) {
   std::lock_guard<std::mutex> lock(mutex_);
   tables_.erase(nativePtr);
 }
 
 } // namespace gluten
 