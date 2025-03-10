#include "VeloxBroadcastHashTable.h"

#include "velox/common/base/Exceptions.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/HashTable.h"
#include "velox/vector/FlatVector.h"

namespace gluten {

VeloxBroadcastHashTable::VeloxBroadcastHashTable(int64_t tableId)
    : tableId_(tableId),
      pool_(facebook::velox::memory::getProcessDefaultMemoryManager().getRoot()->addLeafPool("broadcast_hash_table")) {
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

        if (rowType_) {
            // Assuming the first column is the key for simplicity. Adapt as needed.
            std::vector<facebook::velox::core::FieldVector> keys = {rowType_->childAt(0)};
            hashBuild_ = std::make_unique<facebook::velox::exec::HashBuild>(keys, std::vector<facebook::velox::core::FieldVector>(), false, pool_.get());
        }
    }

    if (hashBuild_) {
        hashBuild_->addInput(rowVector);
    }

    tableData_.push_back(rowVector);
}

std::unique_ptr<VeloxColumnarBatchIterator> VeloxBroadcastHashTable::createIterator() {
   return std::make_unique<VeloxColumnarBatchIteratorImpl>(tableData_);
}

// BroadcastHashTableRegistry implementation
BroadcastHashTableRegistry& BroadcastHashTableRegistry::instance() {
  static BroadcastHashTableRegistry instance;
  return instance;
}

void BroadcastHashTableRegistry::registerTable(std::shared_ptr<VeloxBroadcastHashTable> table) {
  std::lock_guard<std::mutex> lock(mutex_);
  tables_[reinterpret_cast<int64_t>(table.get())] = table;
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

VeloxColumnarBatchIteratorImpl::VeloxColumnarBatchIteratorImpl(const std::vector<facebook::velox::RowVectorPtr>& tableData)
    : tableData_(tableData), currentIndex_(0) {}

bool VeloxColumnarBatchIteratorImpl::hasNext() const {
    return currentIndex_ < tableData_.size();
}

VeloxColumnarBatch VeloxColumnarBatchIteratorImpl::next() {
    if (!hasNext()) {
        throw std::out_of_range("Iterator out of range");
    }
    return VeloxColumnarBatch(tableData_[currentIndex_++]);
}

} // namespace gluten