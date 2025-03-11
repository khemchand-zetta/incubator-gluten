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
#include <vector>
#include "velox/vector/VectorStream.h"

namespace gluten {

/**
 * An iterator over columnar batches stored in a broadcast hash table.
 */
class VeloxColumnarBatchIterator {
public:
  /**
   * Constructor.
   *
   * @param vectors the vectors to iterate over
   */
  explicit VeloxColumnarBatchIterator(std::vector<facebook::velox::VectorPtr> vectors);

  /**
   * Check if there are more batches.
   *
   * @return true if there are more batches
   */
  bool hasNext();

  /**
   * Get the next batch.
   *
   * @return the next batch
   */
  facebook::velox::VectorPtr next();

private:
  std::vector<facebook::velox::VectorPtr> vectors_;
  size_t currentIndex_ = 0;
};

} // namespace gluten
