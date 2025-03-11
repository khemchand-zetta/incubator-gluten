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

#include "VeloxColumnarBatchIterator.h"

 namespace gluten {
 
 VeloxColumnarBatchIterator::VeloxColumnarBatchIterator(
     std::vector<facebook::velox::VectorPtr> vectors)
     : vectors_(std::move(vectors)), currentIndex_(0) {}
 
 bool VeloxColumnarBatchIterator::hasNext() {
   return currentIndex_ < vectors_.size();
 }
 
 facebook::velox::VectorPtr VeloxColumnarBatchIterator::next() {
   if (!hasNext()) {
     throw std::runtime_error("No more elements in the iterator");
   }
   return vectors_[currentIndex_++];
 }
 
 } // namespace gluten
 