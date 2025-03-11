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
package org.apache.gluten.jni;

import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Bridge class to access a native iterator from Java.
 * This delegates to native methods to access the actual C++ iterator.
 */
public class NativeIteratorBridge implements Iterator<ColumnarBatch>, AutoCloseable {
  private final long nativeIteratorId;
  private boolean closed = false;

  /**
   * Constructor.
   *
   * @param nativeIteratorId ID of the native iterator
   */
  public NativeIteratorBridge(long nativeIteratorId) {
    this.nativeIteratorId = nativeIteratorId;
  }

  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    }
    return nativeHasNext(nativeIteratorId);
  }

  @Override
  public ColumnarBatch next() {
    if (closed || !hasNext()) {
      throw new NoSuchElementException("No more elements in the iterator");
    }
    return nativeNext(nativeIteratorId);
  }

  @Override
  public void close() {
    if (!closed) {
      nativeClose(nativeIteratorId);
      closed = true;
    }
  }

  // Native methods
  private native boolean nativeHasNext(long nativeIteratorId);
  private native ColumnarBatch nativeNext(long nativeIteratorId);
  private native void nativeClose(long nativeIteratorId);
}
