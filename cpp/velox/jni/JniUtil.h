/**
 * Create a Java Iterator<ColumnarBatch> from a C++ iterator.
 *
 * @param env JNI environment
 * @param iterator C++ iterator to convert
 * @return Java iterator
 */
jobject createJavaIteratorFromNative(
    JNIEnv* env,
    std::unique_ptr<gluten::VeloxColumnarBatchIterator> iterator);
