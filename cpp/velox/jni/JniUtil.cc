/**
 * Helper class to hold a C++ iterator for access from Java.
 */
class NativeIteratorHolder {
    public:
      NativeIteratorHolder(std::unique_ptr<gluten::VeloxColumnarBatchIterator> iterator)
          : iterator_(std::move(iterator)) {}
    
      gluten::VeloxColumnarBatchIterator* iterator() {
        return iterator_.get();
      }
    
    private:
      std::unique_ptr<gluten::VeloxColumnarBatchIterator> iterator_;
    };
    
    /**
     * Map to store native iterators with a mutex for thread safety.
     */
    static std::unordered_map<jlong, std::shared_ptr<NativeIteratorHolder>> nativeIterators;
    static std::mutex nativeIteratorsMutex;
    
    /**
     * Register a native iterator and get its ID.
     */
    static jlong registerNativeIterator(std::unique_ptr<gluten::VeloxColumnarBatchIterator> iterator) {
      std::lock_guard<std::mutex> lock(nativeIteratorsMutex);
      auto holder = std::make_shared<NativeIteratorHolder>(std::move(iterator));
      jlong id = reinterpret_cast<jlong>(holder.get());
      nativeIterators[id] = holder;
      return id;
    }
    
    /**
     * Get a native iterator by ID.
     */
    static NativeIteratorHolder* getNativeIterator(jlong id) {
      std::lock_guard<std::mutex> lock(nativeIteratorsMutex);
      auto it = nativeIterators.find(id);
      if (it == nativeIterators.end()) {
        return nullptr;
      }
      return it->second.get();
    }
    
    /**
     * Remove a native iterator.
     */
    static void removeNativeIterator(jlong id) {
      std::lock_guard<std::mutex> lock(nativeIteratorsMutex);
      nativeIterators.erase(id);
    }
    
    /**
     * Create a Java Iterator<ColumnarBatch> that delegates to a C++ iterator.
     */
    jobject createJavaIteratorFromNative(
        JNIEnv* env,
        std::unique_ptr<gluten::VeloxColumnarBatchIterator> iterator) {
      // Register the native iterator
      jlong iteratorId = registerNativeIterator(std::move(iterator));
    
      // Find the NativeIteratorBridge class
      jclass bridgeClass = env->FindClass("org/apache/gluten/jni/NativeIteratorBridge");
      if (bridgeClass == nullptr) {
        throwJavaException(env, "Could not find NativeIteratorBridge class");
        return nullptr;
      }
    
      // Find the constructor
      jmethodID constructor = env->GetMethodID(bridgeClass, "<init>", "(J)V");
      if (constructor == nullptr) {
        throwJavaException(env, "Could not find NativeIteratorBridge constructor");
        return nullptr;
      }
    
      // Create a new instance
      return env->NewObject(bridgeClass, constructor, iteratorId);
    }
    