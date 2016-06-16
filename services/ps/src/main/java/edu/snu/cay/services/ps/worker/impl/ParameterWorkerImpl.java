/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.services.ps.worker.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.snu.cay.services.ps.PSParameters.KeyCodecName;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import edu.snu.cay.services.ps.worker.parameters.*;
import edu.snu.cay.services.ps.common.resolver.ServerResolver;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Parameter Server worker that interacts with servers.
 * A single instance of this class can be used by more than one thread safely, if and only if
 * the Codec classes are thread-safe.
 *
 * There are a few client-side optimizations that can be configured.
 * A serialized and hashed representation of a key is cached, avoiding these costs.
 * See {@link WorkerKeyCacheSize}.
 * The remaining configurations are related to the worker-side threads.
 * See {@link WorkerThread}.
 */
@EvaluatorSide
public final class ParameterWorkerImpl<K, P, V> implements ParameterWorker<K, P, V> {
  private static final Logger LOG = Logger.getLogger(ParameterWorkerImpl.class.getName());

  /**
   * Object for processing preValues and applying updates to existing values.
   */
  private final ParameterUpdater<K, P, V> parameterUpdater;

  /**
   * Resolve to a server's Network Connection Service identifier based on hashed key.
   */
  private final ServerResolver serverResolver;

  /**
   * A map of pending pulls, used to reconcile the asynchronous messaging with the synchronous CacheLoader call.
   */
  private final ConcurrentMap<K, PullFuture<V>> pendingPulls;

  /**
   * Number of threads.
   */
  private final int numThreads;

  /**
   * Max size of each partition's queue.
   */
  private final int queueSize;

  /**
   * Duration in ms to keep local entries cached, after which the entries are expired.
   */
  private final long expireTimeout;

  /**
   * Thread pool, where each thread is submitted.
   */
  private final ExecutorService threadPool;

  /**
   * Running threads.
   */
  private final WorkerThread<K, P, V>[] threads;

  /**
   * A cache that stores encoded (serialized) keys and hashes.
   */
  private final LoadingCache<K, EncodedKey<K>> encodedKeyCache;

  /**
   * Send messages to the server using this field.
   * Without {@link InjectionFuture}, this class creates an injection loop with
   * classes related to Network Connection Service and makes the job crash (detected by Tang).
   */
  private final InjectionFuture<WorkerMsgSender<K, P>> sender;

  @Inject
  private ParameterWorkerImpl(@Parameter(ParameterWorkerNumThreads.class) final int numThreads,
                              @Parameter(WorkerQueueSize.class) final int queueSize,
                              @Parameter(WorkerExpireTimeout.class) final long expireTimeout,
                              @Parameter(WorkerKeyCacheSize.class) final int keyCacheSize,
                              @Parameter(KeyCodecName.class) final Codec<K> keyCodec,
                              final ParameterUpdater<K, P, V> parameterUpdater,
                              final ServerResolver serverResolver,
                              final InjectionFuture<WorkerMsgSender<K, P>> sender) {
    this.numThreads = numThreads;
    this.queueSize = queueSize;
    this.expireTimeout = expireTimeout;
    this.parameterUpdater = parameterUpdater;
    this.serverResolver = serverResolver;
    this.sender = sender;
    this.pendingPulls = new ConcurrentHashMap<>();
    this.threadPool = Executors.newFixedThreadPool(numThreads);
    this.threads = initThreads();
    this.encodedKeyCache = CacheBuilder.newBuilder()
        .maximumSize(keyCacheSize)
        .build(new CacheLoader<K, EncodedKey<K>>() {
          @Override
          public EncodedKey<K> load(final K key) throws Exception {
            return new EncodedKey<>(key, keyCodec);
          }
        });
  }

  /**
   * Call after initializing threadPool.
   */
  @SuppressWarnings("unchecked")
  private WorkerThread<K, P, V>[] initThreads() {
    LOG.log(Level.INFO, "Initializing {0} threads", numThreads);
    final WorkerThread<K, P, V>[] initialized = new WorkerThread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      initialized[i] = new WorkerThread<>(pendingPulls, serverResolver, sender, queueSize, expireTimeout);
      threadPool.submit(initialized[i]);
    }
    return initialized;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void push(final K key, final P preValue) {
    try {
      push(encodedKeyCache.get(key), preValue);
    } catch (final ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void push(final EncodedKey<K> encodedKey, final P preValue) {
    final int partitionId = getPartitionIndex(encodedKey.getHash());
    final int threadId = partitionId % numThreads;
    threads[threadId].enqueue(new PushOp(encodedKey, preValue));
  }

  @Override
  public V pull(final K key) {
    try {
      return pull(encodedKeyCache.get(key));
    } catch (final ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public V pull(final EncodedKey<K> encodedKey) {
    final PullOp pullOp = new PullOp(encodedKey);
    final int partitionId = getPartitionIndex(encodedKey.getHash());
    final int threadId = partitionId % numThreads;
    threads[threadId].enqueue(pullOp);
    return pullOp.get();
  }

  @Override
  public List<V> pull(final List<K> keys) {
    // transform keys to encoded keys
    final List<EncodedKey<K>> encodedKeys = new ArrayList<>(keys.size());
    for (final K key : keys) {
      try {
        encodedKeys.add(encodedKeyCache.get(key));
      } catch (final ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    return pullEncodedKeys(encodedKeys);
  }

  public List<V> pullEncodedKeys(final List<EncodedKey<K>> encodedKeys) {
    final List<PullOp> pullOps = new ArrayList<>(encodedKeys.size());
    for (final EncodedKey<K> encodedKey : encodedKeys) {
      final PullOp pullOp = new PullOp(encodedKey);
      pullOps.add(pullOp);
      final int partitionId = getPartitionIndex(encodedKey.getHash());
      final int threadId = partitionId % numThreads;
      threads[threadId].enqueue(pullOp);
    }
    final List<V> values = new ArrayList<>(pullOps.size());
    for (final PullOp pullOp : pullOps) {
      values.add(pullOp.get());
    }
    return values;
  }

  public void invalidateAll() {
    for (int i = 0; i < numThreads; i++) {
      threads[i].invalidateAll();
    }
  }

  private int getPartitionIndex(final int keyHash) {
    return keyHash % numThreads;
  }

  /**
   * Close the worker, after waiting a maximum of {@code timeoutMs} milliseconds
   * for queued messages to be sent.
   */
  @Override
  public void close(final long timeoutMs) throws InterruptedException, TimeoutException, ExecutionException {

    final Future result = Executors.newSingleThreadExecutor().submit(new Runnable() {
      @Override
      public void run() {
        // Close all threads
        for (int i = 0; i < numThreads; i++) {
          threads[i].close();
        }
        // Wait for shutdown to complete on all threads
        for (int i = 0; i < numThreads; i++) {
          threads[i].waitForShutdown();
        }
      }
    });

    result.get(timeoutMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Handles incoming pull replies, by setting the value of the future.
   * This will notify the Partition's (synchronous) CacheLoader method to continue.
   * Called by {@link AsyncWorkerHandlerImpl#processReply}.
   */
  public void processReply(final K key, final V value) {
    final PullFuture<V> future = pendingPulls.get(key);
    if (future != null) {
      future.setValue(value);
    } else {
      // Because we use partitions, there can be at most one active pendingPull for a key.
      // Thus, a null value should never appear.
      throw new RuntimeException(String.format("Pending pull was not found for key %s", key));
    }
  }

  /**
   * A simple Future that will wait on a get, until a value is set.
   * We do not implement a true Future, because this is simpler.
   */
  private static final class PullFuture<V> {
    private V value = null;

    /**
     * Block until a value is set or the maximum waiting time elapses.
     * It returns null when it fails to get the value in given timeout.
     * @param timeout the maximum time to wait in milliseconds
     * @return the value
     */
    public synchronized V getValue(final long timeout) {
      try {
        wait(timeout);
      } catch (final InterruptedException e) {
        LOG.log(Level.WARNING, "InterruptedException on wait", e);
      }
      return value;
    }

    /**
     * Set the value and unblock all waiting gets.
     * @param value the value
     */
    public synchronized void setValue(final V value) {
      this.value = value;
      notify();
    }
  }

  /**
   * A generic operation; operations are queued at each Partition.
   */
  private interface Op<K, V> {
    /**
     * Method to apply when dequeued by the Partition.
     * @param kvCache the raw LoadingCache, provided by the Partition.
     */
    void apply(LoadingCache<EncodedKey<K>, Wrapped<V>> kvCache);
  }

  /**
   * Wrapped values for use within each partition's cache.
   * Wrapping allows the partition to replace the value on a local update,
   * without updating the write time of the cache entry.
   */
  private static class Wrapped<V> {
    private V value;

    public Wrapped(final V value) {
      this.value = value;
    }

    public V getValue() {
      return value;
    }

    public void setValue(final V value) {
      this.value = value;
    }
  }

  /**
   * A push operation.
   */
  private class PushOp implements Op<K, V> {
    private static final int MAX_RETRY_COUNT = 10;

    private final EncodedKey<K> encodedKey;
    private final P preValue;

    public PushOp(final EncodedKey<K> encodedKey, final P preValue) {
      this.encodedKey = encodedKey;
      this.preValue = preValue;
    }

    /**
     * First, update the local value, only if it is already cached.
     * Second, send the update to the remote PS.
     * @param kvCache the raw LoadingCache, provided by the Partition.
     */
    @Override
    public void apply(final LoadingCache<EncodedKey<K>, Wrapped<V>> kvCache) {
      final Wrapped<V> wrapped = kvCache.getIfPresent(encodedKey);

      // If it exists, update the local value, without updating the cache's write time
      if (wrapped != null) {
        final V oldValue = wrapped.getValue();
        final V deltaValue = parameterUpdater.process(encodedKey.getKey(), preValue);
        if (deltaValue == null) {
          return;
        }
        final V updatedValue = parameterUpdater.update(oldValue, deltaValue);
        wrapped.setValue(updatedValue);
      }

      // Send to remote PS
      int retryCount = 0;
      while (true) {
        if (++retryCount > MAX_RETRY_COUNT) {
          throw new RuntimeException("Fail to send push message");
        }

        // re-resolve server for every retry
        // since an operation may throw NetworkException when routing table is obsolete
        final String serverId = serverResolver.resolveServer(encodedKey.getHash());
        LOG.log(Level.FINEST, "Resolve server for encodedKey. key: {0}, hash: {1}",
            new Object[]{encodedKey.getKey(), encodedKey.getHash()});

        try {
          sender.get().sendPushMsg(serverId, encodedKey, preValue);
        } catch (final NetworkException e) {
          LOG.log(Level.FINE, "NetworkException while sending push msg. Do retry", e);
          continue;
        }
        break;
      }
    }
  }

  /**
   * A pull operation.
   * Also exposes a blocking {@link #get} method to retrieve the result of the pull.
   */
  private class PullOp implements Op<K, V> {
    private final EncodedKey<K> encodedKey;
    private V value;

    public PullOp(final EncodedKey<K> encodedKey) {
      this.encodedKey = encodedKey;
    }

    /**
     * Delegate loading to the cache, then update the value and notify waiting gets.
     * @param kvCache the raw LoadingCache, provided by the Partition.
     */
    @Override
    public void apply(final LoadingCache<EncodedKey<K>, Wrapped<V>> kvCache) {
      try {
        final V loadedValue = kvCache.get(encodedKey).getValue();
        synchronized (this) {
          this.value = loadedValue;
          notify();
        }
      } catch (final ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * A blocking get.
     * @return the value
     */
    public V get() {
      synchronized (this) {
        while (value == null) {
          try {
            wait();
          } catch (final InterruptedException e) {
            LOG.log(Level.WARNING, "InterruptedException on wait", e);
          }
        }
        return value;
      }
    }
  }

  /**
   * A partition for the cache on the Worker.
   * The basic structure is similar to the partition for the Server at
   * {@link edu.snu.cay.services.ps.server.api.ParameterServer}.
   *
   * The partitions at the Worker can be independent of the partitions at the Server. In other words,
   * the number of worker-side partitions does not have to be equal to the number of server-side partitions.
   *
   * A remotely read pull remains in the local cache for a duration of expireTimeout.
   * Pushes are applied locally while the parameter is cached.
   * The single queue-and-thread, combined with the server, provides a guarantee that
   * all previous local pushes are applied to a pull, if it is locally cached.
   *
   * This means pull operations are queued behind push operations.
   * We should further explore this trade-off with real ML workloads.
   */
  private static class WorkerThread<K, P, V> implements Runnable {
    private static final int MAX_RETRY_COUNT = 10;
    private static final long QUEUE_TIMEOUT_MS = 3000;

    private final LoadingCache<EncodedKey<K>, Wrapped<V>> kvCache;
    private final BlockingQueue<Op<K, V>> queue;
    private final ArrayList<Op<K, V>> localOps; // Operations drained from the queue, and processed locally.
    private final int drainSize; // Max number of operations to drain per iteration.

    private volatile boolean close = false;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public WorkerThread(final ConcurrentMap<K, PullFuture<V>> pendingPulls,
                        final ServerResolver serverResolver,
                        final InjectionFuture<WorkerMsgSender<K, P>> sender,
                        final int queueSize,
                        final long expireTimeout) {
      kvCache = CacheBuilder.newBuilder()
          .concurrencyLevel(1)
          .expireAfterWrite(expireTimeout, TimeUnit.MILLISECONDS)
          .build(new CacheLoader<EncodedKey<K>, Wrapped<V>>() {
            private static final long RETRY_INTERVAL_MS = 5000;

            @Override
            public Wrapped<V> load(final EncodedKey<K> encodedKey) {
              final PullFuture<V> future = new PullFuture<>();
              pendingPulls.put(encodedKey.getKey(), future);

              V value = null;

              int retryCount = 0;
              while (value == null) {
                if (++retryCount > MAX_RETRY_COUNT) {
                  throw new RuntimeException("Fail to load a value for pull");
                }

                // re-resolve server for every retry
                // since an operation may throw NetworkException when routing table is obsolete
                final String serverId = serverResolver.resolveServer(encodedKey.getHash());
                LOG.log(Level.FINEST, "Resolve server for encodedKey. key: {0}, hash: {1}",
                    new Object[]{encodedKey.getKey(), encodedKey.getHash()});

                try {
                  sender.get().sendPullMsg(serverId, encodedKey);
                } catch (final NetworkException e) {
                  LOG.log(Level.FINE, "NetworkException while sending pull msg. Do retry", e);
                  continue;
                }

                // if failed, future returns null and pull is retried in the while loop
                value = future.getValue(RETRY_INTERVAL_MS);
              }

              pendingPulls.remove(encodedKey.getKey());
              return new Wrapped<>(value);
            }
          });
      queue = new ArrayBlockingQueue<>(queueSize);
      this.drainSize = queueSize / 10;
      this.localOps = new ArrayList<>(drainSize);
    }

    /**
     * Enqueue an operation onto the queue, blocking if the queue is full.
     * When the queue is full, this method will block; thus, a full queue will block the thread calling
     * enqueue, e.g., from the NCS message thread pool, until the queue is drained.
     *
     * @param op the operation to enqueue
     */
    public void enqueue(final Op<K, V> op) {
      try {
        queue.put(op);
      } catch (final InterruptedException e) {
        LOG.log(Level.SEVERE, "Enqueue failed with InterruptedException", e);
        return;
      }
    }

    /**
     * Invalidate all cached pulls.
     */
    public void invalidateAll() {
      kvCache.invalidateAll();
    }

    /**
     * @return number of pending operations in the queue.
     */
    public int opsPending() {
      int opsPending = 0;
      opsPending += queue.size();
      opsPending += localOps.size();
      return opsPending;
    }

    /**
     * Loop that dequeues operations and applies them.
     * Dequeues are only performed through this thread.
     */
    @Override
    public void run() {
      while (!close || !queue.isEmpty()) {
        // First, poll and apply. The timeout allows the run thread to close cleanly within timeout ms.
        try {
          final Op<K, V> op = queue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (op == null) {
            continue;
          }
          op.apply(kvCache);
        } catch (final InterruptedException e) {
          LOG.log(Level.SEVERE, "Poll failed with InterruptedException", e);
          continue;
        }

        // Then, drain up to drainSize of the remaining queue and apply.
        // Calling drainTo does not block if queue is empty, which is why we poll first.
        // This should be faster than polling each op, because the blocking queue's lock is only acquired once.
        queue.drainTo(localOps, drainSize);
        for (final Op<K, V> op : localOps) {
          op.apply(kvCache);
        }
        localOps.clear();
      }

      shutdown();
    }

    /**
     * Close the run thread.
     */
    public void close() {
      close = true;
    }

    private void shutdown() {
      shutdown.set(true);
      synchronized (shutdown) {
        shutdown.notifyAll();
      }
    }

    /**
     * Wait for shutdown confirmation (clean close has finished).
     */
    public void waitForShutdown() {
      while (!shutdown.get()) {
        try {
          synchronized (shutdown) {
            shutdown.wait();
          }
        } catch (final InterruptedException e) {
          LOG.log(Level.WARNING, "InterruptedException while waiting for close to complete", e);
        }
      }
    }
  }
}