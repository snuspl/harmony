/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.cay.services.et.evaluator.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.snu.cay.services.et.configuration.parameters.KeyCodec;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by xyzi on 12/01/2018.
 */
public final class EncodedKeyCache<K> {
  private static final Logger LOG = Logger.getLogger(EncodedKeyCache.class.getName());

  private final LoadingCache<K, EncodedKey<K>> encodedKeyLoadingCache;

  private final StreamingCodec<K> keyCodec;

  @Inject
  private EncodedKeyCache(@Parameter(KeyCodec.class) final StreamingCodec<K> keyCodec) {
    this.encodedKeyLoadingCache = initCache();
    this.keyCodec = keyCodec;
  }

  public EncodedKey<K> getEncodedKey(final K key) {
    try {
      return encodedKeyLoadingCache.get(key);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private LoadingCache<K, EncodedKey<K>> initCache() {
    return CacheBuilder.newBuilder()
        .concurrencyLevel(2)
        .build(new CacheLoader<K, EncodedKey<K>>() {
          @Override
          public EncodedKey<K> load(final K key) throws Exception {
            LOG.log(Level.INFO, "load EncodedKey for key {0}", key);
            return new EncodedKey<>(key, keyCodec);
          }
        });
  }
}
