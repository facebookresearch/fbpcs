/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

package com.facebook.business.cloudbridge.pl.server.deployment.cache;

import java.util.concurrent.ConcurrentHashMap;

public class ThreadSafeCache<K, V> {
  private ConcurrentHashMap<K, V> map;

  public ThreadSafeCache() {
    map = new ConcurrentHashMap<K, V>();
  }

  public void put(K key, V value) {
    map.put(key, value);
  }

  public V get(K key) {
    return map.get(key);
  }

  public void remove(K key) {
    map.remove(key);
  }

  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

  public int size() {
    return map.size();
  }

  public void clear() {
    map.clear();
  }
}
