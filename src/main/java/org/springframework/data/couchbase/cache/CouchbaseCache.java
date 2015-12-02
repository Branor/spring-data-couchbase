/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.cache;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import java.util.concurrent.ExecutionException;

/**
 * The {@link CouchbaseCache} class implements the Spring Cache interface on top of Couchbase Server and the Couchbase
 * Java SDK.
 *
 * @see <a href="http://static.springsource.org/spring/docs/current/spring-framework-reference/html/cache.html">
 *   Official Spring Cache Reference</a>
 * @author Michael Nitschinger
 * @author Konrad Król
 */
public class CouchbaseCache implements Cache {

  private final Logger logger = LoggerFactory.getLogger(CouchbaseCache.class);
  /**
   * The actual CouchbaseClient instance.
   */
  private final CouchbaseClient client;

  /**
   * The name of the cache.
   */
  private final String name;
  
  /**
   * TTL value for objects in this cache
   */
  private final int ttl;

  /**
   * Delimiter for separating the name from the key in the document id of objects in this cache
   */
  private final String DELIMITER = ":";

  /**
   * The design document of the view used by this cache to retrieve documents in a specific namespace.
   */
  private final String CACHE_DESIGN_DOCUMENT = "cache";

  /**
   * The name of the view used by this cache to retrieve documents in a specific namespace.
   */
  private final String CACHE_VIEW = "names";

  /**
   * Determines whether to always use the flush() method to clear the cache.
   */
  private Boolean alwaysFlush = false;

  /**
   * Construct the cache and pass in the CouchbaseClient instance.
   *
   * @param name the name of the cache reference.
   * @param client the CouchbaseClient instance.
   */
  public CouchbaseCache(final String name, final CouchbaseClient client) {
    this.name = name;
    this.client = client;
    this.ttl = 0;

    if(!getAlwaysFlush())
      ensureViewExists();
  }

  /**
   * Construct the cache and pass in the CouchbaseClient instance.
   *
   * @param name the name of the cache reference.
   * @param client the CouchbaseClient instance.
   * @param ttl TTL value for objects in this cache
   */
  public CouchbaseCache(final String name, final CouchbaseClient client, int ttl) {
    this.name = name;
    this.client = client;
    this.ttl = ttl;

    if(!getAlwaysFlush())
      ensureViewExists();
  }

  /**
   * Returns the name of the cache.
   *
   * @return the name of the cache.
   */
  public final String getName() {
    return name;
  }

  /**
   * Returns the actual CouchbaseClient instance.
   *
   * @return the actual CouchbaseClient instance.
   */
  public final CouchbaseClient getNativeCache() {
    return client;
  }
  
  /**
   * Returns the TTL value for this cache.
   * 
   * @return TTL value
   */
  public final int getTtl() {
	  return ttl;
  }

  /**
   * Get an element from the cache.
   *
   * @param key the key to lookup against.
   * @return the fetched value from Couchbase.
   */
  public final ValueWrapper get(final Object key) {
    String documentId = getDocumentId(key.toString());
    Object result = client.get(documentId);
    return (result != null ? new SimpleValueWrapper(result) : null);
  }

  @SuppressWarnings("unchecked")
  public final <T> T get(final Object key, final Class<T> clazz) {
    String documentId = getDocumentId(key.toString());
    return (T) client.get(documentId);
  }

  /**
   * Store a object in Couchbase.
   *
   * @param key the Key of the storable object.
   * @param value the Object to store.
   */
  public final void put(final Object key, final Object value) {
    if (value != null) {
      String documentId = getDocumentId(key.toString());
      client.set(documentId, ttl, value);
    } else {
      evict(key);
    }
  }

  /**
   * Remove an object from Couchbase.
   *
   * @param key the Key of the object to delete.
   */
  public final void evict(final Object key) {
    String documentId = getDocumentId(key.toString());
    client.delete(documentId);
  }

  /**
   * Clear the complete cache.
   *
   * Note that this action is very destructive, so only use it with care.
   * Also note that "flush" may not be enabled on the bucket.
   */
  public final void clear() {
    if(getAlwaysFlush() || name == null || name.trim().length() == 0)
      try {
        client.flush().get();
      } catch (Exception e) {
        logger.error("Couchbase flush error: ", e);
      }
    else
      evictAllDocuments();
  }

  /*
   * (non-Javadoc)
   * @see org.springframework.cache.Cache#putIfAbsent(java.lang.Object, java.lang.Object)
   */
  public ValueWrapper putIfAbsent(Object key, Object value) {
		
      if(get(key) == null) {
          put(key, value);
          return null;
      }

      return new SimpleValueWrapper(value);
	}

  private String getDocumentId(String key) {
    if(name == null || name.trim().length() == 0)
      return key;
    else
      return name + DELIMITER + key;
  }

  private void evictAllDocuments() {
    View view = client.getView(CACHE_DESIGN_DOCUMENT, CACHE_VIEW);
    Query query = new Query();
    query.setStale(Stale.FALSE);
    query.setKey(name);

    ViewResponse response = client.query(view, query);
    for(ViewRow row : response) {
      client.delete(row.getId());
    }
  }

  private void ensureViewExists() {
    DesignDocument doc = null;

    try {
      doc = client.getDesignDoc(CACHE_DESIGN_DOCUMENT);
    } catch (Exception e) {
    }

    if(doc != null) {
      for(ViewDesign view : doc.getViews()) {
        if(view.getName() == CACHE_VIEW)
          return;
      }
    }

    String function = "function (doc, meta) {var tokens = meta.id.split('" + DELIMITER + "'); if(tokens.length > 0)emit(tokens[0]);}";
    doc = new DesignDocument(CACHE_DESIGN_DOCUMENT);
    ViewDesign view = new ViewDesign(CACHE_VIEW, function);
    doc.setView(view);

    client.createDesignDoc(doc);
  }

  /**
   * Gets whether the cache should always use the flush() method to clear all documents.
   *
   * @return returns whether the cache should always use the flush() method to clear all documents.
   */
  public Boolean getAlwaysFlush() {
    return alwaysFlush;
  }

  /**
   * Sets whether the cache should always use the flush() method to clear all documents.
   *
   * @param alwaysFlush Whether the cache should always use the flush() method to clear all documents.
   */
  public void setAlwaysFlush(Boolean alwaysFlush) {
    this.alwaysFlush = alwaysFlush;
  }
}
