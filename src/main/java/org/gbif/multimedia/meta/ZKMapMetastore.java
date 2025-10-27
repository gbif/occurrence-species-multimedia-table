/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.multimedia.meta;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;

/**
 * An implementation of the MapMetastore backed by Zookeeper.
 *
 * This uses a Zookeeper Path Cache pattern to watch for changes of the ZK node.
 */
@Slf4j
public class ZKMapMetastore implements Closeable {

  private final CuratorFramework client;
  private final String zkNodePath;

  public ZKMapMetastore(String zkEnsemble, int retryIntervalMs, String zkNodePath) {
    this.zkNodePath = zkNodePath;
    client = CuratorFrameworkFactory.newClient(zkEnsemble, new RetryNTimes(Integer.MAX_VALUE, retryIntervalMs));
    client.start();
    checkPathExists();
  }

  public void update(String meta) throws Exception {
    log.info("Updating MapTables[{}] with: {}", zkNodePath, meta);
    client.setData().forPath(zkNodePath, meta.getBytes(StandardCharsets.UTF_8));
  }

  public String getCurrentTableName() throws Exception {
    byte[] data = client.getData().forPath(zkNodePath);
    return new String(data, StandardCharsets.UTF_8);
  }

  private void checkPathExists() {
    try {
      if (client.checkExists().forPath(zkNodePath) == null) {
        client.create().creatingParentsIfNeeded().forPath(zkNodePath, new byte[0]);
        log.info("Created ZK path: {}", zkNodePath);
      } else {
        log.debug("ZK path already exists: {}", zkNodePath);
      }
    } catch (Exception e) {
      log.error("Failed to ensure ZK path exists: {}", zkNodePath, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    client.close();
  }
}
