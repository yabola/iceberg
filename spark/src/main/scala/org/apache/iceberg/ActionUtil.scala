/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg

private[iceberg] object ActionUtil {

  def computeAvgManifestEntrySizeInBytes(manifests: Iterable[ManifestFile]): Long = {
    var totalSize = 0L
    var numEntries = 0

    manifests.foreach { manifest =>
      require(hasFilesCounts(manifest), s"Manifest ${manifest.path} does not have file counts")

      totalSize += manifest.length
      numEntries += manifest.addedFilesCount + manifest.existingFilesCount + manifest.deletedFilesCount
    }

    require(totalSize > 0, "Total size of manifests must be greater than 0")
    require(numEntries > 0, "Number of manifest entries must be greater than 0")

    totalSize / numEntries
  }

  private def hasFilesCounts(manifest: ManifestFile): Boolean = {
    manifest.addedFilesCount != null && manifest.existingFilesCount != null && manifest.deletedFilesCount != null
  }
}
