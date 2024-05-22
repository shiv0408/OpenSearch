/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import java.io.IOException;
import java.io.InputStream;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.core.action.ActionListener;

/**
 * An interface to read/write and object from/to a remote storage. This interface is agnostic of the remote storage type.
 * @param <T> The object type which can be upload to or download from remote storage.
 */
public interface RemoteObject <T> {
    public T get();
    public String clusterUUID();
    public InputStream serialize() throws IOException;
    public T deserialize(InputStream inputStream) throws IOException;

    public CheckedRunnable<IOException> writeAsync(ActionListener<Void> listener);
    public T read() throws IOException;
    public void readAsync(ActionListener<T> listener);

}
