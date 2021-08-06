/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

public class ScrollContextLimitException extends OpenSearchException {
    public ScrollContextLimitException(Throwable cause) {
        super(cause);
    }

    public ScrollContextLimitException(String msg, Object... args) {
        super(msg, args);
    }

    public ScrollContextLimitException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }

    public ScrollContextLimitException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        Throwable cause = unwrapCause();
        if (cause == this) {
            return RestStatus.TOO_MANY_REQUESTS;
        } else {
            return ExceptionsHelper.status(cause);
        }
    }
}
