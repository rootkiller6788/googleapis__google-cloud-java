/*
 * Copyright 2026 Google LLC
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google LLC nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.google.api.gax.rpc;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.resumable.ChunkUploadRequest;
import com.google.api.gax.resumable.ChunkUploadResponse;
import com.google.api.gax.resumable.QueryStatusRequest;
import com.google.api.gax.resumable.QueryStatusResponse;
import com.google.api.gax.resumable.ResumableUploadStatus;
import com.google.api.gax.retrying.ExponentialRetryAlgorithm;
import com.google.api.gax.retrying.RetryAlgorithm;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.retrying.RetryingFuture;
import com.google.api.gax.retrying.ScheduledRetryingExecutor;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

/**
 * Coordinates chunk transmission steps of a resumable upload session.
 *
 * @param <ResponseT> the type of the final response message returned once the upload completes
 */
@InternalApi
@NullMarked
final class ResumableUploadChunkCoordinator<ResponseT> {

  // Per GAX-R7: local and per-attempt deadlines are derived from the global timeout at chunk
  // dispatch time; only backoff delay parameters are static.
  private static final RetrySettings CHUNK_RETRY_SETTINGS =
      RetrySettings.newBuilder()
          .setInitialRetryDelayDuration(Duration.ofMillis(100))
          .setRetryDelayMultiplier(1.3)
          .setMaxRetryDelayDuration(Duration.ofMinutes(1))
          .setMaxAttempts(5)
          .build();

  private final Executor chunkExecutor =
      MoreExecutors.newSequentialExecutor(MoreExecutors.directExecutor());

  private final UnaryCallable<ChunkUploadRequest, ChunkUploadResponse<ResponseT>>
      uploadChunkCallable;
  private final UnaryCallable<QueryStatusRequest, QueryStatusResponse<ResponseT>>
      queryStatusCallable;
  private final String uploadUrl;
  private final RewindableStreamBuffer buffer;
  private final ResumableUploadCallSettings settings;
  private final long deadlineNanos;
  private final ApiCallContext callContext;
  private final ClientContext clientContext;
  private final SettableApiFuture<ResponseT> result = SettableApiFuture.create();
  private volatile @Nullable ApiFuture<?> currentChunkFuture;

  ResumableUploadChunkCoordinator(
      UnaryCallable<ChunkUploadRequest, ChunkUploadResponse<ResponseT>> uploadChunkCallable,
      UnaryCallable<QueryStatusRequest, QueryStatusResponse<ResponseT>> queryStatusCallable,
      String uploadUrl,
      InputStream payload,
      ResumableUploadCallSettings settings,
      long deadlineNanos,
      ClientContext clientContext) {
    this.uploadChunkCallable =
        checkNotNull(uploadChunkCallable, "uploadChunkCallable must not be null");
    this.queryStatusCallable =
        checkNotNull(queryStatusCallable, "queryStatusCallable must not be null");
    this.uploadUrl = checkNotNull(uploadUrl, "uploadUrl must not be null");
    checkNotNull(payload, "payload must not be null");
    this.settings = checkNotNull(settings, "settings must not be null");
    this.deadlineNanos = deadlineNanos;
    this.clientContext = checkNotNull(clientContext, "clientContext must not be null");
    this.callContext = clientContext.getDefaultCallContext();
    this.buffer = new RewindableStreamBuffer(payload, settings.getChunkSize(), uploadUrl);
  }

  ApiFuture<ResponseT> start() {
    result.addListener(
        () -> {
          ApiFuture<?> chunk = currentChunkFuture;
          if (result.isCancelled() && chunk != null) {
            chunk.cancel(true);
          }
        },
        MoreExecutors.directExecutor());
    chunkExecutor.execute(() -> transmitChunk(0L));
    return result;
  }

  private void transmitChunk(long currentOffset) {
    // Abort if the session was already completed or canceled.
    if (result.isDone()) {
      return;
    }

    // Read the next chunk slice from the payload stream.
    try {
      buffer.fill(currentOffset);
    } catch (IOException e) {
      result.setException(e);
      return;
    }

    // Determine if this is the final chunk and build the chunk request.
    ResumableUploadCommand command;
    if (buffer.isFinal()) {
      if (buffer.isEmpty()) {
        command = ResumableUploadCommand.FINALIZE;
      } else {
        command = ResumableUploadCommand.UPLOAD_FINALIZE;
      }
    } else {
      command = ResumableUploadCommand.UPLOAD;
    }
    ChunkUploadRequest chunkRequest =
        ChunkUploadRequest.newBuilder()
            .setUploadUrl(uploadUrl)
            .setPayload(buffer.getBuffer())
            .setPayloadLength(buffer.getPayloadLength())
            .setOffset(buffer.getBufferBaseOffset())
            .setFinal(buffer.isFinal())
            .build();

    // Dispatch the chunk upload call and register the in-flight future for cancellation.
    // Per GAX-R7: data-plane chunk commands use half of the original global timeout as both
    // the local and per-attempt deadline, trimmed to the remaining global deadline.
    long remainingNanos = Math.max(1L, deadlineNanos - clientContext.getClock().nanoTime());
    long halfGlobalNanos = settings.getGlobalTimeout().dividedBy(2).toNanos();
    Duration chunkDeadline = Duration.ofNanos(Math.min(halfGlobalNanos, remainingNanos));

    RetrySettings derivedChunkRetrySettings =
        CHUNK_RETRY_SETTINGS.toBuilder()
            .setTotalTimeoutDuration(chunkDeadline)
            .setInitialRpcTimeoutDuration(chunkDeadline)
            .setRpcTimeoutMultiplier(1.0)
            .setMaxRpcTimeoutDuration(chunkDeadline)
            .build();
    ApiCallContext chunkCallContext = callContext.withRetrySettings(derivedChunkRetrySettings);
    ChunkAttemptCallable<ResponseT> attemptCallable =
        new ChunkAttemptCallable<>(
            uploadChunkCallable,
            queryStatusCallable,
            buffer,
            uploadUrl,
            chunkRequest,
            chunkCallContext,
            command,
            deadlineNanos,
            clientContext.getClock());

    RetryAlgorithm<ChunkUploadResponse<ResponseT>> retryAlgorithm =
        new RetryAlgorithm<>(
            new ResumableUploadResultRetryAlgorithm<>(attemptCallable::getCurrentCommand),
            new ExponentialRetryAlgorithm(CHUNK_RETRY_SETTINGS, clientContext.getClock()));
    ScheduledRetryingExecutor<ChunkUploadResponse<ResponseT>> retryingExecutor =
        new ScheduledRetryingExecutor<>(retryAlgorithm, clientContext.getExecutor());

    RetryingFuture<ChunkUploadResponse<ResponseT>> retryingFuture =
        retryingExecutor.createFuture(attemptCallable, chunkCallContext);
    attemptCallable.setRetryingFuture(retryingFuture);
    this.currentChunkFuture = retryingFuture;

    ApiFutures.addCallback(
        retryingFuture,
        new ApiFutureCallback<ChunkUploadResponse<ResponseT>>() {
          @Override
          public void onSuccess(ChunkUploadResponse<ResponseT> response) {
            if (result.isDone()) {
              return;
            }
            long nextOffset = buffer.getBufferBaseOffset() + buffer.getPayloadLength();
            if (response.getUploadStatus() == ResumableUploadStatus.FINAL) {
              result.set(response.getResponse());
            } else if (buffer.isFinal()) {
              result.setException(
                  new IllegalStateException(
                      "Upload stream ended and final chunk was transmitted, but server returned"
                          + " incomplete status for upload URL: "
                          + uploadUrl));
            } else {
              chunkExecutor.execute(() -> transmitChunk(nextOffset));
            }
          }

          @Override
          public void onFailure(Throwable t) {
            if (t instanceof CancellationException || result.isDone()) {
              return;
            }
            result.setException(t);
          }
        },
        MoreExecutors.directExecutor());
    try {
      attemptCallable.call();
    } catch (Throwable t) {
      if (!retryingFuture.isDone()) {
        result.setException(t);
      }
    }
  }
}
