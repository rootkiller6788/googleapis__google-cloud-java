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

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Test;

class ResumableUploadProgressTest {

  @Test
  void builder_defaults() {
    ResumableUploadProgress progress = ResumableUploadProgress.newBuilder().build();
    assertThat(progress.getUploadUrl()).isNull();
    assertThat(progress.getBytesUploaded()).isEqualTo(0L);
    assertThat(progress.getState()).isEqualTo(ResumableUploadProgress.State.STARTING);
    assertThat(progress.getException()).isNull();
  }

  @Test
  void builder_explicitValuesAndToBuilder() {
    Exception ex = new RuntimeException("test error");
    ResumableUploadProgress progress =
        ResumableUploadProgress.newBuilder()
            .setUploadUrl("https://upload.url/session-1")
            .setBytesUploaded(1024L)
            .setState(ResumableUploadProgress.State.UPLOADING)
            .setException(ex)
            .build();

    assertThat(progress.getUploadUrl()).isEqualTo("https://upload.url/session-1");
    assertThat(progress.getBytesUploaded()).isEqualTo(1024L);
    assertThat(progress.getState()).isEqualTo(ResumableUploadProgress.State.UPLOADING);
    assertThat(progress.getException()).isSameInstanceAs(ex);

    ResumableUploadProgress modified =
        progress.toBuilder()
            .setState(ResumableUploadProgress.State.FINALIZED)
            .setBytesUploaded(2048L)
            .build();

    assertThat(modified.getState()).isEqualTo(ResumableUploadProgress.State.FINALIZED);
    assertThat(modified.getBytesUploaded()).isEqualTo(2048L);
    assertThat(modified.getUploadUrl()).isEqualTo("https://upload.url/session-1");
  }

  @Test
  void allStates_canBeRepresented() {
    for (ResumableUploadProgress.State state : ResumableUploadProgress.State.values()) {
      ResumableUploadProgress progress =
          ResumableUploadProgress.newBuilder().setState(state).build();
      assertThat(progress.getState()).isEqualTo(state);
    }
  }
}
