//
//
// Copyright 2015 gRPC authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//

#include <grpc/support/port_platform.h>

#include "src/core/lib/compression/message_compress.h"

#include <string.h>

#include <zconf.h>
#include <zlib.h>

#include <zstd.h>
#include <zstd_errors.h>

#include <grpc/slice_buffer.h>
#include <grpc/support/alloc.h>
#include <grpc/support/log.h>

#include "src/core/lib/slice/slice.h"

#define OUTPUT_BLOCK_SIZE 1024

static int zlib_body(z_stream* zs, grpc_slice_buffer* input,
                     grpc_slice_buffer* output,
                     int (*flate)(z_stream* zs, int flush)) {
  int r = Z_STREAM_END;  // Do not fail on an empty input.
  int flush;
  size_t i;
  grpc_slice outbuf = GRPC_SLICE_MALLOC(OUTPUT_BLOCK_SIZE);
  const uInt uint_max = ~uInt{0};

  GPR_ASSERT(GRPC_SLICE_LENGTH(outbuf) <= uint_max);
  zs->avail_out = static_cast<uInt> GRPC_SLICE_LENGTH(outbuf);
  zs->next_out = GRPC_SLICE_START_PTR(outbuf);
  flush = Z_NO_FLUSH;
  for (i = 0; i < input->count; i++) {
    if (i == input->count - 1) flush = Z_FINISH;
    GPR_ASSERT(GRPC_SLICE_LENGTH(input->slices[i]) <= uint_max);
    zs->avail_in = static_cast<uInt> GRPC_SLICE_LENGTH(input->slices[i]);
    zs->next_in = GRPC_SLICE_START_PTR(input->slices[i]);
    do {
      if (zs->avail_out == 0) {
        grpc_slice_buffer_add_indexed(output, outbuf);
        outbuf = GRPC_SLICE_MALLOC(OUTPUT_BLOCK_SIZE);
        GPR_ASSERT(GRPC_SLICE_LENGTH(outbuf) <= uint_max);
        zs->avail_out = static_cast<uInt> GRPC_SLICE_LENGTH(outbuf);
        zs->next_out = GRPC_SLICE_START_PTR(outbuf);
      }
      r = flate(zs, flush);
      if (r < 0 && r != Z_BUF_ERROR /* not fatal */) {
        gpr_log(GPR_INFO, "zlib error (%d)", r);
        goto error;
      }
    } while (zs->avail_out == 0);
    if (zs->avail_in) {
      gpr_log(GPR_INFO, "zlib: not all input consumed");
      goto error;
    }
  }
  if (r != Z_STREAM_END) {
    gpr_log(GPR_INFO, "zlib: Data error");
    goto error;
  }

  GPR_ASSERT(outbuf.refcount);
  outbuf.data.refcounted.length -= zs->avail_out;
  grpc_slice_buffer_add_indexed(output, outbuf);

  return 1;

error:
  grpc_core::CSliceUnref(outbuf);
  return 0;
}

static void* zalloc_gpr(void* /*opaque*/, unsigned int items,
                        unsigned int size) {
  return gpr_malloc(items * size);
}

static void zfree_gpr(void* /*opaque*/, void* address) { gpr_free(address); }

static int zlib_compress(grpc_slice_buffer* input, grpc_slice_buffer* output,
                         int gzip) {
  z_stream zs;
  int r;
  size_t i;
  size_t count_before = output->count;
  size_t length_before = output->length;
  memset(&zs, 0, sizeof(zs));
  zs.zalloc = zalloc_gpr;
  zs.zfree = zfree_gpr;
  r = deflateInit2(&zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 | (gzip ? 16 : 0),
                   8, Z_DEFAULT_STRATEGY);
  GPR_ASSERT(r == Z_OK);
  r = zlib_body(&zs, input, output, deflate) && output->length < input->length;
  if (!r) {
    for (i = count_before; i < output->count; i++) {
      grpc_core::CSliceUnref(output->slices[i]);
    }
    output->count = count_before;
    output->length = length_before;
  }
  deflateEnd(&zs);
  return r;
}

static int zlib_decompress(grpc_slice_buffer* input, grpc_slice_buffer* output,
                           int gzip) {
  z_stream zs;
  int r;
  size_t i;
  size_t count_before = output->count;
  size_t length_before = output->length;
  memset(&zs, 0, sizeof(zs));
  zs.zalloc = zalloc_gpr;
  zs.zfree = zfree_gpr;
  r = inflateInit2(&zs, 15 | (gzip ? 16 : 0));
  GPR_ASSERT(r == Z_OK);
  r = zlib_body(&zs, input, output, inflate);
  if (!r) {
    for (i = count_before; i < output->count; i++) {
      grpc_core::CSliceUnref(output->slices[i]);
    }
    output->count = count_before;
    output->length = length_before;
  }
  inflateEnd(&zs);
  return r;
}

void errorZDecompress(ZSTD_DStream** pStream, size_t countBefore, size_t lengthBefore, grpc_slice_buffer *outbuf, grpc_slice * out) {
  if (out != nullptr) {
    grpc_core::CSliceUnref(*out);
  }

  if (pStream != nullptr) {
      auto err = ZSTD_freeDStream(*pStream);
      if (ZSTD_isError(err) > 0) {
          gpr_log(GPR_INFO, "zstd: decompress flush all error  :%s", ZSTD_getErrorName(err));
      }
  }

  for (size_t i = countBefore; i < outbuf->count; i++) {
    grpc_core::CSliceUnref(outbuf->slices[i]);
  }
  outbuf->count = countBefore;
  outbuf->length = lengthBefore;
}

void errorZCompress(ZSTD_CStream** pStream, size_t countBefore, size_t lengthBefore, grpc_slice_buffer *outbuf, grpc_slice * out) {
  if (out != nullptr) {
    grpc_core::CSliceUnref(*out);
  }

  if (pStream != nullptr) {
      auto err = ZSTD_freeCStream(*pStream);
      if (ZSTD_isError(err) > 0) {
          gpr_log(GPR_INFO, "zstd: decompress flush all error  :%s", ZSTD_getErrorName(err));
      }
  }

  for (size_t i = countBefore; i < outbuf->count; i++) {
    grpc_core::CSliceUnref(outbuf->slices[i]);
  }
  outbuf->count = countBefore;
  outbuf->length = lengthBefore;
}

static int zstd_compress(grpc_slice_buffer* input, grpc_slice_buffer* output) {

  size_t count_before = output->count;
  size_t length_before = output->length;

  ZSTD_CStream* pStream = ZSTD_createCStream();
  ZSTD_inBuffer   inBuffer;
  ZSTD_outBuffer outBuffer;

  size_t err = ZSTD_initCStream(pStream, 3);
  if (ZSTD_isError(err) > 0) {
    gpr_log(GPR_INFO, "zstd: initialize compress steram error :%s", ZSTD_getErrorName(err));
    errorZCompress(&pStream, count_before, length_before, output, nullptr);
    return 0;
  }

  grpc_slice outbuf = GRPC_SLICE_MALLOC(OUTPUT_BLOCK_SIZE);
  outBuffer.size = static_cast<uInt> GRPC_SLICE_LENGTH(outbuf);
  outBuffer.dst = GRPC_SLICE_START_PTR(outbuf);
  outBuffer.pos = 0;
  const uInt uint_max = ~static_cast<uInt>(0);
  GPR_ASSERT(GRPC_SLICE_LENGTH(outbuf) <= uint_max);

  for (size_t i = 0; i < input->count; i++) {
    GPR_ASSERT(GRPC_SLICE_LENGTH(input->slices[i]) <= uint_max);
    inBuffer.src = GRPC_SLICE_START_PTR(input->slices[i]);
    inBuffer.size = GRPC_SLICE_LENGTH(input->slices[i]);
    inBuffer.pos = 0;

    err = ZSTD_compressStream2(pStream, &outBuffer, &inBuffer, ZSTD_EndDirective::ZSTD_e_continue);
    if (ZSTD_isError(err) > 0) {
      gpr_log(GPR_INFO, "zstd: compress stream error :%s", ZSTD_getErrorName(err));
      errorZCompress(&pStream, count_before, length_before, output, &outbuf);
      return 0;
    }

    auto lastChunk = (i == (input->count - 1));
    bool finished = false;

    while (!finished) {
      if (outBuffer.pos == outBuffer.size) {
        grpc_slice_buffer_add_indexed(output, outbuf);

        outbuf = GRPC_SLICE_MALLOC(OUTPUT_BLOCK_SIZE);
        outBuffer.size = static_cast<uInt> GRPC_SLICE_LENGTH(outbuf);
        outBuffer.dst = GRPC_SLICE_START_PTR(outbuf);
        outBuffer.pos = 0;
      }

      ZSTD_EndDirective directive = lastChunk ? ZSTD_EndDirective::ZSTD_e_end: ZSTD_EndDirective::ZSTD_e_flush;
      err = ZSTD_compressStream2(pStream, &outBuffer, &inBuffer, directive);
      if (ZSTD_isError(err) > 0) {
        gpr_log(GPR_INFO, "zstd: compress flush all error  :%s", ZSTD_getErrorName(err));
        errorZCompress(&pStream, count_before, length_before, output, &outbuf);
        return 0;
      }

      finished = lastChunk ? err == 0 : inBuffer.pos == inBuffer.size;
    }

    if (inBuffer.pos != inBuffer.size) {
      gpr_log(GPR_INFO, "zstd: not all input consumed");
      errorZCompress(&pStream, count_before, length_before, output, &outbuf);
      return 0;
    }
  }

  err = ZSTD_freeCStream(pStream);
  if (ZSTD_isError(err) > 0) {
    gpr_log(GPR_INFO, "zstd: compress flush all error  :%s", ZSTD_getErrorName(err));
    errorZCompress(nullptr, count_before, length_before, output, &outbuf);
    return 0;
  }

  GPR_ASSERT(outbuf.refcount);
  outbuf.data.refcounted.length = outBuffer.pos;
  grpc_slice_buffer_add_indexed(output, outbuf);

  if (output->length > input->length){
    gpr_log(GPR_INFO, "zstd: fail to apply compression");
    errorZCompress(nullptr, count_before, length_before, output, nullptr);
    return 0;
  }
  return 1;
}

static int zstd_decompress(grpc_slice_buffer* input, grpc_slice_buffer* output) {

  size_t count_before = output->count;
  size_t length_before = output->length;

  ZSTD_DStream* pStream = ZSTD_createDStream();
  ZSTD_inBuffer   inBuffer;
  ZSTD_outBuffer outBuffer;

  size_t err = ZSTD_initDStream(pStream);
  if (ZSTD_isError(err) > 0) {
    gpr_log(GPR_INFO, "zstd: initialize decompress steram error :%s", ZSTD_getErrorName(err));
    errorZDecompress(&pStream, count_before, length_before, output, nullptr);
    return 0;
  }

  grpc_slice outbuf = GRPC_SLICE_MALLOC(OUTPUT_BLOCK_SIZE);
  outBuffer.size = static_cast<uInt> GRPC_SLICE_LENGTH(outbuf);
  outBuffer.dst = GRPC_SLICE_START_PTR(outbuf);
  outBuffer.pos = 0;
  const uInt uint_max = ~static_cast<uInt>(0);
  GPR_ASSERT(GRPC_SLICE_LENGTH(outbuf) <= uint_max);

  for (size_t i = 0; i < input->count; i++) {
    GPR_ASSERT(GRPC_SLICE_LENGTH(input->slices[i]) <= uint_max);
    inBuffer.src = GRPC_SLICE_START_PTR(input->slices[i]);
    inBuffer.size = GRPC_SLICE_LENGTH(input->slices[i]);
    inBuffer.pos = 0;

    do
    {
      if (outBuffer.pos == outBuffer.size) {
        grpc_slice_buffer_add_indexed(output, outbuf);

        outbuf = GRPC_SLICE_MALLOC(OUTPUT_BLOCK_SIZE);
        outBuffer.size = static_cast<uInt> GRPC_SLICE_LENGTH(outbuf);
        outBuffer.dst = GRPC_SLICE_START_PTR(outbuf);
        outBuffer.pos = 0;
      }

      err = ZSTD_decompressStream(pStream, &outBuffer, &inBuffer);
      if (ZSTD_isError(err) > 0) {
        gpr_log(GPR_INFO, "zstd: decompress stream error :%s", ZSTD_getErrorName(err));
        errorZDecompress(&pStream, count_before, length_before, output,
                         &outbuf);
      }
      if (outBuffer.pos < outBuffer.size) {
        break;
      }
    } while (true);

    if (inBuffer.pos != inBuffer.size) {
      gpr_log(GPR_INFO, "zstd: not all input consumed");
      errorZDecompress(&pStream, count_before, length_before, output, &outbuf);
      return 0;
    }
  }

  GPR_ASSERT(outbuf.refcount);
  outbuf.data.refcounted.length = outBuffer.pos;
  grpc_slice_buffer_add_indexed(output, outbuf);

  err = ZSTD_freeDStream(pStream);
  if (ZSTD_isError(err) > 0) {
    gpr_log(GPR_INFO, "zstd: decompress flush all error  :%s", ZSTD_getErrorName(err));
    errorZDecompress(nullptr, count_before, length_before, output, nullptr);
    return 0;
  }

  return 1;
}

static int copy(grpc_slice_buffer* input, grpc_slice_buffer* output) {
  size_t i;
  for (i = 0; i < input->count; i++) {
    grpc_slice_buffer_add(output, grpc_core::CSliceRef(input->slices[i]));
  }
  return 1;
}

static int compress_inner(grpc_compression_algorithm algorithm,
                          grpc_slice_buffer* input, grpc_slice_buffer* output) {
  switch (algorithm) {
    case GRPC_COMPRESS_NONE:
      // the fallback path always needs to be send uncompressed: we simply
      // rely on that here
      return 0;
    case GRPC_COMPRESS_DEFLATE:
      return zlib_compress(input, output, 0);
    case GRPC_COMPRESS_GZIP:
      return zlib_compress(input, output, 1);
    case GRPC_COMPRESS_ZSTD:
      return zstd_compress(input, output);
    case GRPC_COMPRESS_ALGORITHMS_COUNT:
      break;
  }
  gpr_log(GPR_ERROR, "invalid compression algorithm %d", algorithm);
  return 0;
}

int grpc_msg_compress(grpc_compression_algorithm algorithm,
                      grpc_slice_buffer* input, grpc_slice_buffer* output) {
  if (!compress_inner(algorithm, input, output)) {
    copy(input, output);
    return 0;
  }
  return 1;
}

int grpc_msg_decompress(grpc_compression_algorithm algorithm,
                        grpc_slice_buffer* input, grpc_slice_buffer* output) {
  switch (algorithm) {
    case GRPC_COMPRESS_NONE:
      return copy(input, output);
    case GRPC_COMPRESS_DEFLATE:
      return zlib_decompress(input, output, 0);
    case GRPC_COMPRESS_GZIP:
      return zlib_decompress(input, output, 1);
    case GRPC_COMPRESS_ZSTD:
      return zstd_decompress(input, output);
    case GRPC_COMPRESS_ALGORITHMS_COUNT:
      break;
  }
  gpr_log(GPR_ERROR, "invalid compression algorithm %d", algorithm);
  return 0;
}
