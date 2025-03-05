// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    // 计算当前 Block 中剩余的空间
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    // 如果剩余的空间小于头部的大小，则需要切换到新的 Block 中
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        // 填充当前 Block 中剩余的空间
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    // 计算出当前 Block 可以容纳的数据长度
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    // 计算需要写入的数据长度，如果本次需要写入的数据长度小于 avail，则取这次需要写入的长度，反正直接用 avail
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    // 如果这次需要写入的长度等于 slice 的长度，则说明这是一个完整的记录
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);
    // 更新 ptr 和 left，ptr 的剩余需要写入的数据位点，left 是还需要写入的数据长度
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

// 将单条物理记录写到日志中
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];
  // 设置头部字段，length 以及记录的类型
  // 头部字段一共是 7 个字节，前 4 个字节是 crc 校验，第 5 个到第 6 个是存储 length 长度， 最后一个字节是记录的类型
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  // 将 crc 添加到头部中的前4个字节中
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  // 将头部字段写到 dest_ 中
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    // 将数据部分写到 dest_ 中
    s = dest_->Append(Slice(ptr, length));
    if (s.ok()) {
      // 将数据刷到磁盘上
      s = dest_->Flush();
    }
  }
  // 更新当前 Block 的偏移量
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
