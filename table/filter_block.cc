// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg; // 一个 Filter 作用于 2048 字节的数据块

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase); // 计算当前 block_offset 对应的 filter 的索引
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) { // 如果 filter 的索引超过当前 filter_offsets_ 的大小，那么就需要生成新的 filter
    GenerateFilter();
  }
}

// 将 key 添加到 keys_ 中，将 key 的起始位置添加到 start_ 中
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

// 将 fliter 中的数据写到 result_ 中
Slice FilterBlockBuilder::Finish() {
  // 如果 start_ 中还有数据的话，那么就需要生成一个新的 filter
  // 这里的 start_ 每次记录一个 DataBlock 中所有的 Key, 根据这个生成一个 Filter 之后自动清空
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    // 将 filter_offsets_ 中的数据追加写到 result_ 中，也就是每个 filter 的偏移量
    PutFixed32(&result_, filter_offsets_[i]);
  }

  // 将最后一个 filter 的偏移量追加写到 result_ 中
  PutFixed32(&result_, array_offset);
  // 将 kFilterBaseLg 写到 result_ 中, kFilterBaseLg 表示 Bloom 的 Hash 计算粒度 
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

// 生成一个 filter, 并将结果追加到 result_ 中
void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  // 将 keys_ 中的数据提取出来，放到 tmp_keys_ 中
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  // filter_offsets_ 记录了中每个 filter 的偏移量，最开始的时候这个偏移量是 0
  // filter 的实际内容是记录在 result_ 中
  filter_offsets_.push_back(result_.size());
  // 根据一组 Keys 生成一个 Filter, 将 filter 的数据内容追加到 result_ 中
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1];
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4;
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
