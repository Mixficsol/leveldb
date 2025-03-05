// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

// 往文件中添加 key-value 对
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  // 如果之前有一个 entry 了，需要做一下 Key 的判断
  if (r->num_entries > 0) {
    // 确保当前的 Key 大于之前最后一个插入的 Key
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }


  // 写完一个 DataBlock 落盘后需要写 index_block 的 entry
  if (r->pending_index_entry) {
    // 确保 data_block 是空的
    assert(r->data_block.empty());
    // 从上一个 DataBlock 中获取一个最大的 Key，和当前的 Key 生成一个最短的 Key
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;

    // 往这个 Index-block 中添加一个条目, 这里记录了上一个 Data-block 的偏移量和大小，作为 Index-Block 中的一个条目
    // 注意第一次的时候 offset 是 0，size 是 datablock 的大小，这个大小不包括 trailer (type+crc) 的大小
    r->pending_handle.EncodeTo(&handle_encoding);
    // 往 Index-block 中写入数据
    // 往 Index-block 中写入源数据以及对应的偏移量和大小，Key 是上一个 Data-block 的最大 Key，Value 是上一个 Data-block 的偏移量和大小
    // 这个的 Key 是上一个 Data-block 的最大 Key，Value 是上一个 Data-block 的偏移量和大小 (用 Slice 存储)
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  // 如果在 Option 设置中打开了布隆过滤器，则调用 AddKey 加入到 filter_block 中
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  // 替换最后一个 Key 的值以及更新条目
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  // 第一步先开始往 Data-block 中添加数据
  r->data_block.Add(key, value);

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  // 判断当前的这个 block 是否已经达到了 block_size 的大小，如果达到了就 Flush，默认配置是 4KB
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  // 判断 data_block 是否为空
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    // 等待刷盘
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

// 将一个 Block 写入到文件中
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  // 选择压缩算法对一个 data-block 进行压缩
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }

    case kZstdCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Zstd_Compress(r->options.zstd_compression_level, raw.data(),
                              raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Zstd not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  // 将 buffer, 重启点等信息清空
  block->Reset();
}

// 将一个 Raw-Block 的内容落盘到文件中
// 这里说的 Raw-Block 可以表示任何 Block, 不仅仅是 Data-Block
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  // 一开始的时候这个 offset 是 0
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  // 先往文件里面追加 block_contents
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    // 计算出 crc 校验码
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    // 将 crc 校验码写入到 trailer 中
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    // 再往文件中追加 trailer
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      // 更新 offset
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

// 如果一个 immutable 中所有的数据都被写到 Data-Block 中并且落盘的话，那么就需要调用 Finish
// Finish 会将 filter block, metaindex block, index block, footer 写入到文件中
Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  // 这里的 handle 主要用于后续 footer 的写入，主要记载了每个 block 的 offset 和 size
  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    // 将布隆过滤器写入到文件中, 这里 filter_block_handle 记录了 filter block 的 offset 和 size
    // 这里的 offset 实际上就是最后的 Data-Block 的 offset, size 就是 filter-block 的大小
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  // metaindex 用于记录 filter block 的位置
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      // 这里是将 filter block 的名字写入到 meta_index_block 中
      // 为 filter.leveldb.BuiltinBloomFilter2
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      // 记录了最后一次 Data-Block 写入的大小和 offset
      // 也就是 filter block 的起始点位和 filter block 的大小
      filter_block_handle.EncodeTo(&handle_encoding);
      // 将 filter block 的名字和 handle 写入到 meta_index_block 中
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    // 将 meta_index_block 写入到文件中，后面追加了 crc 校验码以及 type
    // meta_index_block 只有一条 entry, 用来记录 filter block 的位置和名字
    // 所以 MetaIndex Block 主要是记录了 filter block 的起始位置，大小和名字
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  // 补全最后一个写 DataBlock 的 IndexBlock 条目
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
