// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

// 负责打开一个 SSTable 文件并为后续的查询操作做准备
Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  // 如果文件大小小于页脚的大小，那么就不是一个 SST 文件
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  // 读取页脚
  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  // 将 footer 的内容读取到 footer_input 中
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  // 解析页脚
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents index_block_contents;
  ReadOptions opt;
  // 如果开启了 paranoid_checks，那么就需要校验 checksum
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }

  // 读取 index block, 将文件内容读取到 index_block_contents 中
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    // 创建一个 Block 对象来存储 index_block 的内容。
    Block* index_block = new Block(index_block_contents);
    // 创建一个 Rep 对象, 将元信息保存到 Rep 中
    // Rep 类是 Table 类的一个内部类，用于保存 Table 的元信息
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    // 如果开启了 block_cache，那么就为这个 Table 创建一个 cache_id
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    // 创建一个 Table 对象, 并将 rep 保存到 Table 中
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

// 通过读取 meta_index_block 的内容，提取出 filter block 的内容
void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  // 将 meta_index_block 的内容读取到 contents 中
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  // 创建一个 Meta-Block 对象来存储 meta_index_block 的内容
  Block* meta = new Block(contents);

  // 创建一个 block 的迭代器
  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  // 查找 filter block 的位置
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    // iter->value() 就是 filter 的起始点位和大小，并继续解析
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

// 读取 filter block 的内容
void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  // 将 filter block 的内容提取到 block 中
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  // 为 filter block 创建一个 FilterBlockReader 对象，用于后续的查询操作
  // 相当于在 table-cache 中保存了一个 filter block
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  // 获取 block_cache
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  // input 存储了 Data-Block 的偏移量和大小
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      // 将 cache_id 和 Data-Block 的偏移量存储到 cache_key_buffer 中
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      // 创建一个 Slice 对象 key, key 是由 cache_id 和 Data-Block 的偏移量组成的
      Slice key(cache_key_buffer, sizeof(cache_key_buffer))
      // 在 block_cache 中查找是否有对应的 Data-Block，这里的 Key 是由 cache_id 和 Data-Block 的偏移量组成的;
      cache_handle = block_cache->Lookup(key);
      // 如果找到了，那么就返回这个 Data-Block
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        // 如果没有找到，那么就从 table-cache 读中读取文件找到 Data-Block
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          // 将 Block 里面的内容放到 Block 对象中
          block = new Block(contents);
          // 将 Block 插入到 block_cache 中
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      // 如果 Block-cache 是空的，说明没有开启 Block-Cache 那么就从 table-cache 读中读取文件找到 Data-Block
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    // 为 Block 分配一个迭代器
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    // 如果没有找到的话，返回一个空的迭代器
    iter = NewErrorIterator(s);
  }
  return iter;
}


Iterator* Table::NewIterator(const ReadOptions& options) const {
  // 返回一个用于读取 Index-Block 的迭代器
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  // 获取一个可以读取 Index-Block 的迭代器
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    // 如果连布隆过滤器都没有，那么就直接跳过 Index-Block 所记载的 Data-Block
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      // 否则的话说明这个 Key 可能存在于当前这个 Data-Block 中
      // 这里的 iiter->key() 就是 Index-Block 中的 value
      // 返回一个可以读取 Data-Block 的迭代器
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
