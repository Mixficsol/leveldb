// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() { delete cache_; }

// 在 TableCache 中查找是否有对应文件的缓存 handle
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  // 将 file_number 编码到 buf 中, 固定长度为 8 个字节
  EncodeFixed64(buf, file_number);
  // 将 file_number 作为 Key 查找对应的缓存 handle
  Slice key(buf, sizeof(buf));
  // 在 table-cache 中查找是否有对应的这个文件的缓存 handle
  *handle = cache_->Lookup(key);
  // 如果没有找到对应的缓存 handle，那么就需要创建一个新的 TableAndFile 对象
  if (*handle == nullptr) {
    // 根据数据库名称和文件编号生成文件名
    // 新版本的 sst 文件名，ldb 结尾
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    // 打开文件
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      // 早期版本的 sst 文件名，sst 结尾
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      // 如果打开文件失败，那么就尝试打开老的 SST 文件
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    // 如果文件打开成功，那么就将 sst 文件里面的内容 (filter-block, index-block) 和一些元信息加载到 Table 对象中
    if (s.ok()) {
      s = Table::Open(options_, file, file_size, &table);
    }

    // 如果加载到 Table 对象中不成功，则释放 file
    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      // 如果数据加载到 Table 成功 ，那么就将 Table 和 File 封装成 TableAndFile 对象，在 table-cache 中插入这个对象
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      // DeleteEntry 是一个清理函数，当缓存条目不再使用时会被调用来释放资源
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

// 功能是创建一个新的 Iterator，并且在此过程中确保正确加载 Table 数据
Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  // 在 TableCache 中查找是否有对应的这个文件的缓存 handle
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  // 从 TableCache 中缓存 handle 中获取 Table 对象
  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  // 更新 Table 中的内容
  Iterator* result = table->NewIterator(options);
  // RegisterCleanup 用于在迭代器不再使用时释放资源
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, handle_result);
    cache_->Release(handle);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
