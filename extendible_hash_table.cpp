#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t initial_bucket_size)
    : global_depth_(0), bucket_size_(initial_bucket_size) {
  // 初始化目录，至少包含一个桶
  dir_.push_back(std::make_shared<Bucket>(bucket_size_, 0));
}
//初始化全局深度为0，桶的大小为 initial_bucket_size。dir_ 是一个目录，最初包含一个桶。

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  size_t index = std::hash<K>()(key) & mask;
  return index;
}
//计算给定键 key 在目录中的索引，使用全局深度作为掩码

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_); 
  //使用 std::scoped_lock 对 latch_ 进行加锁，确保在多线程环境下对全局深度的安全访问。
  int depth = GetGlobalDepthInternal();
  return depth;
}
//获取全局深度（global_depth_）

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}
//内部方法，直接返回当前的全局深度 global_depth_。

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);  
  int depth = GetLocalDepthInternal(dir_index);
  return depth;
}
//获取指定目录索引 dir_index 对应桶的局部深度

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  size_t index = static_cast<size_t>(dir_index);  
  auto bucket = dir_[index];//dir_ 是一个容器索引通常是 size_t 类型
  int depth = bucket->GetDepth();
  return depth;
}
//内部方法，获取指定目录索引的桶的局部深度。

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);  
  int num_buckets = GetNumBucketsInternal();
  return num_buckets;
}
//获取当前哈希表中的桶数量。

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}
//内部方法，直接返回当前的桶数量 num_buckets_。

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
    std::scoped_lock<std::mutex> locker(latch_);
    return dir_.at(IndexOf(key))->Find(key, value);
}
//在相应的桶中查找键 key，如果找到则返回 true 并通过引用 value 返回对应的值。

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
    std::scoped_lock<std::mutex> locker(latch_);
    V find_value;
    if (!dir_.at(IndexOf(key))->Find(key, find_value)) {
        return false;
    }
    dir_.at(IndexOf(key))->Remove(key);
    return true;
}
//先查找键，如果存在则从桶中删除并返回 true。

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
    std::scoped_lock<std::mutex> locker(latch_);  

    while (true) {//该循环允许在插入过程中处理可能的桶分裂，直到成功插入数据为止
        size_t index = IndexOf(key);
        auto bucket = dir_.at(index);
        // 通过 IndexOf(key) 计算该键的索引，以确定对应的桶。
        //bucket 是指向目录中该索引所指向的桶的智能指针。
        
        // 尝试插入，如果成功，返回
        if (bucket->Insert(key, value)) {
            return;
        }

        // 如果当前桶已满，则进行桶分裂
        if (bucket->GetDepth() == global_depth_) {
            int primary_dir_len = dir_.size();  // 扩展前的目录长度

            // 增加全局深度
            global_depth_++;

            // 新扩展的shared_ptr依次指向原来的桶
            //primary_dir_len 是扩展前的目录长度，表示当前 dir_ 中桶的数量。
            for (int i = 0; i < primary_dir_len; i++) {
                dir_.emplace_back(dir_.at(i));
            }
        }

        // 增加当前桶的局部深度
        bucket->IncrementDepth();

        // 桶分裂
        int local_mask = (1 << bucket->GetDepth()) - 1;
        size_t origin_index = index & local_mask;  // 原始桶目录下标
        size_t divide_index = (origin_index ^ (~local_mask >> 1)) & local_mask;  // 分裂桶目录下标
        std::shared_ptr<Bucket> origin_bucket = bucket;  // 指向原始桶
        std::shared_ptr<Bucket> divide_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());  // 指向分裂桶
        num_buckets_++;//增加总桶的数量

        // 数据分裂
        auto origin_items = origin_bucket->GetItems();  // 单独提取以避免迭代器失效，从原始桶中提取所有键值对。
        //遍历原始桶的项，如果某个项的索引不再属于原始桶（即应该在新桶中），则将其从原始桶中删除并插入到新分裂的桶中。
        for (const auto &[k, v] : origin_items) {
            if ((IndexOf(k) & local_mask) != origin_index) {
                origin_bucket->Remove(k);
                divide_bucket->Insert(k, v);
            }
        }

        // 目录重映射，遍历目录，更新桶的指针
        for (size_t dir_index = 0; dir_index < dir_.size(); dir_index++) {
            if ((dir_index & local_mask) == origin_index) {
                dir_.at(dir_index) = origin_bucket;
                //如果目录下标与 origin_index 匹配，那么该位置的桶应该指向原始桶 origin_bucket。
            } else if ((dir_index & local_mask) == divide_index) {
                dir_.at(dir_index) = divide_bucket;
                //如果目录下标与 divide_index 匹配，那么该位置的桶应该指向新分裂的桶 divide_bucket。
            }
        }
    }
}




//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &item : list_) {
    if (item.first == key) { 
      value = item.second;    
      return true;
    }
  }
  return false;
}
//在桶中查找给定的键 key，如果找到，则将对应的值赋给 value，并返回 true；如果未找到，则返回 false。

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = list_.begin();
  while (it != list_.end()) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
    ++it;
  }
  return false;  
}
//从桶中删除指定的键 key，如果成功删除，返回 true；如果未找到该键，则返回 false。

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &pair : list_) {
    if (pair.first == key) {
      pair.second = value;
      return true;  
    }
  }
  if (IsFull()) {
    return false;  
  }
  list_.emplace_back(key, value);
  return true;
}
//向桶中插入一个键值对。如果键已存在，则更新其值；如果桶已满，返回 false。

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub