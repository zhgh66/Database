#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t victim_id = -1;
  size_t max_backward_distance = std::numeric_limits<size_t>::min();  // 初始化为最小值
  size_t latest_access_time = std::numeric_limits<size_t>::max();      // 初始化为最大值

  // std::cout << "Starting eviction process..." << std::endl;

  bool found_candidate_with_less_than_k_accesses = false;
  
  // 1. 遍历所有可驱逐的页面，选择回退距离最大或最久未访问的页面
  for (const auto &entry : access_history_) {
    frame_id_t id = entry.first;

    // 跳过不可驱逐的页面
    if (!evictable_[id]) {
      continue;
    }

    const std::vector<size_t> &history = entry.second;
    size_t backward_distance;

    // 2. 判断访问历史小于 K 次的页面
    if (history.size() < k_) {
      // 如果访问历史小于 K 次，则使用 LRU 规则（比较第一次被访问的时间戳）
      found_candidate_with_less_than_k_accesses = true;
      backward_distance = history[0];  // 使用第一次访问的时间戳进行 LRU 计算
    } else {
      // 计算 K 次回退距离
      backward_distance = current_timestamp_ - history[history.size() - k_];
    }

    // Debug: 打印每个页面的信息
    // std::cout << "Frame: " << id 
    //           << ", History Size: " << history.size() 
    //           << ", Backward Distance: " << backward_distance 
    //           << ", Latest Access Time: " << latest_access_time 
    //           << ", Last Access Timestamp: " << history.back() << std::endl;

    // 3. 选择回退距离最大或回退距离相等时按访问时间选择的页面
    if (found_candidate_with_less_than_k_accesses) {
      // 如果存在被引用次数少于 K 次的页面，按照传统 LRU 方式选择
      if (backward_distance < latest_access_time) {
        latest_access_time = backward_distance;
        victim_id = id;
        // std::cout << "Selected Frame for LRU: " << victim_id << " (backward_distance: " << backward_distance << ")" << std::endl;
      }
    } else {
      // 如果所有页面都被引用 K 次或更多次，按照回退距离选择
      if (backward_distance > max_backward_distance || 
          (backward_distance == max_backward_distance && history.back() < latest_access_time)) {
        max_backward_distance = backward_distance;
        victim_id = id;
        latest_access_time = history.back();
        // std::cout << "Selected Frame: " << victim_id << " (backward_distance: " << max_backward_distance 
        //           << ", latest_access_time: " << latest_access_time << ")" << std::endl;
      }
    }
  }

  // 4. 如果没有找到合适的驱逐页面，返回 false
  if (victim_id == -1) {
    // std::cout << "No victim found for eviction." << std::endl;  
    return false;
  }

  // 5. 驱逐选择的页面
  *frame_id = victim_id;
  // std::cout << "Evicting Frame: " << victim_id << std::endl;

  // 驱逐后更新状态：从访问历史中删除，设置不可驱逐
  access_history_.erase(victim_id);
  evictable_[victim_id] = false;
  curr_size_--;

  return true;
}


void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);

  // Check if the frame exists in access history
  if (access_history_.find(frame_id) == access_history_.end()) {
    access_history_[frame_id] = {};
  }

  // Record the current timestamp in the frame's access history
  access_history_[frame_id].push_back(current_timestamp_);

  // Increment the timestamp
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);

  // 只有当页面仍然存在并且未被删除时才进行设置
  if (access_history_.find(frame_id) == access_history_.end()) {
    // std::cout << "Frame " << frame_id << " is already removed, skipping SetEvictable." << std::endl;
    return;  // 页面已经被删除，跳过
  }

  // std::cout << "Setting evictable for frame " << frame_id << " to " << set_evictable << std::endl;
  
  if (evictable_[frame_id] != set_evictable) {
    evictable_[frame_id] = set_evictable;
    if (set_evictable) {
      curr_size_++;  // 页面设置为可驱逐时，增加计数
    } else {
      curr_size_--;  // 页面设置为不可驱逐时，减少计数
    }
  }
  
  // 调试输出 evictable_ 和 current size
  // std::cout << "Evictable for frame " << frame_id << ": " << evictable_[frame_id] << std::endl;
  // std::cout << "Current size after SetEvictable: " << curr_size_ << std::endl;
}





void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  
  // 只在页面存在且是可驱逐时进行删除
  if (access_history_.find(frame_id) != access_history_.end() && evictable_[frame_id]) {
    //std::cout << "Removing frame " << frame_id << std::endl;
    access_history_.erase(frame_id);  // 删除页面的访问历史记录
    evictable_[frame_id] = false;  // 设置页面为不可驱逐
    curr_size_--;  // 调整当前可驱逐页面的数量
  }
  
  // 调试输出删除后的状态
  // std::cout << "Evictable after remove: " << evictable_[frame_id] << std::endl;
  // std::cout << "Access history size after remove: " << access_history_.size() << std::endl;
  // std::cout << "Current size after remove: " << curr_size_ << std::endl;
}





auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  
  size_t count = 0;
  for (const auto& entry : evictable_) {
    if (entry.second) {
      count++;
    }
  }

  // std::cout << "Current size: " << count << std::endl;  // 调试输出
  return count;
}






}  // namespace bustub
