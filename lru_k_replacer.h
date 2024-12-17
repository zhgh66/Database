//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>
#include<iostream>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   * Constructor for LRUKReplacer.
   *
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   * @param k the history length for LRU-K
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * Destroys the LRUKReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * Find the frame with largest backward k-distance and evict that frame.
   * Only frames that are marked as 'evictable' are candidates for eviction.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * Toggle whether a frame is evictable or non-evictable.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * Remove an evictable frame from replacer, along with its access history.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  // Frame access history: stores the timestamps of access for each frame
  std::unordered_map<frame_id_t, std::vector<size_t>> access_history_;
  
  // Track evictable status for each frame
  std::unordered_map<frame_id_t, bool> evictable_;

  // Maximum number of frames and the value of k for LRU-K
  size_t replacer_size_;
  size_t k_;

  // Current time step (or timestamp)
  size_t current_timestamp_{0};

  // Size of the replacer (number of evictable frames)
  size_t curr_size_{0};

  // Mutex for thread-safety
  std::mutex latch_;
};

}  // namespace bustub
