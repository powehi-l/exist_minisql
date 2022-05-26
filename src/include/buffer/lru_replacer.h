#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>
#include <mutex>
#include <unordered_set>
/**增加unordered_map对应库*/
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

private:
//  // add your own private member variables here
 using ListIterator = typename std::list<frame_id_t>::const_iterator;
 /** number of pages in buffer pool */
 size_t _num_pages;
 /** lrulist */
 std::list<frame_id_t> lrulist;
 /** lrumap */
 //std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> lrumap;
 std::unordered_map<frame_id_t,ListIterator> lrumap;

};

#endif  // MINISQL_LRU_REPLACER_H
