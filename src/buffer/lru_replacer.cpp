#include "buffer/lru_replacer.h"


LRUReplacer::LRUReplacer(size_t num_pages):_num_pages(num_pages){
  lrumap.reserve(this->_num_pages);
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(Size()==0){
    return false;
  }
  *frame_id = lrulist.back();
  lrumap.erase(*frame_id);
  lrulist.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  /**避免重复pin一个页面（找不到该page）*/
  if(lrumap.find(frame_id) == lrumap.end()){return;}
  lrulist.erase(lrumap[frame_id]);
  lrumap.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  /**避免重复unpin（已有该page）*/
  if(lrumap.count(frame_id)!=0){
    return;
  }
  lrulist.push_front(frame_id);
  lrumap.emplace(frame_id,lrulist.begin());
}

size_t LRUReplacer::Size() {
  /**返回lrumap的大小作为现在的size*/
  return lrumap.size();
}