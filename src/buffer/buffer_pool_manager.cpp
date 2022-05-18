#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    Page *p = nullptr;
    frame_id_t frame_id = -1;
    if (page_table_.find(page_id) != page_table_.end()){
        frame_id = page_table_[page_id];
        p = &pages_[frame_id];
        p->pin_count_++;
        replacer_->Pin(frame_id);
        return p;
    }
    if (!(free_list_.empty() && replacer_->Size() == 0)){
        if (!free_list_.empty()){
            frame_id = free_list_.front();
            free_list_.pop_front();
        }
        if (replacer_->Size() > 0) {
            replacer_->Victim(&frame_id);
        }
        p = &pages_[frame_id];
        if (p->IsDirty()){
            disk_manager_->WritePage(p->page_id_, p->GetData());
            p->is_dirty_ = false;
        }
        page_table_.erase(p->page_id_);
        if (page_id != INVALID_PAGE_ID)
            page_table_.emplace(page_id, frame_id);
        p->ResetMemory();
        p->page_id_ = page_id;
        disk_manager_->ReadPage(page_id, p->GetData());
        p->pin_count_++;
        replacer_->Pin(frame_id);
        return p;
    }
    return p;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
    frame_id_t frame_id = -1;
    Page *p = nullptr;
    if (!(free_list_.empty() && replacer_->Size() == 0)) {
        if (!free_list_.empty()){
            frame_id = free_list_.front();
            free_list_.pop_front();
        }
        if (replacer_->Size() > 0) {
            replacer_->Victim(&frame_id);
        }
        p = &pages_[frame_id];
        page_id = disk_manager_->AllocatePage();
        if (p->IsDirty()){
            disk_manager_->WritePage(p->page_id_, p->GetData());
            p->is_dirty_ = false;
        }
        page_table_.erase(p->page_id_);
        if (page_id != INVALID_PAGE_ID)
            page_table_.emplace(page_id, frame_id);
        p->ResetMemory();
        p->page_id_ = page_id;
        disk_manager_->ReadPage(page_id, p->GetData());
        p->pin_count_++;
        replacer_->Pin(frame_id);
        return p;
    }
  return p;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
    frame_id_t frame_id = -1;
    Page *p = nullptr;
    if (page_table_.find(page_id) == page_table_.end())
        return true;
    frame_id = page_table_[page_id];
    p = &pages_[frame_id];
    if (p->pin_count_ > 0)
        return false;
    disk_manager_->DeAllocatePage(page_id);
    if (p->IsDirty()){
        disk_manager_->WritePage(p->page_id_, p->GetData());
        p->is_dirty_ = false;
    }
    page_table_.erase(p->page_id_);
    p->ResetMemory();
    p->page_id_ = INVALID_PAGE_ID;
    free_list_.push_back(frame_id);
    return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    frame_id_t frame_id = -1;
    Page *p = nullptr;
    if (page_table_.find(page_id) == page_table_.end())
        return false;
    frame_id = page_table_[page_id];
    p = &pages_[frame_id];
    if (p->pin_count_ <= 0)
        return false;
    p->pin_count_--;
    if (p->pin_count_ == 0)
        replacer_->Unpin(frame_id);
    p->is_dirty_ = is_dirty;
    return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
    frame_id_t frame_id = -1;
    Page *p = nullptr;
    if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end())
        return false;
    frame_id = page_table_[page_id];
    p = &pages_[frame_id];
    disk_manager_->WritePage(p->page_id_, p->GetData());
    p->is_dirty_ = false;
    return true;
}

void BufferPoolManager::FlushAllPages() {
    Page *p = nullptr;
    for (size_t i = 0; i < pool_size_; i++) {
        p = &pages_[i];
        if (p->page_id_ == INVALID_PAGE_ID || !p->IsDirty())
            continue;
        else {
            disk_manager_->WritePage(p->page_id_, p->GetData());
            p->is_dirty_ = false;
        }
    }
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}