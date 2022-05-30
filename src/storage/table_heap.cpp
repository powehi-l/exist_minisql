#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
    if (row.GetSerializedSize(schema_) > TablePage::SIZE_MAX_ROW)
        return false;
    page_id_t item_page_id = first_page_id_;
    TablePage *item_page = (TablePage *)buffer_pool_manager_->FetchPage(item_page_id);
    buffer_pool_manager_->UnpinPage(item_page_id, false);
    while (true) {
        bool flag = item_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
        if(flag){
            buffer_pool_manager_->UnpinPage(item_page_id, true);
            return true;
        }
        if (item_page->GetNextPageId() == INVALID_PAGE_ID)
            break;
        item_page_id = item_page->GetNextPageId();
        item_page = (TablePage *)buffer_pool_manager_->FetchPage(item_page_id);
        buffer_pool_manager_->UnpinPage(item_page_id, false);
    }
    buffer_pool_manager_->UnpinPage(item_page_id, true);
    page_id_t new_page_id = INVALID_PAGE_ID;
    TablePage *new_page = (TablePage *)buffer_pool_manager_->NewPage(new_page_id);
    buffer_pool_manager_->UnpinPage(new_page_id,false);
    if (!new_page)
        return false;
    new_page->Init(new_page_id, item_page_id, log_manager_, txn);
    new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    buffer_pool_manager_->UnpinPage(new_page_id,true);
    item_page->SetNextPageId(new_page_id);
    buffer_pool_manager_->UnpinPage(item_page_id,true);
    return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
    TablePage *page = (TablePage *)buffer_pool_manager_->FetchPage(rid.GetPageId());
    if(page == nullptr)
        return false;
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    Row old_row(rid);
    bool flag = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
    if(flag){
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
        return true;
    }
    else{
        MarkDelete(rid, txn);
        flag = InsertTuple(row, txn);
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), flag);
        return flag;
    }
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
    TablePage *page = (TablePage *)buffer_pool_manager_->FetchPage(rid.GetPageId());
    page->ApplyDelete(rid, txn, log_manager_);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(),true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
    TablePage *item_page = (TablePage *)buffer_pool_manager_->FetchPage(first_page_id_);
    page_id_t next_page_id = item_page->GetNextPageId();
    buffer_pool_manager_->DeletePage(item_page->GetPageId());
    while(next_page_id != INVALID_PAGE_ID){
        item_page = (TablePage *)buffer_pool_manager_->FetchPage(next_page_id);
        next_page_id = item_page->GetNextPageId();
        buffer_pool_manager_->DeletePage(item_page->GetPageId());
    }
    return;
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
    TablePage *tpage = (TablePage *)buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId());
    if(!tpage)
        return false;
    else{
        bool flag;
        flag = tpage->GetTuple(row,schema_,txn,lock_manager_);
        buffer_pool_manager_->UnpinPage(tpage->GetTablePageId(), false);
        return flag;
    }
}

TableIterator TableHeap::Begin(Transaction *txn) {
    RowId rid;
    page_id_t page_id = first_page_id_;
    while(page_id != INVALID_PAGE_ID){
        TablePage * page = (TablePage *)buffer_pool_manager_->FetchPage(page_id);
        bool flag = page->GetFirstTupleRid(&rid);
        buffer_pool_manager_->UnpinPage(page_id, false);
        if(flag)
            break;
        page_id = page->GetNextPageId();
    }
    return TableIterator(this, rid);
}

TableIterator TableHeap::End() {
    return TableIterator(this, RowId(INVALID_ROWID));
}
