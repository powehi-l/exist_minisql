#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator() {

}

TableIterator::TableIterator(const TableIterator &other): table_heap_(other.table_heap_), row_(new Row(*other.row_)){

}

TableIterator::TableIterator(TableHeap *table_heap, RowId rid): table_heap_(table_heap), row_(new Row(rid)) {
    if (rid.GetPageId() != INVALID_PAGE_ID)
        table_heap_->GetTuple(row_, nullptr);
}

TableIterator::~TableIterator() {
    delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
    return row_->GetRowId() == itr.row_->GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
    return !(*this == itr);
}

Row &TableIterator::operator*() {
    return *(this->row_);
}

TableIterator& TableIterator::operator=(const TableIterator &other){
    table_heap_ = other.table_heap_;
    row_ = other.row_;
    return *this;
}

Row *TableIterator::operator->() {
    return row_;
}

TableIterator &TableIterator::operator++() {
    BufferPoolManager *buffer_pool_manager = table_heap_->buffer_pool_manager_;
    TablePage * item_page = (TablePage *)buffer_pool_manager->FetchPage(row_->GetRowId().GetPageId());
    RowId next_row_id;
    if(!item_page->GetNextTupleRid(row_->GetRowId(),&next_row_id))
        while(item_page->GetNextPageId() != INVALID_PAGE_ID){
            TablePage * next_page=(TablePage *)buffer_pool_manager->FetchPage(item_page->GetNextPageId());
            buffer_pool_manager->UnpinPage(item_page->GetTablePageId(),false);
            item_page = next_page;
            if(item_page->GetFirstTupleRid(&next_row_id))
                break;
        }
    row_->SetRowId(next_row_id);
    if (*this != table_heap_->End())
        table_heap_->GetTuple(row_, nullptr);
    buffer_pool_manager->UnpinPage(item_page->GetTablePageId(), false);
    return *this;
}

TableIterator TableIterator::operator++(int) {
    TableIterator pre(*this);
    ++(*this);
    return pre;
}
