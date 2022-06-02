#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator() {

}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_, int index_, BufferPoolManager* buffer_pool_manager_)
  :index(index_), leaf(leaf_), buffer_pool_manager(buffer_pool_manager_){}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  if(leaf != nullptr)
    buffer_pool_manager->UnpinPage(leaf->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  return leaf->GetItem(index);
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  index++;
  if(index >= leaf->GetSize()){
    page_id_t next_page_id = leaf->GetNextPageId();

    if(next_page_id == INVALID_PAGE_ID)
      leaf = nullptr;
    else{
      Page* page = buffer_pool_manager->FetchPage(next_page_id);
      leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
      index = 0;
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return this->leaf == itr.leaf;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  return this->leaf != itr.leaf;
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
