#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(0);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(0 <= index && index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(0 <= index && index < GetSize());
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for(int i = 0; i < GetSize(); i++)
    if(array_[i].second == value)
      return i;
  return INVALID_PAGE_ID;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  // replace with your own code
//  assert(0 <= index && index < GetSize());
  return array_[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // replace with your own code
//  for(int i = 1; i < GetSize(); i++){
//    if(comparator(key,array_[i].first) < 0)
//      return array_[i-1].second;
//  }
//  return array_[GetSize()-1].second;
  int start = 1;
  int end = GetSize() - 1;

  // find the first key > key
  while (start <= end) {
    int mid = (end - start) / 2 + start;
    if (comparator(array_[mid].first, key) <= 0) {
      start = mid + 1;
    } else {
      end = mid - 1;
    }
  }

  // the last <= key
  return array_[start - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array_[0].second = old_value;
  array_[1] = {new_key, new_value};
  IncreaseSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  for(int i = GetSize(); i > 0; i--){
    if(array_[i-1].second == old_value){
      array_[i] = {new_key, new_value};
      IncreaseSize(1);
      break;
    }
    array_[i] = array_[i-1];
  }
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  uint32_t half = (GetSize() + 1) / 2;
  recipient->CopyNFrom(array_ + GetSize() - half, half, buffer_pool_manager);
  SetSize(GetSize() - half);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  for(int i = 0; i < size; i++){
    array_[i + GetSize()] = *items++;
    auto *page = buffer_pool_manager->FetchPage(array_[i + GetSize()].second);
    if(page != nullptr){
      auto *child = reinterpret_cast<BPlusTreePage*>(page->GetData());
      child->SetParentPageId(GetPageId());
      buffer_pool_manager->UnpinPage(page->GetPageId(), true);
    }
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
    for(int i = index; i < GetSize()-1; i++){
      array_[i] = array_[i+1];
    }
    IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // replace with your own code
  SetSize(0);
  return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  int start = recipient->GetSize();
  for(int i = 0; i < GetSize(); i++){
    recipient->array_[i + start] = array_[i];

    auto* page = buffer_pool_manager->FetchPage(array_[i].second);
    if(page == nullptr)
      ASSERT(false, "fail to fetch page");
    auto* child_node = reinterpret_cast<BPlusTreePage*>(page->GetData());
    child_node->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child_node->GetPageId(), true);
  }
  recipient->array_[start].first = middle_key;
  recipient->IncreaseSize(GetSize());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
    MappingType pair = array_[0];
    for(int i = 0; i < GetSize()-1; i++)
      array_[i] = array_[i+1];
    IncreaseSize(-1);

    recipient->CopyLastFrom(pair, buffer_pool_manager);

    auto *page = buffer_pool_manager->FetchPage(pair.second);
    if(page == nullptr)
      ASSERT(false, "fail to fetch child page");
    auto* child_node = reinterpret_cast<BPlusTreePage*>(page->GetData());
    child_node->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child_node->GetPageId(), true);

//    auto* parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
//    if(parent_page == nullptr)
//      ASSERT(false, "fail to fetch parent page");
//    auto* parent_node = reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
//    parent_node->SetKeyAt(parent_node->ValueIndex(GetPageId()), array_[0].first);
//    buffer_pool_manager->UnpinPage(parent_page->GetPageId(), true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  array_[GetSize()] = pair;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFirstFrom(array_[GetSize()-1], buffer_pool_manager);
  IncreaseSize(-1);
//  recipient->SetKeyAt(0, middle_key);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  for(int i = GetSize()-1; i >= 0; i--)
    array_[i+1] = array_[i];
  array_[0] = pair;
  IncreaseSize(1);

  auto* page = buffer_pool_manager->FetchPage(array_[0].second);
  if(page != nullptr){
    auto* child = reinterpret_cast<BPlusTreePage*>(page->GetData());
    child->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  }

//  auto parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
//  if(parent_page == nullptr)
//    ASSERT(false, "fail to fetch parent page");
//  auto parent_node = reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
//  parent_node->SetKeyAt(parent_node->ValueIndex(GetPageId()), array_[0].first);
//  buffer_pool_manager->UnpinPage(parent_node->GetPageId(), true);
}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;