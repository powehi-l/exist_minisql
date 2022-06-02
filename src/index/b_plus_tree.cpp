#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          root_page_id_(INVALID_PAGE_ID),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {

}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  auto leaf_page = FindLeafPage(key, false);
  auto node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  bool flag = false;
  if(node != nullptr){
    ValueType value;
    if(node->Lookup(key, value, comparator_)){
      result.push_back(value);
      flag = true;
    }
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  }
  return flag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if(IsEmpty()){
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto *page = buffer_pool_manager_->NewPage(root_page_id_);
  if (page == nullptr) {
    ASSERT(false, "fail to fetch page");
  }
  auto root = reinterpret_cast<LeafPage *>(page->GetData());
  UpdateRootPageId(true);
  root->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  root->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
    auto* leaf_page = FindLeafPage(key, false);
    if(leaf_page == nullptr)
      ASSERT(false, "fail to fetch page");
    auto* leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
    ValueType temp;
    if(leaf->Lookup(key, temp, comparator_)){
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      return false;
    }
    else{
      leaf->Insert(key, value, comparator_);
      if(leaf->GetSize() == leaf->GetMaxSize() + 1){
        auto *new_leaf = Split<LeafPage>(leaf);
        new_leaf->SetNextPageId(leaf->GetNextPageId());
        leaf->SetNextPageId(new_leaf->GetPageId());
        InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf, transaction);
        buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
      }
      buffer_pool_manager_->UnpinPage((leaf_page->GetPageId()), true);
      return true;
    }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id;
  auto* new_page = buffer_pool_manager_->NewPage(new_page_id);
  if(new_page == nullptr)
    ASSERT(false, "fail to new a page when split");
  auto *new_node = reinterpret_cast<N*>(new_page->GetData());
  new_node->Init(new_page_id, node->GetParentPageId(), node->GetMaxSize());
  node->MoveHalfTo(new_node, buffer_pool_manager_);
//  if(node->IsRootPage()){
//    auto* new_node = reinterpret_cast<InternalPage *>(new_page->GetData());
//    new_node->Init(new_page_id, node->GetParentPageId(),internal_max_size_);
//    auto *inter_page = reinterpret_cast<InternalPage*>(node);
//    inter_page->MoveHalfTo(new_node, buffer_pool_manager_);
//  }
//  else{
//    auto* new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
//    new_node->Init(new_page_id, node->GetParentPageId(),leaf_max_size_);
//    auto *leaf_page = reinterpret_cast<LeafPage *>(node);
//    leaf_page->MoveHalfTo(new_node);
//  }
//  N *last_node = reinterpret_cast<N*>(new_page);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if(old_node->IsRootPage()){
    Page* new_page = buffer_pool_manager_->NewPage(root_page_id_);
    if(new_page == nullptr)
      ASSERT(false, "fail to new page when create root");
    auto *new_root = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId(1);
    buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    return;
  }
  else {
    page_id_t parent_id = old_node->GetParentPageId();
    auto *page = buffer_pool_manager_->FetchPage(parent_id);
    if (page == nullptr) ASSERT(false, "fail to fetch parent page when insert page into parent");
    auto* parent_page = reinterpret_cast<InternalPage*>(page->GetData());
    parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

    if(parent_page->GetSize() == parent_page->GetMaxSize()+1){
      auto *new_parent_page = Split<InternalPage>(parent_page);
      InsertIntoParent(parent_page, new_parent_page->KeyAt(0), new_parent_page, transaction);
      buffer_pool_manager_->UnpinPage(new_parent_page->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if(IsEmpty()) return;
  auto* leaf_page = reinterpret_cast<LeafPage*>(FindLeafPage(key, false));

  int cur_size = leaf_page->RemoveAndDeleteRecord(key, comparator_);
  auto* parent_page = buffer_pool_manager_->FetchPage(leaf_page->GetParentPageId());
  if(parent_page == nullptr)
    ASSERT(false, "fail to fetch parent page");
  auto* parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  parent_node->SetKeyAt(parent_node->ValueIndex(leaf_page->GetPageId()), leaf_page->KeyAt(0));
  if(cur_size < leaf_page->GetMinSize())
    CoalesceOrRedistribute(leaf_page, transaction);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if(node->IsRootPage()){
    return AdjustRoot(node);
  }
  auto* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto* parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());

  int index = parent_node->ValueIndex(node->GetPageId());
  int sibling = index - 1;
  if(index == 0)
    sibling = 1;
  auto* sibling_page = buffer_pool_manager_->FetchPage(parent_node->ValueAt(sibling));
  auto* sibling_node = reinterpret_cast<N*>(sibling_page->GetData());

  if(node->GetSize() + sibling_node->GetSize() <= node->GetMaxSize()){
    if(index == 0){
      N* temp = node;
      node = sibling_node;
      sibling_node = temp;
    }
    index = parent_node->ValueIndex(node->GetPageId());
    Coalesce(&sibling_node, &node, &parent_node, index, transaction);
  }
  else{
    Redistribute(sibling_node, node, index);
  }

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);

  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
    assert((*neighbor_node)->GetSize() + (*node)->GetSize() <= (*node)->GetMaxSize());
    (*node)->MoveAllTo(*neighbor_node, (*parent)->KeyAt(index), buffer_pool_manager_);
    (*parent)->Remove(index);
    buffer_pool_manager_->DeletePage((*node)->GetPageId());
    if((*parent)->GetSize() < (*parent)->GetMinSize()){
      return CoalesceOrRedistribute<InternalPage>(*parent, transaction);
    }
    return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if(page == nullptr)
    ASSERT(false, "fail to fetch parent page");
  auto *parent_node = reinterpret_cast<InternalPage*>(page->GetData());
  KeyType middle_key = parent_node->KeyAt(parent_node->ValueIndex(node->GetPageId()));
  if(index == 0){
    neighbor_node->MoveFirstToEndOf(node, middle_key, buffer_pool_manager_);
    parent_node->SetKeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
  }
  else{
    neighbor_node->MoveLastToFrontOf(node, middle_key, buffer_pool_manager_);
    parent_node->SetKeyAt(parent_node->ValueIndex(node->GetPageId()), node->KeyAt(0));
  }

  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
//  if(neighbor_node->IsLeafPage()){
//    auto* leaf_n = reinterpret_cast<LeafPage*>(neighbor_node);
//    if(index == 0)
//      neighbor_node->MoveFirstToEndOf(node);
//    else
//      neighbor_node->MoveLastToFrontOf(node);
//  }
//  else{
//    auto* inter_n = reinterpret_cast<InternalPage *>(neighbor_node);
//    auto* inter_c = reinterpret_cast<InternalPage*>(node);
//    if(index == 0)
//      inter_n->MoveFirstToEndOf(inter_c,inter_n->KeyAt(0),buffer_pool_manager_);
//    else
//      inter_n->MoveLastToFrontOf(inter_c ,inter_n->KeyAt(inter_n->GetSize()-1), buffer_pool_manager_);
//  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if(old_root_node->IsLeafPage()){
    assert(old_root_node->GetSize() == 0);
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return true;
  }
  assert(old_root_node->GetSize() == 1);
  auto *old_root = reinterpret_cast<InternalPage *>(old_root_node);

  page_id_t new_root = old_root->RemoveAndReturnOnlyChild();
  root_page_id_ = new_root;
  UpdateRootPageId();

  auto* new_root_page = buffer_pool_manager_->FetchPage(new_root);
  if(new_root_page == nullptr)
    ASSERT(false, "fail to fetch page when adjust root");
  auto *new_root_node = reinterpret_cast<InternalPage*>(new_root_page->GetData());
  new_root_node->SetParentPageId(INVALID_PAGE_ID);
  buffer_pool_manager_->UnpinPage(new_root, true);
  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  return INDEXITERATOR_TYPE();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
    assert(root_page_id_ != INVALID_PAGE_ID);
    auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
    auto *node = reinterpret_cast<BPlusTreePage*>(page->GetData());

    while(!node->IsLeafPage()){
      auto  *in_node = reinterpret_cast<InternalPage *>(node);
      page_id_t child_page_id;
      if(leftMost){
        child_page_id = in_node->ValueAt(0);
      }else{
        child_page_id = in_node->Lookup(key, comparator_);
      }
      assert(child_page_id > 0);
      buffer_pool_manager_->UnpinPage((page->GetPageId()), false);
      page = buffer_pool_manager_->FetchPage(child_page_id);
      node = reinterpret_cast<BPlusTreePage*>(page->GetData());
    }
    return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changejd.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto* page = buffer_pool_manager_->FetchPage(0);
  if(page == nullptr)
    ASSERT(false, "fail to fetch page");
  auto* root_page_ = reinterpret_cast<IndexRootsPage*>(page->GetData());
  if(insert_record){
    root_page_->Insert(index_id_, root_page_id_);
  }
  else{
    root_page_->Update(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(0, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
