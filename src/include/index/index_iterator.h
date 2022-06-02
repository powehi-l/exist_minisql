#ifndef MINISQL_INDEX_ITERATOR_H
#define MINISQL_INDEX_ITERATOR_H

#include "page/b_plus_tree_leaf_page.h"

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator();
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_, int index_, BufferPoolManager* buffer_pool_manager_);
  ~IndexIterator();

  /** Return the key/value pair this iterator is currently pointing at. */
  const MappingType &operator*();

  /** Move to the next key/value pair.*/
  IndexIterator &operator++();

  /** Return whether two iterators are equal */
  bool operator==(const IndexIterator &itr) const;

  /** Return whether two iterators are not equal. */
  bool operator!=(const IndexIterator &itr) const;

private:
  // add your own private member variables here
 int index = 0;
 B_PLUS_TREE_LEAF_PAGE_TYPE* leaf = nullptr;
 BufferPoolManager *buffer_pool_manager = nullptr;
};


#endif //MINISQL_INDEX_ITERATOR_H
