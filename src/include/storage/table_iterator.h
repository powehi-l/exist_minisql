#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"


class TableHeap;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
  TableIterator();

  TableIterator(const TableIterator &other);

  TableIterator(TableHeap *table_heap, RowId rid);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  Row &operator*();

  TableIterator& operator=(const TableIterator &other);

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  // add your own private member variables here
  TableHeap *table_heap_;
  Row *row_;
};

#endif //MINISQL_TABLE_ITERATOR_H
