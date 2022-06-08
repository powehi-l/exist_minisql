#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  // ASSERT(false, "Not Implemented yet");
  char *newbuf = buf;
  //magic_number
  memcpy(newbuf, &CATALOG_METADATA_MAGIC_NUM, sizeof(uint32_t));
  newbuf += sizeof(uint32_t);
  //table_meta_pages;
  uint32_t table_size=table_meta_pages_.size();
  memcpy(newbuf,&table_size,sizeof(uint32_t));
  newbuf += sizeof(uint32_t);
  for(auto it : table_meta_pages_) {
    memcpy(newbuf,&it.first,sizeof(table_id_t));
    newbuf+=sizeof(table_id_t);
    memcpy(newbuf,&it.second,sizeof(page_id_t));
    newbuf+=sizeof(page_id_t);
  }
  //index_meta_pages;
  uint32_t index_size=index_meta_pages_.size();
  memcpy(newbuf,&index_size,sizeof(uint32_t));
  newbuf += sizeof(uint32_t);
  for(auto it : index_meta_pages_) {
    memcpy(newbuf,&it.first,sizeof(index_id_t));
    newbuf+=sizeof(index_id_t);
    memcpy(newbuf,&it.second,sizeof(page_id_t));
    newbuf+=sizeof(page_id_t);
  }

}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  // ASSERT(false, "Not Implemented yet");
//  std::map<table_id_t, page_id_t> table_meta_pages_;
//  std::map<index_id_t, page_id_t> index_meta_pages_;
  char *newbuf = buf;
//  CatalogMeta *meta = CatalogMeta::NewInstance(heap);
  auto meta = CatalogMeta::NewInstance(heap);
  std::map<table_id_t, page_id_t> _table_meta_pages_;
  std::map<index_id_t, page_id_t> _index_meta_pages_;
  uint32_t table_size;
  uint32_t index_size;
  table_id_t table_id;
  index_id_t index_id;
  page_id_t page_id;
  //magic_num
  //    uint32_t magic_num = MACH_READ_UINT32(newbuf);
  newbuf += sizeof(uint32_t);
  //table_meta_pages;
  table_size = MACH_READ_UINT32(newbuf);
  newbuf += sizeof(uint32_t);
  for (uint32_t i=0;i<table_size;i++) {
    table_id = MACH_READ_FROM(table_id_t,(newbuf));
    newbuf += sizeof(table_id_t);
    page_id = MACH_READ_FROM(page_id_t,(newbuf));
    newbuf += sizeof(page_id_t);
    _table_meta_pages_.insert(pair<table_id_t, page_id_t>(table_id, page_id));
  }
  //index_meta_pages;
  index_size = MACH_READ_UINT32(newbuf);
  newbuf += sizeof(uint32_t);
  for (uint32_t i=0;i<index_size;i++) {
    index_id = MACH_READ_FROM(index_id_t,(newbuf));
    newbuf += sizeof(index_id_t);
    page_id = MACH_READ_FROM(page_id_t,(newbuf));
    newbuf += sizeof(page_id_t);
    _index_meta_pages_.insert(pair<index_id_t, page_id_t>(index_id, page_id));
  }
  //copy
  meta->table_meta_pages_ = _table_meta_pages_;
  meta->index_meta_pages_ = _index_meta_pages_;

  //copy
//  void *mem = heap->Allocate(sizeof(CatalogMeta));
//  catalog_meta = new(mem)CatalogMeta(_table_meta_pages_,_index_meta_pages_);
//  *(meta->GetIndexMetaPages())=_index_meta_pages_;
//  *(meta->GetTableMetaPages())= _table_meta_pages_;
  return meta;
//  return meta;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  uint32_t size=0;
  //magic_num
  size += sizeof(uint32_t);
  //available data
  size += sizeof(uint32_t)*2;
  size +=table_meta_pages_.size()*(sizeof(table_id_t)+sizeof(page_id_t));
  size +=index_meta_pages_.size()*(sizeof(index_id_t)+sizeof(page_id_t));
  return size;
}

CatalogMeta::CatalogMeta(){}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager),heap_(new SimpleMemHeap()){
  // ASSERT(false, "Not Implemented yet");
  if(init){
    next_table_id_ = 0;
    next_index_id_ = 0;
    catalog_meta_ = CatalogMeta::NewInstance(heap_);
  }
  else{}
  //tag:catalog_meta和next_table_id和next_index_id的初始化问题
}

CatalogManager::~CatalogManager() {
  delete heap_;
}
/**  [[maybe_unused]] CatalogMeta *catalog_meta_;
 * std::map<table_id_t, page_id_t> table_meta_pages_;
 * std::map<index_id_t, page_id_t> index_meta_pages_;
 * // memory heap //
 * MemHeap *heap_;
 * */
//[[maybe_unused]] BufferPoolManager *buffer_pool_manager_; //1
//[[maybe_unused]] LockManager *lock_manager_; //1
//[[maybe_unused]] LogManager *log_manager_; //1
//[[maybe_unused]] CatalogMeta *catalog_meta_;###???
//[[maybe_unused]] std::atomic<table_id_t> next_table_id_; //1###???
//[[maybe_unused]] std::atomic<index_id_t> next_index_id_; //1###???
// map for tables
//std::unordered_map<std::string, table_id_t> table_names_; //1
//std::unordered_map<table_id_t, TableInfo *> tables_; //1
// map for indexes: table_name->index_name->indexes
//[[maybe_unused]] std::unordered_map<std::string, std::unordered_map<std::string, index_id_t>> index_names_; //1
//[[maybe_unused]] std::unordered_map<index_id_t, IndexInfo *> indexes_; //1
// memory heap
//MemHeap *heap_;
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  //table已存在
  if(table_names_.count(table_name)!=0)return DB_TABLE_ALREADY_EXIST;
  //create table heap
  TableHeap *table = TableHeap::Create(buffer_pool_manager_,schema,txn,
                                       log_manager_,lock_manager_,heap_);
  //fetch table_id
  const auto table_id = next_table_id_.fetch_add(1);
  //create table meta::tag::page_id分配问题
  page_id_t page_id;
  buffer_pool_manager_->NewPage(page_id);
  auto table_meta = TableMetadata::Create(table_id,table_name,
                                          page_id,schema,heap_);
  //table_info
  //auto meta = TableInfo::Create(heap_);
  //auto meta = table_info;
  table_info= TableInfo::Create(heap_);
  table_info->Init(table_meta,table);

  //Update the internal tracking mechanisms
  tables_.emplace(table_id, table_info);
  table_names_.emplace(table_name, table_id);
  index_names_.emplace(table_name, std::unordered_map<std::string, index_id_t>{});
  catalog_meta_->table_meta_pages_.insert(pair<table_id_t, page_id_t>(table_id,page_id));
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto table_id = table_names_.find(table_name);
  // Table not found
  if(table_id == table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto meta = tables_.find(table_id->second);
  table_info = meta->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  std::vector<TableInfo *> tables_t{};
  auto i= tables_.begin();
  if(i==tables_.end())return DB_TABLE_NOT_EXIST;
  //tables_t.reserve(sizeof(TableInfo*));
  if(i==tables_.end()) return DB_TABLE_NOT_EXIST;
  else{
    for(;i!=tables_.end();++i)
      tables.push_back(i->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  // ASSERT(false, "Not Implemented yet");
  // table not exist
  if(table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  // index already exist
  auto &table_indexes = index_names_.find(table_name)->second;
  if(table_indexes.find(index_name)!=table_indexes.end()) return DB_INDEX_ALREADY_EXIST;
  //get table_meta
  TableInfo *table;
  GetTable(table_name,table);
  auto table_schema = table->GetSchema();
  auto columns = table_schema->GetColumns();
  //key map
  uint32_t oi;
  std::vector<uint32_t> key_map;
  for(auto i=index_keys.begin();i!=index_keys.end();++i){
    oi=0;
    for(auto it = columns.begin();it!=columns.end();++it){
      if((*it)->GetName()==(*i))break;
      oi++;
    }
    key_map.push_back(oi);
  }

  //index metadata
  const auto index_id = next_index_id_.fetch_add(1);
  const auto table_id = table_names_.find(table_name)->second;
  auto meta = IndexMetadata::Create(index_id,index_name,table_id,key_map,heap_);
  //index info
  index_info = IndexInfo::Create(heap_);
  index_info->Init(meta,table,buffer_pool_manager_);
  //update internal tracking
  indexes_.emplace(index_id,index_info);
  table_indexes.emplace(index_name,index_id);
  catalog_meta_->index_meta_pages_.insert(pair<index_id_t, page_id_t>(index_id,page_id));
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  auto table = index_names_.find(table_name);
  if(table ==index_names_.end())return DB_TABLE_NOT_EXIST;
  auto &table_indexes = table->second;
  auto index_meta = table_indexes.find(index_name);
  if(index_meta == table_indexes.end())return DB_INDEX_NOT_FOUND;
  auto index = indexes_.find(index_meta->second);
  index_info = index->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  //ensure the table exists
  if(table_names_.find(table_name)==table_names_.end()) return DB_TABLE_NOT_EXIST;

  auto table_indexes = index_names_.find(table_name);
  std::vector<IndexInfo *> index{};
  index.reserve(table_indexes->second.size());
  for(const auto &index_meta:table_indexes->second){
    auto index_t = indexes_.find(index_meta.second);
    index.push_back(index_t->second);
  }
  //get indexes
  indexes = index;
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {

  //table not exist
  if(table_names_.find(table_name)==table_names_.end()) return DB_TABLE_NOT_EXIST;
  //table_id
  auto table_id = table_names_.find(table_name)->second;
  //erase from table_names_
  table_names_.erase(table_name);
  //erase from tables_
  tables_.erase(table_id);
  //index_id
  auto indexes = index_names_.find(table_name)->second;
  //erase from index_names_
  index_names_.erase(table_name);
  //erase from indexes_
  for(auto i:indexes) indexes_.erase(i.second);

  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {

  if(table_names_.find(table_name)==table_names_.end())return DB_TABLE_NOT_EXIST;
  auto indexes =index_names_.find(table_name)->second;
  //index_id
  auto index_id = indexes.find(index_name)->second;
  //erase from indexes_
  indexes_.erase(index_id);
  //erase from index_names_
  indexes.erase(index_name);
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto meta = tables_.find(table_id);
  if (meta == tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = meta->second;
  return DB_SUCCESS;
}