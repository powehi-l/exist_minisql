#include "catalog/catalog.h"
//#include <iostream>
//#include <fstream>
#include<cstring>

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
  else{
    ReadCatalog();
  }
  //tag:catalog_meta和next_table_id和next_index_id的初始化问题
}

CatalogManager::~CatalogManager() {
  WriteCatalog();
  delete heap_;
}
// store order: catalog_meta->next_table_id_->next_index_id_->table_size->
// foreach(tables_id->table_meta_size->table_meta->index_size->
// ->foreach(index_id->index_meta_size->index_meta))
void CatalogManager::WriteCatalog(){
  ofstream outfile;
//  static string db_file_name = "catalog_test.db";
  //db_file_name
  std::string db_file_name = buffer_pool_manager_->GetDiskManager()->GetFileName();
  db_file_name = db_file_name.substr(0,db_file_name.find_last_of('.'));
  db_file_name +=".dat";
  outfile.open(db_file_name,ios::app);
  //catalog_meta
  char *catalog_meta = reinterpret_cast<char *>(heap_->Allocate(PAGE_SIZE));
  catalog_meta_->SerializeTo(catalog_meta);
  outfile.write(catalog_meta,catalog_meta_->GetSerializedSize());
  outfile<<endl;
  //next_table_id_
  outfile<<next_table_id_<<endl;
  //next_index_id_
  outfile<<next_index_id_<<endl;
  //table_size
  outfile<<table_names_.size()<<endl;
  //for each table
  for(auto i:table_names_){
    //table_id
    outfile<<i.second<<endl;
    //table_meta_size
    uint32_t table_meta_size = tables_.find(i.second)->second->GetTableMeta()->GetSerializedSize();
    outfile<<table_meta_size<<endl;
    //table_meta
    char *table_meta = reinterpret_cast<char *>(heap_->Allocate(PAGE_SIZE));
    tables_.find(i.second)->second->GetTableMeta()->SerializeTo(table_meta);
    outfile.write(table_meta,table_meta_size);
    outfile<<endl;
    //indexes:map<index_name,index_id>
    auto indexes = index_names_.find(i.first)->second;
    //index_size
    outfile<<indexes.size()<<endl;
    // for each index
    for(auto index:indexes){
      //index_id
      outfile<<index.second<<endl;
      //index_meta_size
      uint32_t index_meta_size = indexes_.find(index.second)->second->GetIndexMeta()->GetSerializedSize();
      outfile<<index_meta_size<<endl;
      //index_meta
      char *index_meta = reinterpret_cast<char *>(heap_->Allocate(PAGE_SIZE));
      indexes_.find(index.second)->second->GetIndexMeta()->SerializeTo(index_meta);
      outfile.write(index_meta,index_meta_size);
      outfile<<endl;
    }

  }
  outfile.close();
}
// store order: catalog_meta->next_table_id_->next_index_id_->table_size->
// foreach(tables_id->table_meta_size->table_meta->index_size->
// ->foreach(index_id->index_meta_size->index_meta))
void CatalogManager::ReadCatalog(){
  //MAX_FILE_SIZE
  uint32_t MAX_FILE_SIZE=16 * 1024;
  //db_file_name
  std::string db_file_name = buffer_pool_manager_->GetDiskManager()->GetFileName();
  db_file_name = db_file_name.substr(0,db_file_name.find_last_of('.'));
  db_file_name +=".dat";
  ifstream infile;
  infile.open(db_file_name,ios::in);
  //catalog_meta
  char *catalog_meta = reinterpret_cast<char *>(heap_->Allocate(PAGE_SIZE));
  infile.getline(catalog_meta,catalog_meta_->GetSerializedSize());
  catalog_meta_ = CatalogMeta::DeserializeFrom(catalog_meta,heap_);
  //next_table_id
  char next_table_id[MAX_FILE_SIZE];
  infile.getline(next_table_id,MAX_FILE_SIZE);
  next_table_id_ = atoi(next_table_id);
  //next_table_id
  char next_index_id[MAX_FILE_SIZE];
  infile.getline(next_index_id,MAX_FILE_SIZE);
  next_index_id_ = atoi(next_index_id);
  //table_size
  char table_size_[MAX_FILE_SIZE];
  infile.getline(table_size_,MAX_FILE_SIZE);
  uint32_t table_size = atoi(table_size_);

  for(uint32_t i=0;i<table_size;i++){
    //table_id
    char table_id_[MAX_FILE_SIZE];
    infile.getline(table_id_,MAX_FILE_SIZE);
    table_id_t table_id = atoi(table_id_);
    //table_meta_size
    char table_meta_size_[MAX_FILE_SIZE];
    infile.getline(table_meta_size_,MAX_FILE_SIZE);
    uint32_t table_meta_size = atoi(table_meta_size_);
    //table_meta
    char *table_meta_ = reinterpret_cast<char *>(heap_->Allocate(PAGE_SIZE));
    infile.getline(table_meta_,table_meta_size);
    TableMetadata *table_meta;
    TableMetadata::DeserializeFrom(table_meta_,table_meta,heap_);
    //load table
    LoadTable(table_id,INVALID_PAGE_ID,table_meta);
    //index_size
    char index_size_[MAX_FILE_SIZE];
    infile.getline(index_size_,MAX_FILE_SIZE);
    uint32_t index_size = atoi(index_size_);
    for(uint32_t oi=0;oi<index_size;oi++){
      //index_id
      char index_id_[MAX_FILE_SIZE];
      infile.getline(index_id_,MAX_FILE_SIZE);
      index_id_t index_id = atoi(index_id_);
      //index_meta_size
      char index_meta_size_[MAX_FILE_SIZE];
      infile.getline(index_meta_size_,MAX_FILE_SIZE);
      uint32_t index_meta_size = atoi(index_meta_size_);
      //index_meta
      char *index_meta_ = reinterpret_cast<char *>(heap_->Allocate(PAGE_SIZE));
      infile.getline(index_meta_,index_meta_size);
      IndexMetadata *index_meta;
      IndexMetadata::DeserializeFrom(index_meta_,index_meta,heap_);
      //load index
      LoadIndex(index_id,INVALID_PAGE_ID,index_meta);
    }

  }

  infile.close();
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
// map for tables
//std::unordered_map<std::string, table_id_t> table_names_; //1
//std::unordered_map<table_id_t, TableInfo *> tables_; //1
// map for indexes: table_name->index_name->indexes
//[[maybe_unused]] std::unordered_map<std::string, std::unordered_map<std::string, index_id_t>> index_names_; //1
//[[maybe_unused]] std::unordered_map<index_id_t, IndexInfo *> indexes_; //1
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id, TableMetadata *table_meta) {
  // ASSERT(false, "Not Implemented yet");
  //table_names
  table_names_.emplace(table_meta->GetTableName(),table_id);
  //create table heap
  TableHeap *table = TableHeap::Create(buffer_pool_manager_,table_meta->GetSchema(), nullptr,
                                       log_manager_,lock_manager_,heap_);
  //TableInfo
  TableInfo *table_info;
  table_info= TableInfo::Create(heap_);
  table_info->Init(table_meta,table);
  tables_.emplace(table_id,table_info);
  //track
  index_names_.emplace(table_meta->GetTableName(), std::unordered_map<std::string, index_id_t>{});
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id, IndexMetadata *index_meta) {
  // ASSERT(false, "Not Implemented yet");
  //index_names_
  std::string table_name;
  table_name = tables_.find(index_meta->GetTableId())->second->GetTableName();
  index_names_.find(table_name)->second.emplace(index_meta->GetIndexName(),index_id);
  //index info
  IndexInfo *index_info = IndexInfo::Create(heap_);
  index_info->Init(index_meta,tables_.find(index_meta->GetTableId())->second,buffer_pool_manager_);
  indexes_.emplace(index_id,index_info);

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto meta = tables_.find(table_id);
  if (meta == tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = meta->second;
  return DB_SUCCESS;
}