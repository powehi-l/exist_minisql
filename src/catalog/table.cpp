#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
//  static constexpr uint32_t TABLE_METADATA_MAGIC_NUM = 344528;
//  table_id_t table_id_;
//  std::string table_name_;
//  page_id_t root_page_id_;
//  Schema *schema_;
  char* newbuf = buf;
  //magic_num
  memcpy(newbuf,&TABLE_METADATA_MAGIC_NUM,sizeof(uint32_t));
  newbuf += sizeof(uint32_t);
  //  table_id_t table_id_;
  memcpy(newbuf,&table_id_,sizeof(table_id_t));
  newbuf += sizeof(table_id_t);
  //  std::string table_name_;
  uint32_t name_len = table_name_.length();
  memcpy(newbuf,&name_len,sizeof(uint32_t));
  newbuf += sizeof(uint32_t);
  newbuf += table_name_.copy(newbuf,name_len);
  //  page_id_t root_page_id_;
  memcpy(newbuf,&root_page_id_,sizeof(page_id_t));
  newbuf += sizeof(page_id_t);
  //  Schema *schema_;
  newbuf += schema_->SerializeTo(newbuf);
  return newbuf-buf;
}

uint32_t TableMetadata::GetSerializedSize() const {
  uint32_t size=0;
  //magic_num
  size += sizeof(uint32_t);
  //available data
  size += sizeof(table_id_t)+sizeof(uint32_t)+table_name_.length()+sizeof(page_id_t)+schema_->GetSerializedSize();
  return size;
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  //  static constexpr uint32_t TABLE_METADATA_MAGIC_NUM = 344528;
  //  table_id_t table_id_;
  //  std::string table_name_;
  //  page_id_t root_page_id_;
  //  Schema *schema_;
  char *newbuf = buf;
  table_id_t _table_id_;
  uint32_t  table_name_l;
  std::string _table_name_;
  page_id_t _root_page_id_;
  std::vector<Column *> columns;
  auto *_schema_ = new Schema(columns);
  //magic_num
  //    uint32_t magic_num = MACH_READ_UINT32(newbuf);
  newbuf += sizeof(uint32_t);
  //table_id_
  _table_id_ = MACH_READ_FROM(table_id_t ,(newbuf));
  newbuf += sizeof(table_id_t);
  //table_name_
  table_name_l = MACH_READ_UINT32(newbuf);
  newbuf+=sizeof(uint32_t);
  _table_name_.append(newbuf, table_name_l);
  newbuf += table_name_l;
  //root_page_id
  _root_page_id_ = MACH_READ_FROM(page_id_t,(newbuf));
  newbuf += sizeof(page_id_t);
  //schema
  newbuf += _schema_->DeserializeFrom(newbuf, _schema_, heap);
  //copy
  void *mem = heap->Allocate(sizeof(TableMetadata));
  table_meta = new(mem)TableMetadata(_table_id_,_table_name_,_root_page_id_,_schema_);
  return newbuf - buf;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
