#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
//  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
//  index_id_t index_id_;
//  std::string index_name_;
//  table_id_t table_id_;
//  std::vector<uint32_t> key_map_;

  char* newbuf = buf;
  //magic_num
  memcpy(newbuf,&INDEX_METADATA_MAGIC_NUM,sizeof(uint32_t));
  newbuf+=sizeof(uint32_t);
  //index_id
  memcpy(newbuf,&index_id_, sizeof(index_id_t));
  newbuf+=sizeof(index_id_t);
  //index_name
  uint32_t name_len = index_name_.length();
  memcpy(newbuf,&name_len,sizeof(uint32_t));
  newbuf += sizeof(uint32_t);
  newbuf += index_name_.copy(newbuf,name_len);
  //table_id_
  memcpy(newbuf,&table_id_,sizeof(table_id_t));
  newbuf +=sizeof(table_id_t);
  //key_map_
  uint32_t key_map_l = key_map_.size();
  memcpy(newbuf,&key_map_l,sizeof(uint32_t));
  newbuf +=sizeof(uint32_t);
  for(auto i=key_map_.begin();i!=key_map_.end();++i){
    //tag:undetermined
    memcpy(newbuf,&i,sizeof(uint32_t));
    newbuf+=sizeof(uint32_t);
  }

  return newbuf - buf;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  //  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  //  index_id_t index_id_;
  //  std::string index_name_;
  //  table_id_t table_id_;
  //  std::vector<uint32_t> key_map_;
  uint32_t size=0;
  //magic_num
  size += sizeof(uint32_t);
  //available data
  size += sizeof(index_id_t)+2*sizeof(uint32_t)+index_name_.length()+sizeof(table_id_t)+key_map_.size()*sizeof(uint32_t);
  return size;
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  //  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  //  index_id_t index_id_;
  //  std::string index_name_;
  //  table_id_t table_id_;
  //  std::vector<uint32_t> key_map_;
  char *newbuf = buf;
  index_id_t _index_id_;
  uint32_t  index_name_l;
  std::string _index_name_;
  table_id_t _table_id_;
  uint32_t  key_map_l;
  std::vector<uint32_t> _key_map_;
  //magic_num
  //    uint32_t magic_num = MACH_READ_UINT32(newbuf);
  newbuf += sizeof(uint32_t);
  //index_id_
  _index_id_ = MACH_READ_FROM(index_id_t ,(newbuf));
  newbuf += sizeof(index_id_t);
  //index_name_
  index_name_l = MACH_READ_UINT32(newbuf);
  newbuf+=sizeof(uint32_t);
  _index_name_.append(newbuf, index_name_l);
  newbuf += index_name_l;
  //table_id_
  _table_id_ = MACH_READ_FROM(table_id_t ,(newbuf));
  newbuf+=sizeof(table_id_t);
  //key_map_
  key_map_l = MACH_READ_UINT32(newbuf);
  newbuf += sizeof(uint32_t);
  for(uint32_t i=0;i<key_map_l;++i) {
    _key_map_.push_back(MACH_READ_UINT32(newbuf));
    newbuf += sizeof(uint32_t);
  }
  //copy
  void *mem = heap->Allocate(sizeof(IndexMetadata));
  index_meta = new(mem)IndexMetadata(_index_id_,_index_name_,_table_id_,_key_map_);
  return newbuf - buf;
}