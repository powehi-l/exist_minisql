#include "record/column.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
    char *newbuf = buf;
//    //magic_number
//    memcpy(newbuf, &COLUMN_MAGIC_NUM, sizeof(uint32_t));
//    newbuf += sizeof(uint32_t);
    //name_
    uint32_t name_len = name_.length();
    memcpy(newbuf, &name_len, sizeof(uint32_t));
    newbuf += sizeof(uint32_t);
    newbuf += name_.copy(newbuf, name_len);
    //type_
    uint8_t tflag;
    if (type_ == kTypeInvalid) { tflag = 0; }
    else if (type_ == kTypeChar) { tflag = 1; }
    else if (type_ == kTypeFloat) { tflag = 2; }
    else if (type_ == kTypeInt) { tflag = 3; }
    else { tflag = 0; }
    memcpy(newbuf, &tflag, sizeof(uint8_t));
    newbuf += sizeof(uint8_t);
    //len_
    memcpy(newbuf, &len_, sizeof(uint32_t));
    newbuf += sizeof(uint32_t);
    //table_ind_
    memcpy(newbuf, &table_ind_, sizeof(uint32_t));
    newbuf += sizeof(uint32_t);
    //nullable_
    memcpy(newbuf, &nullable_, sizeof(bool));
    newbuf += sizeof(bool);
    //unique_
    memcpy(newbuf, &unique_, sizeof(bool));
    newbuf += sizeof(bool);
    return newbuf - buf;
}

uint32_t Column::GetSerializedSize() const {
  // replace with your code here
    return uint32_t(sizeof(uint32_t) * 4 + name_.length() + sizeof(uint8_t) + sizeof(bool) * 2);
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  // replace with your code here
    char *newbuf = buf;
//    //magic_num
//    uint32_t magic_num = MACH_READ_UINT32(newbuf);
//    newbuf += sizeof(uint32_t);
    //column_name
    uint32_t name_len = MACH_READ_UINT32(newbuf);
    newbuf += sizeof(uint32_t);
    std::string column_name;
    column_name.append(newbuf, name_len);
    newbuf += name_len;
    //type
    uint8_t tflag = MACH_READ_FROM(uint8_t,newbuf);
    TypeId type;
    if (tflag == 0) { type = kTypeInvalid; }
    else if (tflag == 1) { type = kTypeChar; }
    else if (tflag == 2) { type = kTypeFloat; }
    else if (tflag == 3) { type = kTypeInt; }
    else { type = kTypeInvalid; }
    newbuf += sizeof(uint8_t);
    //length
    uint32_t length = MACH_READ_UINT32(newbuf);
    newbuf += sizeof(uint32_t);
    //index
    uint32_t index = MACH_READ_UINT32(newbuf);
    newbuf += sizeof(uint32_t);
    //nullable
    bool nullable = MACH_READ_FROM(bool, newbuf);
    newbuf += sizeof(bool);
    //unique
    bool unique = MACH_READ_FROM(bool, newbuf);
    newbuf += sizeof(bool);
    //copy
    void *mem = heap->Allocate(sizeof(Column));
    column = new(mem)Column(column_name, type, length, index, nullable, unique);
    return newbuf - buf;
}
