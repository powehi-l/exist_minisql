#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
    char *newbuf = buf;
    //magic_number
    memcpy(newbuf, &SCHEMA_MAGIC_NUM, sizeof(uint32_t));
    newbuf += sizeof(uint32_t);
    //columns
    for (auto column : columns_)
        newbuf += column->SerializeTo(newbuf);
    return newbuf - buf;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
    uint32_t size = sizeof(uint32_t);
    for (auto column : columns_)
        size += column->GetSerializedSize();
    return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // replace with your code here
    char *newbuf = buf;
    //magic_num
//    uint32_t magic_num = MACH_READ_UINT32(newbuf);
    newbuf += sizeof(uint32_t);
    //columns
    std::vector<Column *> columns;
    Column *column;
    while (*newbuf){
        Column::DeserializeFrom(newbuf, column, heap);
        columns.push_back(column);
        newbuf += column->GetSerializedSize();
    }
    void *mem = heap->Allocate(sizeof(Schema));
    schema = new(mem)Schema(columns);
    return newbuf - buf;
}