#ifndef NITCBASE_BLOCKBUFFER_H
#define NITCBASE_BLOCKBUFFER_H

#include <cstdint>
#include <cstring>

#include "../Disk_Class/Disk.h"
#include "../define/constants.h"
#include "StaticBuffer.h"

struct HeadInfo {
  int32_t blockType;
  int32_t pblock;
  int32_t lblock;
  int32_t rblock;
  int32_t numEntries;
  int32_t numAttrs;
  int32_t numSlots;
  unsigned char reserved[4];
};

typedef union Attribute {
  double nVal;
  char sVal[ATTR_SIZE];
} Attribute;

int compareAttrs(Attribute attr1, Attribute attr2, int attrType);

struct InternalEntry {
  int32_t lChild;
  union Attribute attrVal;
  int32_t rChild;
};

struct Index {
  union Attribute attrVal;
  int32_t block;
  int32_t slot;
  unsigned char unused[8];
};

class BlockBuffer {
 protected:
  // field
  int blockNum;
  // methods
  int loadBlockAndGetBufferPtr(unsigned char **buffPtr);
 
 public:
  // methods
  static int getFreeBlock(int blockType) {
    for (int i = 0; i < DISK_BLOCKS; i++) {
        if (StaticBuffer::blockAllocMap[i] == UNUSED_BLK) {
            StaticBuffer::blockAllocMap[i] = (unsigned char)blockType;
            return i;
        }
    }
    return E_DISKFULL;
  }

  static int setBlockType(int blockNum, int blockType) {
    if (blockNum < 0 || blockNum >= DISK_BLOCKS) return E_OUTOFBOUND;
    StaticBuffer::blockAllocMap[blockNum] = (unsigned char)blockType;
    return SUCCESS;
  }

  BlockBuffer(char blockType) {
    int blockTypeInt;
    if (blockType == 'R') blockTypeInt = REC;
    else if (blockType == 'I') blockTypeInt = IND_INTERNAL;
    else if (blockType == 'L') blockTypeInt = IND_LEAF;
    else blockTypeInt = UNUSED_BLK;

    int freeBlockNum = BlockBuffer::getFreeBlock(blockTypeInt);
    this->blockNum = freeBlockNum;

    if (freeBlockNum >= 0 && freeBlockNum < DISK_BLOCKS) {
      struct HeadInfo head;
      head.blockType = blockTypeInt;
      head.pblock = -1;
      head.lblock = -1;
      head.rblock = -1;
      head.numEntries = 0;
      head.numAttrs = 0;
      head.numSlots = 0;
      setHeader(&head);
    }
  }
  BlockBuffer(int blockNum);
  int getBlockNum();
  int getHeader(struct HeadInfo *head);
  int setHeader(struct HeadInfo *head);
  void releaseBlock();
};

class RecBuffer : public BlockBuffer {
 public:
  // methods
  RecBuffer() : BlockBuffer('R') {}
  RecBuffer(int blockNum);
  int getSlotMap(unsigned char *slotMap);
  int setSlotMap(unsigned char *slotMap);
  int getRecord(union Attribute *rec, int slotNum);
  int setRecord(union Attribute *rec, int slotNum);
};

class IndBuffer : public BlockBuffer {
 public:
  IndBuffer(int blockNum);
  IndBuffer(char blockType);
  virtual int getEntry(void *ptr, int indexNum) = 0;
  virtual int setEntry(void *ptr, int indexNum) = 0;
};

class IndInternal : public IndBuffer {
 public:
  IndInternal();
  IndInternal(int blockNum);
  int getEntry(void *ptr, int indexNum);
  int setEntry(void *ptr, int indexNum);
};

class IndLeaf : public IndBuffer {
 public:
  IndLeaf();
  IndLeaf(int blockNum);
  int getEntry(void *ptr, int indexNum);
  int setEntry(void *ptr, int indexNum);
};

#endif  // NITCBASE_BLOCKBUFFER_H