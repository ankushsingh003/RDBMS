#include "BlockBuffer.h"
#include <string.h>
#include <math.h>

int compareAttrs(Attribute attr1, Attribute attr2, int attrType) {
  double diff;
  if (attrType == STRING) {
    diff = strcmp(attr1.sVal, attr2.sVal);
  } else {
    diff = attr1.nVal - attr2.nVal;
  }

  if (diff > 0) return 1;
  if (diff < 0) return -1;
  return 0;
}

// Constructor for BlockBuffer (Constructor 2 - for existing blocks)
BlockBuffer::BlockBuffer(int blockNum) {
  this->blockNum = blockNum;
}

int BlockBuffer::getBlockNum() {
  return this->blockNum;
}

// Get the header of the block through the buffer
int BlockBuffer::getHeader(struct HeadInfo *head) {
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  memcpy(head, bufferPtr, HEADER_SIZE);
  return SUCCESS;
}

// Load the block into the buffer and get a pointer to it
int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) {
  // Check if the block is already in the buffer
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

  if (bufferNum == E_BLOCKNOTINBUFFER) {
    // If not in buffer, find a free buffer block
    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

    if (bufferNum == E_DISKFULL) {
      return E_DISKFULL;
    }

    // Read the block from the disk into the assigned buffer block
    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }

  // Update timestamps for LRU
  for (int i = 0; i < BUFFER_CAPACITY; i++) {
    if (!StaticBuffer::metainfo[i].free) {
      StaticBuffer::metainfo[i].timeStamp++;
    }
  }
  StaticBuffer::metainfo[bufferNum].timeStamp = 0;

  // Provide the pointer to the buffer block
  *buffPtr = StaticBuffer::blocks[bufferNum];
  return SUCCESS;
}

int BlockBuffer::setHeader(struct HeadInfo *head) {
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  memcpy(bufferPtr, head, HEADER_SIZE);

  StaticBuffer::setDirtyBit(this->blockNum);

  return SUCCESS;
}

void BlockBuffer::releaseBlock() {
  if (this->blockNum < 0 || this->blockNum >= DISK_BLOCKS || this->blockNum == RELCAT_BLOCK || this->blockNum == ATTRCAT_BLOCK) return;

  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);
  if (bufferNum != E_BLOCKNOTINBUFFER) {
    StaticBuffer::metainfo[bufferNum].free = true;
  }

  StaticBuffer::blockAllocMap[this->blockNum] = (unsigned char)UNUSED_BLK;
  this->blockNum = INVALID_BLOCKNUM;
}

// Constructor for RecBuffer (Constructor 2 - for existing blocks)

// Constructor for RecBuffer (Constructor 2 - for existing blocks)
RecBuffer::RecBuffer(int blockNum) : BlockBuffer(blockNum) {}

int RecBuffer::getSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  this->getHeader(&head);

  int numSlots = head.numSlots;
  memcpy(slotMap, bufferPtr + HEADER_SIZE, numSlots);

  return SUCCESS;
}

int RecBuffer::setSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  this->getHeader(&head);

  int numSlots = head.numSlots;
  memcpy(bufferPtr + HEADER_SIZE, slotMap, numSlots);

  StaticBuffer::setDirtyBit(this->blockNum);

  return SUCCESS;
}

// Get the record at a specific slotNum through the buffer
int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  this->getHeader(&head);

  int numAttrs = head.numAttrs;
  int numSlots = head.numSlots;

  // Offset of record = HEADER_SIZE + numSlots + (slotNum * recordSize)
  int recordSize = numAttrs * ATTR_SIZE;
  unsigned char *slotPointer = bufferPtr + HEADER_SIZE + numSlots + (slotNum * recordSize);
  
  memcpy(rec, slotPointer, recordSize);

  return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum) {
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  this->getHeader(&head);

  int numAttrs = head.numAttrs;
  int numSlots = head.numSlots;

  int recordSize = numAttrs * ATTR_SIZE;
  unsigned char *slotPointer = bufferPtr + HEADER_SIZE + numSlots + (slotNum * recordSize);

  memcpy(slotPointer, rec, recordSize);

  StaticBuffer::setDirtyBit(this->blockNum);

  return SUCCESS;
}
IndBuffer::IndBuffer(int blockNum) : BlockBuffer(blockNum) {}
IndBuffer::IndBuffer(char blockType) : BlockBuffer(blockType) {}

IndInternal::IndInternal() : IndBuffer('I') {}
IndInternal::IndInternal(int blockNum) : IndBuffer(blockNum) {}

int IndInternal::getEntry(void *ptr, int indexNum) { return SUCCESS; }
int IndInternal::setEntry(void *ptr, int indexNum) { return SUCCESS; }

IndLeaf::IndLeaf() : IndBuffer('L') {}
IndLeaf::IndLeaf(int blockNum) : IndBuffer(blockNum) {}

int IndLeaf::getEntry(void *ptr, int indexNum) { return SUCCESS; }
int IndLeaf::setEntry(void *ptr, int indexNum) { return SUCCESS; }
