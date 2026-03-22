#include "StaticBuffer.h"

// Initializing the static members
unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];

StaticBuffer::StaticBuffer() {
  for (int i = 0; i < BUFFER_CAPACITY; i++) {
    metainfo[i].free = true;
    metainfo[i].dirty = false;
    metainfo[i].blockNum = -1;
    metainfo[i].timeStamp = -1;
  }
}

StaticBuffer::~StaticBuffer() {
  // At this stage, write-back is not implemented, so nothing to do here.
}

int StaticBuffer::getFreeBuffer(int blockNum) {
  if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }

  // Find a free buffer block
  for (int i = 0; i < BUFFER_CAPACITY; i++) {
    if (metainfo[i].free) {
      metainfo[i].free = false;
      metainfo[i].blockNum = blockNum;
      return i;
    }
  }

  // If no free buffer is found, return E_DISKFULL for now (LRU will be implemented later)
  return E_DISKFULL;
}

int StaticBuffer::getBufferNum(int blockNum) {
  if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }

  // Check if the block is already in the buffer
  for (int i = 0; i < BUFFER_CAPACITY; i++) {
    if (!metainfo[i].free && metainfo[i].blockNum == blockNum) {
      return i;
    }
  }

  return E_BLOCKNOTINBUFFER;
}
