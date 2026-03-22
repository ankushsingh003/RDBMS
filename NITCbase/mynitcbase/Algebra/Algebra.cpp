#include "Algebra.h"
#include <cstdio>
#include <cstring>
#include <cstdlib>

/* 
 * This function will be called by the Frontend to handle the SELECT command.
 * For now, it performs a linear search on the source relation and prints 
 * the records that satisfy the condition.
 */
int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
  int srcRelId = OpenRelTable::getRelId(srcRel);      // Get the relId of the source relation
  std::printf("DEBUG: select srcRel=%s, srcRelId=%d\n", srcRel, srcRelId);
  if (srcRelId == E_RELNOTEXIST) {
    return E_RELNOTOPEN;
  }

  AttrCatEntry attrCatBuf;
  // Get the attribute catalog entry for the attribute mentioned in the WHERE clause
  int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatBuf);
  std::printf("DEBUG: getAttrCatEntry(relId=%d, attr=%s) returned %d\n", srcRelId, attr, ret);
  if (ret != SUCCESS) {
    return ret;
  }

  // Convert the string value to the appropriate type
  Attribute attrVal;
  if (attrCatBuf.attrType == NUMBER) {
    attrVal.nVal = std::atof(strVal);
  } else {
    std::strcpy(attrVal.sVal, strVal);
  }

  /*** Build the searchIndex in the cache to start from the beginning ***/
  RelCacheTable::resetSearchIndex(srcRelId);

  RelCatEntry relCatBuf;
  RelCacheTable::getRelCatEntry(srcRelId, &relCatBuf);

  /*** Iterate through the records and print those that satisfy the condition ***/
  bool found = false;
  while (true) {
    RecId recid = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);

    if (recid.block != -1 && recid.slot != -1) {
      found = true;
      RecBuffer block(recid.block);
      Attribute *record = (Attribute *)std::malloc(sizeof(Attribute) * relCatBuf.numAttrs);
      block.getRecord(record, recid.slot);

      for (int i = 0; i < relCatBuf.numAttrs; i++) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
        
        if (attrCatEntry.attrType == NUMBER) {
          std::printf("%d | ", (int)record[i].nVal);
        } else {
          std::printf("%s | ", record[i].sVal);
        }
      }
      std::printf("\n");
      std::free(record);
    } else {
      break;
    }
  }

  if (!found) {
    // printf("No records match the condition\n");
    return SUCCESS;
  }

  return SUCCESS;
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]) {
  if (std::strcmp(relName, RELCAT_RELNAME) == 0 || std::strcmp(relName, ATTRCAT_RELNAME) == 0) {
    return E_NOTPERMITTED;
  }

  int relId = OpenRelTable::getRelId(relName);
  if (relId == E_RELNOTEXIST) {
    return E_RELNOTOPEN;
  }

  RelCatEntry relCatBuf;
  RelCacheTable::getRelCatEntry(relId, &relCatBuf);

  if (nAttrs != relCatBuf.numAttrs) {
    return E_NATTRMISMATCH;
  }

  Attribute relRecord[nAttrs];
  for (int i = 0; i < nAttrs; i++) {
    AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId, i, &attrCatBuf);
    if (attrCatBuf.attrType == NUMBER) {
      relRecord[i].nVal = std::atof(record[i]);
    } else {
      std::strcpy(relRecord[i].sVal, record[i]);
    }
  }

  int prevBlockNum = -1;
  int currBlockNum = relCatBuf.firstBlk;
  int slotNum = -1;

  while (currBlockNum != -1) {
    RecBuffer block(currBlockNum);
    struct HeadInfo head;
    block.getHeader(&head);

    unsigned char slotMap[relCatBuf.numSlotsPerBlk];
    block.getSlotMap(slotMap);

    for (int i = 0; i < relCatBuf.numSlotsPerBlk; i++) {
      if (slotMap[i] == SLOT_UNOCCUPIED) {
        slotNum = i;
        break;
      }
    }

    if (slotNum != -1) break;

    prevBlockNum = currBlockNum;
    currBlockNum = head.rblock;
  }

  if (slotNum == -1) {
    int newBlockNum = BlockBuffer::getFreeBlock(REC);
    if (newBlockNum == E_DISKFULL) return E_DISKFULL;

    RecBuffer newBlock(newBlockNum);
    
    if (prevBlockNum != -1) {
      RecBuffer prevBlock(prevBlockNum);
      struct HeadInfo prevHead;
      prevBlock.getHeader(&prevHead);
      prevHead.rblock = newBlockNum;
      prevBlock.setHeader(&prevHead);
    } else {
      relCatBuf.firstBlk = newBlockNum;
    }

    relCatBuf.lastBlk = newBlockNum;
    
    struct HeadInfo newHead;
    newBlock.getHeader(&newHead);
    newHead.numAttrs = relCatBuf.numAttrs;
    newHead.numSlots = relCatBuf.numSlotsPerBlk;
    newHead.lblock = prevBlockNum;
    newHead.rblock = -1;
    newBlock.setHeader(&newHead);

    unsigned char slotMap[relCatBuf.numSlotsPerBlk];
    for (int i = 0; i < relCatBuf.numSlotsPerBlk; i++) slotMap[i] = SLOT_UNOCCUPIED;
    newBlock.setSlotMap(slotMap);

    currBlockNum = newBlockNum;
    slotNum = 0;
  }

  RecBuffer block(currBlockNum);
  block.setRecord(relRecord, slotNum);

  unsigned char slotMap[relCatBuf.numSlotsPerBlk];
  block.getSlotMap(slotMap);
  slotMap[slotNum] = SLOT_OCCUPIED;
  block.setSlotMap(slotMap);

  struct HeadInfo head;
  block.getHeader(&head);
  head.numEntries++;
  block.setHeader(&head);

  relCatBuf.numRecs++;
  RelCacheTable::setRelCatEntry(relId, &relCatBuf);

  return SUCCESS;
}