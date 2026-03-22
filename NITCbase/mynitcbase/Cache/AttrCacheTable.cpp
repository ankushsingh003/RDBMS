#include "AttrCacheTable.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

AttrCacheEntry *AttrCacheTable::attrCache[MAX_OPEN];

int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry *attrCatBuf) {
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if (attrCache[relId] == NULL) {
    return E_RELNOTOPEN;
  }

  for (AttrCacheEntry *entry = attrCache[relId]; entry != NULL; entry = entry->next) {
    if (entry->attrCatEntry.offset == attrOffset) {
      *attrCatBuf = entry->attrCatEntry;
      return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}

int AttrCacheTable::getAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatBuf) {
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if (attrCache[relId] == NULL) {
    return E_RELNOTOPEN;
  }

  for (AttrCacheEntry *entry = attrCache[relId]; entry != NULL; entry = entry->next) {
    if (strcmp(entry->attrCatEntry.attrName, attrName) == 0) {
      *attrCatBuf = entry->attrCatEntry;
      return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}

void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS], AttrCatEntry *attrCatEntry) {
  memcpy(attrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal, ATTR_SIZE);
  attrCatEntry->relName[ATTR_SIZE - 1] = '\0';
  memcpy(attrCatEntry->attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal, ATTR_SIZE);
  attrCatEntry->attrName[ATTR_SIZE - 1] = '\0';
  attrCatEntry->attrType = (int)record[ATTRCAT_ATTR_TYPE_INDEX].nVal;
  attrCatEntry->primaryFlag = (bool)record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal;
  attrCatEntry->rootBlock = (int)record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
  attrCatEntry->offset = (int)record[ATTRCAT_OFFSET_INDEX].nVal;
}

void AttrCacheTable::attrCatEntryToRecord(AttrCatEntry *attrCatEntry, union Attribute record[ATTRCAT_NO_ATTRS]) {
  memcpy(record[ATTRCAT_REL_NAME_INDEX].sVal, attrCatEntry->relName, ATTR_SIZE);
  memcpy(record[ATTRCAT_ATTR_NAME_INDEX].sVal, attrCatEntry->attrName, ATTR_SIZE);
  record[ATTRCAT_ATTR_TYPE_INDEX].nVal = (double)attrCatEntry->attrType;
  record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = (double)attrCatEntry->primaryFlag;
  record[ATTRCAT_ROOT_BLOCK_INDEX].nVal = (double)attrCatEntry->rootBlock;
  record[ATTRCAT_OFFSET_INDEX].nVal = (double)attrCatEntry->offset;
}

int AttrCacheTable::setAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatBuf) {
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }
  if (attrCache[relId] == NULL) {
    return E_RELNOTOPEN;
  }

  for (AttrCacheEntry *entry = attrCache[relId]; entry != NULL; entry = entry->next) {
    if (strcmp(entry->attrCatEntry.attrName, attrName) == 0) {
      entry->attrCatEntry = *attrCatBuf;
      entry->dirty = true;
      return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}

int AttrCacheTable::setAttrCatEntry(int relId, int attrOffset, AttrCatEntry *attrCatBuf) {
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }
  if (attrCache[relId] == NULL) {
    return E_RELNOTOPEN;
  }

  for (AttrCacheEntry *entry = attrCache[relId]; entry != NULL; entry = entry->next) {
    if (entry->attrCatEntry.offset == attrOffset) {
      entry->attrCatEntry = *attrCatBuf;
      entry->dirty = true;
      return SUCCESS;
    }
  }

  return E_ATTRNOTEXIST;
}