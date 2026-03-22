#include "Algebra.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "../BlockAccess/BlockAccess.h"

bool isNumber(char *str) {
  int len = strlen(str);
  int decimalCount = 0;
  if (len == 0) return false;
  for (int i = 0; i < len; i++) {
    if (str[i] == '.') {
      decimalCount++;
    } else if (str[i] < '0' || str[i] > '9') {
      if (i == 0 && str[i] == '-') continue;
      return false;
    }
  }
  return decimalCount <= 1;
}

/* 
 * This function will be called by the Frontend to handle the SELECT command.
 * For now, it performs a linear search on the source relation and prints 
 * the records that satisfy the condition.
 */
int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
  int srcRelId = OpenRelTable::getRelId(srcRel);      // Get the relId of the source relation
  if (srcRelId == E_RELNOTEXIST) {
    return E_RELNOTOPEN;
  }

  AttrCatEntry attrCatBuf;
  // Get the attribute catalog entry for the attribute mentioned in the WHERE clause
  int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatBuf);
  if (ret != SUCCESS) {
    return ret;
  }

  // Convert the string value to the appropriate type
  Attribute attrVal;
  if (attrCatBuf.attrType == NUMBER) {
    if (isNumber(strVal)) {
      attrVal.nVal = atof(strVal);
    } else {
      return E_ATTRTYPEMISMATCH;
    }
  } else {
    strcpy(attrVal.sVal, strVal);
  }

  /*** Build the searchIndex in the cache to start from the beginning ***/
  RelCacheTable::resetSearchIndex(srcRelId);

  RelCatEntry srcRelCatBuf;
  RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatBuf);
  int numAttrs = srcRelCatBuf.numAttrs;

  // 1. Prepare attribute names and types for the target relation
  char (*attr_names)[ATTR_SIZE] = (char (*)[ATTR_SIZE])malloc(sizeof(char[ATTR_SIZE]) * numAttrs);
  int *attr_types = (int *)malloc(sizeof(int) * numAttrs);
  for (int i = 0; i < numAttrs; i++) {
    AttrCatEntry attrEntry;
    AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrEntry);
    strcpy(attr_names[i], attrEntry.attrName);
    attr_types[i] = attrEntry.attrType;
  }

  // 2. Create target relation
  ret = Schema::createRel(targetRel, numAttrs, attr_names, attr_types);
  if (ret != SUCCESS) {
    free(attr_names);
    free(attr_types);
    return ret;
  }
  free(attr_names);
  free(attr_types);

  // 3. Open target relation
  int targetRelId = OpenRelTable::openRel(targetRel);
  if (targetRelId < 0) {
    Schema::deleteRel(targetRel);
    return targetRelId;
  }

  // 4. Iterate through matching records and insert into target relation
  Attribute record[numAttrs];
  while (true) {
    RecId matchRecId = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);
    if (matchRecId.block != -1 && matchRecId.slot != -1) {
      RecBuffer block(matchRecId.block);
      block.getRecord(record, matchRecId.slot);

      ret = BlockAccess::insert(targetRelId, record);
      if (ret != SUCCESS) {
        OpenRelTable::closeRel(targetRelId);
        Schema::deleteRel(targetRel);
        return ret;
      }
    } else {
      break;
    }
  }

  // 5. Close target relation
  OpenRelTable::closeRel(targetRelId);

  return SUCCESS;
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]) {
  if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0) {
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
      if (isNumber(record[i])) {
        relRecord[i].nVal = atof(record[i]);
      } else {
        return E_ATTRTYPEMISMATCH;
      }
    } else {
      strcpy(relRecord[i].sVal, record[i]);
    }
  }

  return BlockAccess::insert(relId, relRecord);
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {
  int srcRelId = OpenRelTable::getRelId(srcRel);
  if (srcRelId == E_RELNOTEXIST) {
    return E_RELNOTOPEN;
  }

  RelCatEntry srcRelCatBuf;
  RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatBuf);
  int numAttrs = srcRelCatBuf.numAttrs;

  char (*attr_names)[ATTR_SIZE] = (char (*)[ATTR_SIZE])malloc(sizeof(char[ATTR_SIZE]) * numAttrs);
  int *attr_types = (int *)malloc(sizeof(int) * numAttrs);
  for (int i = 0; i < numAttrs; i++) {
    AttrCatEntry attrEntry;
    AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrEntry);
    strcpy(attr_names[i], attrEntry.attrName);
    attr_types[i] = attrEntry.attrType;
  }

  int ret = Schema::createRel(targetRel, numAttrs, attr_names, attr_types);
  if (ret != SUCCESS) {
    free(attr_names);
    free(attr_types);
    return ret;
  }
  free(attr_names);
  free(attr_types);

  int targetRelId = OpenRelTable::openRel(targetRel);
  if (targetRelId < 0) {
    Schema::deleteRel(targetRel);
    return targetRelId;
  }

  RelCacheTable::resetSearchIndex(srcRelId);
  Attribute record[numAttrs];
  while (BlockAccess::project(srcRelId, record) == SUCCESS) {
    ret = BlockAccess::insert(targetRelId, record);
    if (ret != SUCCESS) {
        OpenRelTable::closeRel(targetRelId);
        Schema::deleteRel(targetRel);
        return ret;
    }
  }

  OpenRelTable::closeRel(targetRelId);

  return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE]) {
  int srcRelId = OpenRelTable::getRelId(srcRel);
  if (srcRelId == E_RELNOTEXIST) {
    return E_RELNOTOPEN;
  }

  RelCatEntry srcRelCatBuf;
  RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatBuf);
  int src_nAttrs = srcRelCatBuf.numAttrs;

  int *attr_offsets = (int *)malloc(sizeof(int) * tar_nAttrs);
  int *attr_types = (int *)malloc(sizeof(int) * tar_nAttrs);

  for (int i = 0; i < tar_nAttrs; i++) {
    AttrCatEntry attrEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &attrEntry);
    if (ret != SUCCESS) {
        free(attr_offsets);
        free(attr_types);
        return ret;
    }
    attr_offsets[i] = attrEntry.offset;
    attr_types[i] = attrEntry.attrType;
  }

  int ret = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);
  if (ret != SUCCESS) {
    free(attr_offsets);
    free(attr_types);
    return ret;
  }

  int targetRelId = OpenRelTable::openRel(targetRel);
  if (targetRelId < 0) {
    Schema::deleteRel(targetRel);
    free(attr_offsets);
    free(attr_types);
    return targetRelId;
  }

  RelCacheTable::resetSearchIndex(srcRelId);
  Attribute srcRecord[src_nAttrs];
  while (BlockAccess::project(srcRelId, srcRecord) == SUCCESS) {
    Attribute tarRecord[tar_nAttrs];
    for (int i = 0; i < tar_nAttrs; i++) {
        tarRecord[i] = srcRecord[attr_offsets[i]];
    }
    ret = BlockAccess::insert(targetRelId, tarRecord);
    if (ret != SUCCESS) {
        OpenRelTable::closeRel(targetRelId);
        Schema::deleteRel(targetRel);
        free(attr_offsets);
        free(attr_types);
        return ret;
    }
  }

  free(attr_offsets);
  free(attr_types);

  OpenRelTable::closeRel(targetRelId);

  return SUCCESS;
}

int Algebra::display(char relName[ATTR_SIZE]) {
  int relId = OpenRelTable::getRelId(relName);
  if (relId == E_RELNOTEXIST) {
    return E_RELNOTOPEN;
  }

  RelCatEntry relCatBuf;
  RelCacheTable::getRelCatEntry(relId, &relCatBuf);
  int numAttrs = relCatBuf.numAttrs;

  // Print header
  for (int i = 0; i < numAttrs; i++) {
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, i, &attrCatEntry);
    printf("%s | ", attrCatEntry.attrName);
  }
  printf("\n");
  for (int i = 0; i < numAttrs; i++) printf("--- | ");
  printf("\n");

  RelCacheTable::resetSearchIndex(relId);
  Attribute record[numAttrs];
  while (BlockAccess::project(relId, record) == SUCCESS) {
    for (int i = 0; i < numAttrs; i++) {
      AttrCatEntry attrCatEntry;
      AttrCacheTable::getAttrCatEntry(relId, i, &attrCatEntry);
      if (attrCatEntry.attrType == NUMBER) {
        printf("%g | ", record[i].nVal);
      } else {
        printf("%s | ", record[i].sVal);
      }
    }
    printf("\n");
  }

  return SUCCESS;
}