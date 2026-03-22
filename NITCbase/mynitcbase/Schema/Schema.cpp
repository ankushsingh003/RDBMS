#include "Schema.h"

#include <math.h>
#include <string.h>

int Schema::createRel(char relName[], int numOfAttributes, char attrNames[][ATTR_SIZE], int attrType[]) {
  Attribute attrVal;
  strcpy(attrVal.sVal, relName);

  RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, attrVal, EQ);
  if (relcatRecId.block != -1 && relcatRecId.slot != -1) {
    return E_RELEXIST;
  }

  for (int i = 0; i < numOfAttributes; i++) {
    for (int j = i + 1; j < numOfAttributes; j++) {
      if (strcmp(attrNames[i], attrNames[j]) == 0) {
        return E_DUPLICATEATTR;
      }
    }
  }

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, relName);
  relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = numOfAttributes;
  relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0;
  relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1;
  relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1;
  relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = floor(2016.0 / (16.0 * numOfAttributes + 1.0));

  int ret = BlockAccess::insert(RELCAT_RELID, relCatRecord);
  if (ret != SUCCESS) {
    return ret;
  }

  for (int i = 0; i < numOfAttributes; i++) {
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName);
    strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrNames[i]);
    attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrType[i];
    attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;
    attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;
    attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = i;

    ret = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);
    if (ret != SUCCESS) {
      Schema::deleteRel(relName); // Cleanup
      return ret;
    }
  }

  RelCatEntry relCatBuf;
  RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatBuf);
  relCatBuf.numRecs++;
  RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatBuf);

  RelCatEntry attrCatRelCatBuf;
  RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrCatRelCatBuf);
  attrCatRelCatBuf.numRecs += numOfAttributes;
  RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrCatRelCatBuf);

  return SUCCESS;
}

int Schema::deleteRel(char relName[ATTR_SIZE]) {
  if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0) {
    return E_NOTPERMITTED;
  }

  int relId = OpenRelTable::getRelId(relName);
  if (relId != E_RELNOTEXIST) {
    return E_RELOPEN;
  }

  return BlockAccess::deleteRelation(relName);
}

int Schema::openRel(char relName[ATTR_SIZE]) {
  int ret = OpenRelTable::openRel(relName);
  if (ret >= 0) {
    return SUCCESS;
  }
  return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE]) {
  if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0) {
    return E_NOTPERMITTED;
  }

  int relId = OpenRelTable::getRelId(relName);
  if (relId < 0) {
    return relId;
  }

  return OpenRelTable::closeRel(relId);
}

int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE]) {
  if (strcmp(oldRelName, RELCAT_RELNAME) == 0 || strcmp(oldRelName, ATTRCAT_RELNAME) == 0 ||
      strcmp(newRelName, RELCAT_RELNAME) == 0 || strcmp(newRelName, ATTRCAT_RELNAME) == 0) {
    return E_NOTPERMITTED;
  }

  if (OpenRelTable::getRelId(oldRelName) != E_RELNOTEXIST) {
    return E_RELOPEN;
  }

  return BlockAccess::renameRelation(oldRelName, newRelName);
}

int Schema::renameAttr(char relName[ATTR_SIZE], char oldAttrName[ATTR_SIZE], char newAttrName[ATTR_SIZE]) {
  if (strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0) {
    return E_NOTPERMITTED;
  }

  if (OpenRelTable::getRelId(relName) != E_RELNOTEXIST) {
    return E_RELOPEN;
  }

  return BlockAccess::renameAttribute(relName, oldAttrName, newAttrName);
}
