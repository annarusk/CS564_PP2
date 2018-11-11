/**
 * Authors: Jim Twieg (9044486265)
 * 	Anna Rusk
 * 	Katie Zutter
 * 
 * Buffer.cpp defines the BufMgr class and necessary functions
 * to act as a simple DBMS buffer manager using a Clock replacement
 * policy.
 * 
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	/* Deconstructor for the buffer manager.
	 * Flush any dirty pages to disk then deallocate
	 * buffer pool, hash table, and buffer description table
	 */
	for (FrameId i=0; i<numBufs; i++) {
		if (bufDescTable[i].dirty) {
			bufDescTable[i].file->writePage(bufPool[i]);
		}
	}
	delete[] bufPool;
	delete hashTable;
	delete[] bufDescTable;
}

void BufMgr::advanceClock()
{
	/* Advances to next frame in bufPool according to Clock
	 * replacement policy, returning to 0 as needed
	 */
	clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
	/*	Allocate free frame using clock policy.
	 *	If replacing frame, remove from hashTable and write to disk if dirty.
	 *	Throw exception if all frames pinned.
	 *	Not threadsafe.
	 */
	FrameId startFrame = clockHand; // Initial frame, so we know we went around
	bool frameAvail = false; // Track whether there is an unpinned page in buffer
	
	while(true) {
		if(!bufDescTable[clockHand].valid) {
			// Always choose if current frame invalid.
			frame = clockHand;
			advanceClock();
			return;
		}
		else if(bufDescTable[clockHand].pinCnt > 0) {
			// If current frame in use, dereference and skip.
			bufDescTable[clockHand].refbit = 0;
			advanceClock();
		}
		else if(bufDescTable[clockHand].refbit == 1) {
			// If current frame not in use, but referenced, dereference and skip.
			bufDescTable[clockHand].refbit = 0;
			frameAvail = true; // There is at least one frame open
			advanceClock();
		}
		else {
			// Valid, unpinned, unreferenced -> Replace frame
			if(bufDescTable[clockHand].dirty) {
				// Need to write dirty frame to disk before replacing
				bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
				bufDescTable[clockHand].dirty = false;
			}
			// Need to remove reference to existing frame from HashTable
			hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
			bufDescTable[clockHand].Clear();
			frame = clockHand;
			advanceClock();
			return;
		}
		// No Frames available
		if (clockHand == startFrame && frameAvail == false) {
			throw BufferExceededException();
		}
	}
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	/*	Invoke hashTable->lookup to see if page is already in buffer.
	 *	- If HashNotFound, allocate frame and read page into buffer.
	 *	Insert page into hashTable. Set entry in bufDescTable.
	 *	Return pointer to frame in "page"
	 *	- If page found, reset refbit, increment pinCnt, return pointer 
	 *	to frame in "page"
	 */
	FrameId frame;
	try {
		hashTable->lookup(file, pageNo, frame);
		// Page found
		bufDescTable[frame].refbit = 1;
		bufDescTable[frame].pinCnt++;
		page = &bufPool[frame];
  	}
	catch (HashNotFoundException e) {
		// Page not found, read into buffer from file.
    	allocBuf(frame);
    	bufPool[frame] = file->readPage(pageNo);
    	hashTable->insert(file, pageNo, frame);
    	bufDescTable[frame].Set(file, pageNo);
    	page = &bufPool[frame];
  	}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	/*	Decrement pinCnt, possibly set dirty bit.
	 *	Throw PageNotPinned if pinCnt=0.
	 */
	FrameId frame;
	try {
		hashTable->lookup(file, pageNo, frame);
		if (bufDescTable[frame].pinCnt == 0) {
			throw PageNotPinnedException(file->filename(), pageNo, frame);
		} else {
			bufDescTable[frame].pinCnt--;
			if (dirty) bufDescTable[frame].dirty = true;
		}
	}
	catch (HashNotFoundException e) {
		// Do nothing if page not in buffer
		return;
	}
}

void BufMgr::flushFile(const File* file) 
{
	/*	Scan bufDescTable for pages in buffer for file.
	 *	- if frame is dirty, write to disk and unset dirty bit
	 * Remove page from hashTable, clear entry in bufDescTable
	 * Need to check for frames which are pinned or invalid.
	 */
	for(FrameId i=0; i<numBufs; i++) {
		if(bufDescTable[i].file == file) {
			// Check for error conditions
			if(bufDescTable[i].pinCnt > 0)
				throw PagePinnedException(file->filename(), bufDescTable[i].pageNo,i);
			if(!bufDescTable[i].valid)
				throw BadBufferException(i,bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			if(bufDescTable[i].dirty) { // Dirty page needs to be written to disk
				bufDescTable[i].file->writePage(bufPool[i]);
				bufDescTable[i].dirty = false;
			}
			hashTable->remove(file,bufDescTable[i].pageNo);
			bufDescTable[i].Clear();
		}
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	/*	Create empty page with file->allocatePage.
	 *	Get buffer pool frame with allocBuf.
	 *	Store in hashtable with HashTable->insert.
	 *	Set entry in bufDescTable.
	 *	Return page number created and pointer to frame.
	 */
	FrameId frame;
	allocBuf(frame);
	bufPool[frame] = file->allocatePage();
	pageNo = bufPool[frame].page_number();
	hashTable->insert(file, pageNo, frame);
	bufDescTable[frame].Set(file,pageNo);
	page = &bufPool[frame];
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    /*	Deletes page from file.
	 *	First checks to see if page is in buffer pool, 
	 *	then frees frame and removes entry from hashTable.
	 */
	FrameId frame;
	try {
		hashTable->lookup(file, PageNo, frame);
		// Page in buffer, need to clear references.
		hashTable->remove(file, PageNo);
		bufDescTable[frame].Clear();
		file->deletePage(PageNo);
	}
	catch (HashNotFoundException e) {
		// Page not found in buffer, just remove from file.
		file->deletePage(PageNo);
		return;
	}
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
