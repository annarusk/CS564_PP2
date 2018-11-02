/**
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
	/* Flush dirty pages to disk, deallocate buffer pool and bufDescTable */

}

void BufMgr::advanceClock()
{
	clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
	/*	Allocate free frame using clock policy.
		If replacing frame, remove from hashTable and write to disk if dirty.
		Throw exception if all frames pinned.
	*/
	FrameId startFrame = clockHand;
	bool frameAvail = false;
	
	while(true) {
		if(!bufDescTable[clockHand].valid) {
			// Always choose if current frame invalid.
			//bufDescTable[clockHand].valid = true;
			//bufDescTable[clockHand].pinCnt = 0;
			frame = clockHand;
			advanceClock();
			return;
		}
		else if(bufDescTable[clockHand].pinCnt > 0) {
			advanceClock();
		}
		else if(bufDescTable[clockHand].refbit == 1) {
			bufDescTable[clockHand].refbit = 0;
			frameAvail = true;
			advanceClock();
		}
		else {
			// Valid, unpinned, unreferenced -> Replace frame
			if(bufDescTable[clockHand].dirty) {
				// write to disk?
			}
			//bufDescTable[clockHand].pinCnt = 0;
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
		- If HashNotFound, call allocBuf then file->readPage. Insert 
		page into hashTable. Set entry in bufDescTable.
		Return pointer to frame in "page"
		- If page found, set refbit, increment pinCnt, return pointer 
		to frame in "page"
	*/
	//hashTable->insert();
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	/*	Decrement pinCnt, possibly set dirty bit.
		Throw PageNotPinned if pinCnt=0.
	*/
}

void BufMgr::flushFile(const File* file) 
{
	/*	Scan bufDescTable for pages in buffer for file.
		- if dirty, call file->writePage, unset dirty bit
		- remove page from hashTable
		- Clear entry in bufDescTable
	*/
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	/*	Create empty page with file->allocatePage.
		Get buffer pool frame with allocBuf.
		Store in hashtable with HashTable->insert.
		Set entry in bufDescTable.
		Return page number created and pointer to frame.
	*/
	FrameId frame;
	Page newPage = file->allocatePage();
	pageNo = newPage.page_number();
	allocBuf(frame);
	hashTable->insert(file, pageNo, frame);
	bufDescTable[frame].Set(file,pageNo);
	page = &bufPool[frame];

	std::cout << "Selected: " << frame << "\n";
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    /*	Deletes page from file.
		First checks to see if page is in buffer pool, and 
		frees frame and removes entry from hashTable.
	*/
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
