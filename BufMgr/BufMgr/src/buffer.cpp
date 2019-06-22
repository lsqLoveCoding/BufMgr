/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB
 *
 * This file contains information for the buffer pool
 * This file is the main implementation for the Badger DB. It allows for the allocation of buffer pages and files
 * And also the implementation for reading and writing pages to/from memory. It functions as a buffer manager
 * And is used in coordination with 'main.cpp' to run tests on the methods below
 *
 * Student name: 刘思琦
 * Student id: 1163710228
 * Student email: hit1163710228@163.com
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

	/**
	 * Constructor of BufMgr class
	 */
	BufMgr::BufMgr(std::uint32_t bufs)
		: numBufs(bufs) {
		bufDescTable = new BufDesc[bufs];

		for (FrameId i = 0; i < bufs; i++)
		{
			bufDescTable[i].frameNo = i;
			bufDescTable[i].valid = false;
		}

		bufPool = new Page[bufs];

		int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
		hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

		clockHand = bufs - 1;
	}

	/**
	 * Destructor, used to flushes out all dirty pages and deallocates the buﬀer pool and the BufDesc table
	 * 析构函数，把脏数据写入磁盘。清空缓存池，BufDesc表和hash表
	 *
	 * @parameter
	 * @return
	 */
	BufMgr::~BufMgr() {
		for (FrameId i = 0; i < numBufs; i++) {
			if (bufDescTable[i].dirty) {
				bufDescTable[i].file->writePage(bufPool[i]);
				bufDescTable[i].dirty = false;
			}
		}
		delete hashTable; // Deallocate the buffer hash table
		delete[] bufDescTable; // Deallocate the bufDesc table
		delete[] bufPool; // Deallocate the buﬀer pool
	}

	/**
	 * Advance clock to next frame in the buﬀer pool and ensure no overflow
	 * 时钟移动函数，使时钟移动到下一个帧。用取余的方式来防止overflow
	 *
	 * @parameter
	 * @return
	 */
	void BufMgr::advanceClock()
	{
		clockHand = (clockHand + 1) % numBufs;
	}

	/**
	 * Allocates a free frame using the clock algorithm, if necessary, writing a dirty page back to disk.
	 * 使用时钟算法分配空闲帧
	 * 
	 * @parameter frame    Frame reference, frame ID of allocated frame returned via this variable
	 * @return
	 * @throws:BufferExceededException    When no such buffer is found which can be allocated
	 */
	void BufMgr::allocBuf(FrameId & frame) 
	{
		while (true) {
			for (int i = 0; i != (signed)numBufs; i++) {
				advanceClock();
				//当某个帧的valid位为false时，说明这个页面不可用，
				//它就可以被清理掉从而腾出需要的空闲帧，函数返回
				if (!bufDescTable[clockHand].valid) {
					frame = clockHand;
					return;
				}
				//当某个帧的refbit位为true时，说明这个页面最近被使用过并且未被替换，
				//因此该位置不是空闲帧，进入下一次循环
				if (bufDescTable[clockHand].refbit) {
					bufDescTable[clockHand].refbit = false;
					continue;
				}
				//当某个帧的pinCnt>0时，说明这个页面正在被使用，不是空闲帧
				if (bufDescTable[clockHand].pinCnt > 0) {
					continue;
				}
				//当某个帧的dirty位为true时，说明这个页面是脏的，应当将该页面写回磁盘
				if (bufDescTable[clockHand].dirty) {
					bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
				}            
				try {
					if (bufDescTable[clockHand].file) {
						hashTable->remove(bufDescTable[clockHand].file, 
						bufDescTable[clockHand].pageNo);
						bufDescTable[clockHand].Clear();
					}                  
				}catch (HashNotFoundException &e) {
					}
				frame = clockHand;
				return;                
			}

			//当所有的页面都被占用时，抛出异常
			bool allPinned = false;
			for (int i = 0; i < (signed)numBufs; i++) {
				if (!bufDescTable[i].pinCnt) {
					allPinned = true;
					break;
				}
			}
			if (!allPinned) {
				throw BufferExceededException();
			}
		}
	}

	/**
	 * Read the given page from the file into a frame and return the pointer to page
	 * If the requested page is already present in the buffer pool, pointer to that frame is returned
	 * Otherwise a new frame is allocated from the buffer pool to read the page
	 * 将文件中的给定页读入帧，并将指针返回到页面。如果请求的页已经存在于缓冲池中，则返回指向该帧的指针。否则，从缓冲池中分配新的帧来读取页
	 *
	 * @param file    File object
	 * @param PageNo    Page number in the file to be read
	 * @param page    Reference to page pointer. Used to fetch the Page object in which requested page from file is read in
	 */
	void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
	{
		FrameId id;
		try {
			// Page is in the buffer pool
			hashTable->lookup(file, pageNo, id);
			bufDescTable[id].pinCnt++;
		}
		catch (HashNotFoundException e) {
			// Page is not in the buffer pool.
			// Allocate a buffer frame. Read the page from disk
			// Insert the page into the hashtable. Set the frame
			Page pageTemp = file->readPage(pageNo);
			this->allocBuf(id);
			bufPool[id] = pageTemp;
			hashTable->insert(file, pageNo, id);
			bufDescTable[id].Set(file, pageNo);
		}
		bufDescTable[id].refbit = true;
		// Return a pointer to the frame containing the page
		page = &bufPool[id];
	}

	/**
	 * Unpin a page from memory since it is no longer required for it to remain in memory
	 *
	 * @param file    File object
	 * @param PageNo    Page number
	 * @param dirty    True if the page to be unpinned needs to be marked dirty
	 * @throws PageNotPinnedException    If the page is not already pinned
	 * @throws HashNotFoundException    If page is not found in the hash table lookup
	 */
	void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
	{
		// the frame number of the page
		FrameId frameId;
		try {
			hashTable->lookup(file, pageNo, frameId);
		}
		catch (HashNotFoundException e) {
			return; // Throw HashNotFoundException if page is not found in the hash table lookup but nothing needs to do
		}
		// Throw PageNotPinnedException if the pin count is already 0
		if (bufDescTable[frameId].pinCnt == 0) {
			throw PageNotPinnedException(file->filename(), pageNo, frameId);
		}
		bufDescTable[frameId].pinCnt--;
		if (dirty) {
			bufDescTable[frameId].dirty = true; // If dirty is true, set the dirty bit
		}
	}

	/**
	 * Write out all dirty pages of the file to disk
	 * All the frames assigned to the file need to be unpinned from buffer pool before this function be successfully called
	 * Otherwise error
	 * 将文件中的所有脏页写回到磁盘。
	 *
	 * @param file    File object
	 */
	void BufMgr::flushFile(const File* file)
	{
		// Flush file to disk
		for (FrameId k = 0; k < numBufs; k++) {
			if (bufDescTable[k].file == file) {
				if (bufDescTable[k].pinCnt > 0) {
					throw PagePinnedException(file->filename(), bufDescTable[k].pageNo, k);
				}
				else if (!bufDescTable[k].valid) {
					throw BadBufferException(k, bufDescTable[k].dirty, bufDescTable[k].valid, bufDescTable[k].refbit);
				}
				else {
					if (bufDescTable[k].dirty) {
						bufDescTable[k].file->writePage(bufPool[k]);
						bufDescTable[k].dirty = false;
					}
					hashTable->remove(file, bufDescTable[k].pageNo);
					bufDescTable[k].Clear();
				}
			}
		}
	}

	/**
	 * Allocates a new, empty page in the file and returns the Page object
	 * The newly allocated page is also assigned a frame in the buffer pool
	 *
	 * @param file    File object
	 * @param PageNo    Page number, the number assigned to the page in the file is returned via this reference
	 * @param page    Reference to page pointer. The newly allocated in-memory Page object is returned via this reference
	 */
	void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
	{
		FrameId frameId;
		// Allocate an empty page in the specified file and obtain a buffer pool
		PageId newPageId = file->allocatePage().page_number();
		allocBuf(frameId);

		// Set the hash table and frame.
		bufPool[frameId] = file->readPage(newPageId);
		hashTable->insert(file, newPageId, frameId);
		bufDescTable[frameId].Set(file, newPageId);

		pageNo = newPageId;
		page = &bufPool[frameId];
	}

	/**
	 * Delete page from file and also from buffer pool if present
	 *
	 * @param file    File object
	 * @param PageNo    Page number
	 */
	void BufMgr::disposePage(File* file, const PageId PageNo)
	{
		FrameId frameId;
		try {
			// Makes sure that if the page to be deleted is allocated a frame in the buffer pool, that frame
			// is freed and correspondingly entry from hash table is also removed.
			hashTable->lookup(file, PageNo, frameId);
			bufDescTable[frameId].Clear();
			hashTable->remove(file, PageNo);
		}
		catch (HashNotFoundException e) {
		}

		file->deletePage(PageNo);
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
