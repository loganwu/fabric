/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package fsblkstorage

import (
	"os"

	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/pkg/errors"
)

type archiveMgr struct {
	ledgerID       string
	ledgerDir      string
	indexDir       string
	dbProvider     *leveldbhelper.Provider
	indexStore     *blockIndex
	targetBlockNum uint64
	configBlockNum uint64
}

// Archive reverts changes made to the block store beyond a given block number.
func Archive(blockStorageDir, ledgerID string, targetBlockNum uint64, indexConfig *blkstorage.IndexConfig) error {
	r, err := newArchiveMgr(blockStorageDir, ledgerID, indexConfig, targetBlockNum)
	if err != nil {
		return err
	}
	defer r.dbProvider.Close()

	targetBlock, err := r.getBlockByNumber(r.targetBlockNum)
	if err != nil {
		return err
	}
	r.configBlockNum, err = utils.GetLastConfigIndexFromBlock(targetBlock)
	if err != nil {
		return err
	}
	logger.Infof("Get config block number of targetblock, config number [%d]", r.configBlockNum)

	logger.Infof("Archive block index to block number [%d]", targetBlockNum)
	if err := r.archiveBlockIndex(); err != nil {
		return err
	}

	logger.Infof("Archive block files to block number [%d]", targetBlockNum)
	if err := r.archiveBlockFiles(); err != nil {
		return err
	}

	return nil
}

func newArchiveMgr(blockStorageDir, ledgerID string, indexConfig *blkstorage.IndexConfig, targetBlockNum uint64) (*archiveMgr, error) {
	r := &archiveMgr{}

	r.ledgerID = ledgerID
	conf := &Conf{blockStorageDir: blockStorageDir}
	r.ledgerDir = conf.getLedgerBlockDir(ledgerID)
	r.targetBlockNum = targetBlockNum

	r.indexDir = conf.getIndexDir()
	r.dbProvider = leveldbhelper.NewProvider(&leveldbhelper.Conf{DBPath: r.indexDir})
	var err error
	indexDB := r.dbProvider.GetDBHandle(ledgerID)
	r.indexStore, err = newBlockIndex(indexConfig, indexDB)
	return r, err
}

func (r *archiveMgr) archiveBlockIndex() error {
	batchLimit := uint64(10)
	start, limit := uint64(0), r.targetBlockNum-1
	if limit <= start {
		return nil
	}

	end := limit
	for start <= limit {
		if limit-start >= batchLimit {
			end = start + batchLimit - 1
		}
		logger.Infof("Deleting index associated with block number [%d] to [%d]", start, end)
		if err := r.deleteIndexEntriesRange(start, end); err != nil {
			return err
		}
		start = end + 1
	}
	return nil
}

func (r *archiveMgr) deleteIndexEntriesRange(startBlkNum, endBlkNum uint64) error {
	batch := leveldbhelper.NewUpdateBatch()
	lp, err := r.indexStore.getBlockLocByBlockNum(startBlkNum)
	if err != nil {
		return err
	}
	stream, err := newBlockStream(r.ledgerDir, lp.fileSuffixNum, int64(lp.offset), -1)
	defer stream.close()

	numberOfBlocksToRetrieve := endBlkNum - startBlkNum + 1
	for numberOfBlocksToRetrieve > 0 {
		blockBytes, placementInfo, err := stream.nextBlockBytesAndPlacementInfo()
		if err != nil {
			return err
		}

		blockInfo, err := extractSerializedBlockInfo(blockBytes)
		if err != nil {
			return err
		}

		if blockInfo.blockHeader.Number == r.configBlockNum {
			logger.Infof("Skip deleting index of  config block number [%d]", r.configBlockNum)
			numberOfBlocksToRetrieve--
			continue
		}

		err = populateBlockInfoWithDuplicateTxids(blockInfo, placementInfo, r.indexStore)
		if err != nil {
			return err
		}
		addIndexEntriesToBeDeleted(batch, blockInfo, r.indexStore)
		numberOfBlocksToRetrieve--
	}

	batch.Put(indexCheckpointKey, encodeBlockNum(startBlkNum-1))
	return r.indexStore.db.WriteBatch(batch, true)
}

func (r *archiveMgr) archiveBlockFiles() error {
	logger.Infof("Deleting checkpointInfo")
	if err := r.indexStore.db.Delete(blkMgrInfoKey, true); err != nil {
		return err
	}
	// must not use index for block location search since the index can be behind the target block
	targetFileNum, err := binarySearchFileNumForBlock(r.ledgerDir, r.targetBlockNum)
	if err != nil {
		return err
	}

	configBlockFile, err := binarySearchFileNumForBlock(r.ledgerDir, r.configBlockNum)
	if err != nil {
		return err
	}

	logger.Infof("Removing all block files with suffixNum in the range [0] to [%d] except config file [%d]",
		targetFileNum-1, configBlockFile)

	for n := 0; n < targetFileNum; n++ {
		if n != configBlockFile {
			filepath := deriveBlockfilePath(r.ledgerDir, n)
			if err := os.Remove(filepath); err != nil {
				return errors.Wrapf(err, "error removing the block file [%s]", filepath)
			}
		}

	}

	return nil
}

func (r *archiveMgr) getConfigBlock(blknum uint64) (*common.Block, error) {
	lastBlock, err := r.getBlockByNumber(blknum)
	if err != nil {
		return nil, err
	}

	// get most recent config block location from last block metadata
	configBlockIndex, err := utils.GetLastConfigIndexFromBlock(lastBlock)
	if err != nil {
		return nil, err
	}

	// get most recent config block
	configBlock, err := r.getBlockByNumber(configBlockIndex)
	if err != nil {
		return nil, err
	}

	return configBlock, nil
}

func (r *archiveMgr) getBlockByNumber(blockNum uint64) (*common.Block, error) {
	logger.Debugf("getBlockByNumber() - blockNum = [%d]", blockNum)

	loc, err := r.indexStore.getBlockLocByBlockNum(blockNum)
	if err != nil {
		return nil, err
	}
	return r.fetchBlock(loc)
}

func (r *archiveMgr) fetchBlock(lp *fileLocPointer) (*common.Block, error) {
	blockBytes, err := r.fetchBlockBytes(lp)
	if err != nil {
		return nil, err
	}
	block, err := deserializeBlock(blockBytes)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (r *archiveMgr) fetchBlockBytes(lp *fileLocPointer) ([]byte, error) {
	stream, err := newBlockfileStream(r.ledgerDir, lp.fileSuffixNum, int64(lp.offset))
	if err != nil {
		return nil, err
	}
	defer stream.close()
	b, err := stream.nextBlockBytes()
	if err != nil {
		return nil, err
	}
	return b, nil
}

// ValidateParams performs necessary validation
func ValidateParams(blockStorageDir, ledgerID string, targetBlockNum uint64) error {
	logger.Infof("Validating the parameters: ledgerID [%s], block number [%d]",
		ledgerID, targetBlockNum)
	conf := &Conf{blockStorageDir: blockStorageDir}
	ledgerDir := conf.getLedgerBlockDir(ledgerID)

	logger.Debugf("Validating the existance of ledgerID [%s]", ledgerID)
	exists, _, err := util.FileExists(ledgerDir)
	if err != nil {
		return err
	}
	if !exists {
		return errors.Errorf("ledgerID [%s] does not exist", ledgerID)
	}

	logger.Debugf("Validating the given block number [%d] agains the ledger block height", targetBlockNum)
	cpInfo, err := constructCheckpointInfoFromBlockFiles(ledgerDir)
	if err != nil {
		return err
	}
	if cpInfo.lastBlockNumber != targetBlockNum {
		return errors.Errorf("target block number [%d] should be the biggest block number [%d]",
			targetBlockNum, cpInfo.lastBlockNumber)
	}

	return nil

}
