/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/ledgerstorage"
	"github.com/pkg/errors"
)

// ArchiveKVLedger Archive a ledger to a specified block number
func ArchiveKVLedger(ledgerID string, blockNum uint64) error {
	fileLock := leveldbhelper.NewFileLock(ledgerconfig.GetFileLockPath())
	if err := fileLock.Lock(); err != nil {
		return errors.Wrap(err, "as another peer node command is executing,"+
			" wait for that command to complete its execution or terminate it before retrying")
	}
	defer fileLock.Unlock()

	blockstorePath := ledgerconfig.GetBlockStorePath()

	if err := ledgerstorage.ValidateParams(blockstorePath, ledgerID, blockNum); err != nil {
		return err
	}

	logger.Infof("store database snapshot")
	/* TODO loganwu
	if err := storeDatabaseSnapshot(); err != nil {
		return err
	}
	*/

	logger.Info("Archive  ledger store")
	if err := ledgerstorage.Archive(blockstorePath, ledgerID, blockNum); err != nil {
		return err
	}
	logger.Infof("The channel [%s] has been successfully archive to the block number [%d]", ledgerID, blockNum)
	return nil
}
