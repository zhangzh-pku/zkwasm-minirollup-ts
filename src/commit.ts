import mongoose from 'mongoose';
import { TxWitness } from './prover.js';

const ASYNC_COMMIT_WRITES = process.env.ASYNC_COMMIT_WRITES === '1';
const BATCH_COMMIT_WRITES = process.env.BATCH_COMMIT_WRITES === '1';
const COMMIT_BATCH_SIZE = Number.parseInt(process.env.COMMIT_BATCH_SIZE ?? "200", 10);
const COMMIT_FLUSH_MS = Number.parseInt(process.env.COMMIT_FLUSH_MS ?? "50", 10);
const COMMIT_BATCH_FATAL = process.env.COMMIT_BATCH_FATAL === '1';

function clampPositiveInt(value: number, fallback: number): number {
  if (!Number.isFinite(value) || value <= 0) return fallback;
  return Math.floor(value);
}

const COMMIT_BATCH_SIZE_VALUE = clampPositiveInt(COMMIT_BATCH_SIZE, 200);
const COMMIT_FLUSH_MS_VALUE = clampPositiveInt(COMMIT_FLUSH_MS, 50);
const txSchema = new mongoose.Schema({
  msg: { type: String, required: true },
  pkx: { type: String, required: true },
  pky: { type: String, required: true },
  sigx: { type: String, required: true },
  sigy: { type: String, required: true },
  sigr: { type: String, required: true },
});

const commitSchema = new mongoose.Schema({
    key: { type: String, required: true },
    items: { type: [txSchema], default: [] } // List of objects
});

// Add index on key field for fast lookups
commitSchema.index({ key: 1 });

export const CommitModel = mongoose.model('Commit', commitSchema);

// Ensure indexes exist on existing collections - check and create if needed
const ensureIndexes = async () => {
  try {
    const collection = CommitModel.collection;
    const indexes = await collection.indexes();
    const keyIndexExists = indexes.some(index => 
      index.key && index.key.key === 1
    );
    
    if (!keyIndexExists) {
      console.log('Creating index on key field for existing database...');
      await collection.createIndex({ key: 1 });
      console.log('Index on key field created successfully');
    } else {
      console.log('Index on key field already exists');
    }
  } catch (error) {
    const mongoErr = error as any;
    if (mongoErr?.code === 26 || mongoErr?.codeName === 'NamespaceNotFound') {
      console.log('Creating index on key field for new database...');
      await CommitModel.collection.createIndex({ key: 1 });
      console.log('Index on key field created successfully');
      return;
    }
    console.error('Error ensuring indexes:', error);
  }
};

// Export function to be called after MongoDB connection
export { ensureIndexes };

export class TxStateManager {
    currentUncommitMerkleRoot: string;
    uncommittedTxs: TxWitness[];
    preemptcounter: number;

    private pendingWrites: Array<{ key: string; tx: TxWitness }> = [];
    private flushTimer: ReturnType<typeof setTimeout> | null = null;
    private flushInFlight: Promise<void> | null = null;

    constructor(merkleRootHexString: string) {
      this.currentUncommitMerkleRoot = merkleRootHexString;
      this.uncommittedTxs = [];
      this.preemptcounter = 0;
    }

    async getTxFromCommit(key: string): Promise<TxWitness[]> {
      try {
        const commit = await CommitModel.findOne({ key });
        if (commit) {
          console.info(`replay uncommitted transactions for commit ${key}: total ${commit.items.length}`);
          return commit.items.map((x) => {
            return {
              msg: x.msg,
              pkx: x.pkx,
              pky: x.pky,
              sigx: x.sigx,
              sigy: x.sigy,
              sigr: x.sigr,
            };
          });
        } else {
          console.info(`non transactions recorded for commit ${key}`);
          return [];
        }
      } catch (error) {
        console.info(`non transactions recorded for commit ${key}`);
        return [];
      }
    }
    async loadCommit(key: string) {
      this.currentUncommitMerkleRoot = key;
      this.uncommittedTxs = [];
      this.preemptcounter = 0;
      try {
        const commit = await CommitModel.findOne({ key });
        if (commit) {
          console.info(`load commit ${key}: total uncommitted ${commit.items.length}`);
          // Do NOT set preemptcounter to commit.items.length
          // The counter should start from 0 for replay to work correctly
          // Replay transactions will match against existing items using counter as index
        } else {
          console.info(`non transactions recorded for commit ${key}`);
        }
      } catch (error) {
        console.info(`fatal: can not load target commit`);
        process.exit(1);
      }
    }


    async moveToCommit(key: string) {
      try {
        await this.flushPending("moveToCommit");

        this.currentUncommitMerkleRoot = key;
        this.preemptcounter = 0;
        this.uncommittedTxs = [];
        await CommitModel.findOneAndUpdate({
          key: key
        }, {
          key: key,
          items: []
        }, {
          upsert: true
        });
      } catch (error) {
        console.info(`fatal: clear commits should not fail`);
        process.exit(1);
      }
    }

    async insertTxIntoCommit (tx: TxWitness, isReplay = false): Promise<boolean>{
      const key = this.currentUncommitMerkleRoot;
      this.preemptcounter += 1;
      if (isReplay) {
        return true;
      }
      try {
        if (BATCH_COMMIT_WRITES) {
          this.pendingWrites.push({ key, tx });
          if (this.pendingWrites.length >= COMMIT_BATCH_SIZE_VALUE) {
            const flush = this.flushPending("batch_full");
            if (ASYNC_COMMIT_WRITES) {
              void flush;
            } else {
              await flush;
            }
          } else {
            this.scheduleFlush();
          }
        } else {
          const write = CommitModel.updateOne(
            { key },
            { $setOnInsert: { key }, $push: { items: tx } },
            { upsert: true }
          );
          if (ASYNC_COMMIT_WRITES) {
            void write.catch((error) => {
              console.error('Error inserting tx into current bundle:', error);
            });
          } else {
            await write;
          }
        }
        return false; // new tx, needs track
      } catch (error) {
        console.error('Error inserting tx into current bundle:', error);
        throw (error)
      }
    };

    async flushPending(reason = "manual") {
      if (!BATCH_COMMIT_WRITES) return;
      if (this.flushTimer) {
        clearTimeout(this.flushTimer);
        this.flushTimer = null;
      }

      if (this.flushInFlight) {
        await this.flushInFlight;
        if (this.pendingWrites.length === 0) return;
      }

      this.flushInFlight = this.flushInternal(reason)
        .catch((error) => {
          console.error(`Error flushing commit batch (${reason}):`, error);
          if (COMMIT_BATCH_FATAL || !ASYNC_COMMIT_WRITES) {
            console.error('fatal: commit batch flush failed ... process will terminate');
            process.exit(1);
          }
        })
        .finally(() => {
          this.flushInFlight = null;
        });

      await this.flushInFlight;
    }

    private scheduleFlush() {
      if (!BATCH_COMMIT_WRITES) return;
      if (this.flushTimer) return;
      const flushMs = COMMIT_FLUSH_MS_VALUE;
      this.flushTimer = setTimeout(() => {
        this.flushTimer = null;
        void this.flushPending("timer");
      }, flushMs);
    }

    private async flushInternal(reason: string) {
      const batchSize = COMMIT_BATCH_SIZE_VALUE;
      while (this.pendingWrites.length > 0) {
        const key = this.pendingWrites[0]!.key;
        const batch: TxWitness[] = [];
        while (
          this.pendingWrites.length > 0 &&
          this.pendingWrites[0]!.key === key &&
          batch.length < batchSize
        ) {
          batch.push(this.pendingWrites.shift()!.tx);
        }

        const write = CommitModel.updateOne(
          { key },
          { $setOnInsert: { key }, $push: { items: { $each: batch } } },
          { upsert: true }
        );
        await write;
      }
    }
}
