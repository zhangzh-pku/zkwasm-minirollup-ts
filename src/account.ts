import mongoose, {Schema} from 'mongoose';
const accountSchema = new mongoose.Schema({
  pkx: { type: String, required: true, unique: true },
  index: { type: Number, required: true, unique: false },
  data: {
    type: Schema.Types.Mixed,
    required: true
  }
});

accountSchema.index({ index: -1 });

const accountModel = mongoose.model("Account", accountSchema);

export async function ensureAccountIndexes() {
  try {
    const collection = accountModel.collection;
    const indexes = await collection.indexes();
    const indexExists = indexes.some((index) =>
      index.key && (index.key.index === -1 || index.key.index === 1)
    );
    if (!indexExists) {
      console.log('Creating index on Account.index for existing database...');
      await collection.createIndex({ index: -1 });
      console.log('Index on Account.index created successfully');
    } else {
      console.log('Index on Account.index already exists');
    }
  } catch (error) {
    const mongoErr = error as any;
    if (mongoErr?.code === 26 || mongoErr?.codeName === 'NamespaceNotFound') {
      console.log('Creating index on Account.index for new database...');
      await accountModel.collection.createIndex({ index: -1 });
      console.log('Index on Account.index created successfully');
      return;
    }
    console.error('Error ensuring Account indexes:', error);
  }
}

export async function storeAccount(pkx: string, data: JSON, indexer: (data:any)=> number) {
  const index = indexer(data);
  await accountModel.findOneAndUpdate(
    { pkx: pkx},  // Filter: finds the document by key
    { $set: {
        index: index,
        data: data
    } },       // Update operation: sets new data
    { new: true, upsert: true} // Options: return the new doc, upsert if not found, and run validations
  );
}

export async function queryAccounts(start: number) {
  try {
    let doc = await accountModel.find()
    .sort({index: -1})
    .skip(start).limit(100)
    .lean();
    return doc;
  } catch(e) {
    return []
  }
}
