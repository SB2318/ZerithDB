import Dexie, { type Table, liveQuery } from "dexie";
import { v7 as uuidv7 } from "uuid";
import type {
  ZerithDBConfig,
  Document,
  QueryFilter,
  InsertResult,
  UpdateSpec,
} from "zerithdb-core";
import { ZerithDBError, ErrorCode, EventEmitter } from "zerithdb-core";
import { wrapIDBOperation } from "./internal/wrap-idb-operation.js";
import type { BackupExportOptions, BackupSnapshot } from "./backup.js";

type CollectionEvents<T extends Record<string, any>> = {
  mutation: { collectionName: string; doc: Document<T>; type: "insert" | "update" | "delete" };
};

/**
 * A handle to a single named collection within the ZerithDB local database.
 * All operations are async and backed by IndexedDB.
 */
export class CollectionClient<
  T extends Record<string, any> = Record<string, any>
> extends EventEmitter<CollectionEvents<T>> {
  constructor(
    private readonly table: Table<Document<T>>,
    private readonly collectionName: string,
    private readonly peerId: string
  ) {
    super();
  }

  /**
   * Subscribe to changes in the collection.
   * Uses Dexie's liveQuery to reactively notify when documents change.
   *
   * @param callback - Function called with the updated list of all documents
   * @returns An unsubscribe function
   */
  subscribe(callback: (documents: Document<T>[]) => void): () => void {
    const observable = liveQuery(() => this.find());
    const subscription = observable.subscribe({
      next: (docs) => callback(docs),
      error: (err) => console.error(`Error in collection subscription:`, err),
    });
    return () => subscription.unsubscribe();
  }

  /**
   * Insert a new document into the collection.
   * Automatically assigns `_id`, `_createdAt`, `_updatedAt`, `_vclock`, and `_lamport`.
   */
  async insert(document: T): Promise<InsertResult> {
    if (document === null || document === undefined) {
      throw new ZerithDBError(ErrorCode.DB_WRITE_FAILED, "Document cannot be null or undefined");
    }
    const now = Date.now();
    const id = uuidv7();
    const doc: Document<T> = {
      ...document,
      _id: id,
      _createdAt: now,
      _updatedAt: now,
      _vclock: { [this.peerId]: 1 },
      _lamport: now,
      _deleted: false,
    };

    return wrapIDBOperation(
      ErrorCode.DB_WRITE_FAILED,
      `Failed to insert into collection "${this.collectionName}"`,
      async () => {
        await this.table.add(doc);
        this.emit("mutation", { collectionName: this.collectionName, doc, type: "insert" });
        return { id };
      }
    );
  }

  /**
   * Insert multiple documents in a single atomic operation.
   */
  async insertMany(documents: T[]): Promise<InsertResult[]> {
    if (!Array.isArray(documents) || documents.length === 0) {
      throw new ZerithDBError(ErrorCode.DB_WRITE_FAILED, "Documents must be a non-empty array");
    }
    for (const doc of documents) {
      if (doc === null || doc === undefined) {
        throw new ZerithDBError(
          ErrorCode.DB_WRITE_FAILED,
          "Documents array cannot contain null or undefined"
        );
      }
    }
    const now = Date.now();
    const docs = documents.map((doc) => ({
      ...doc,
      _id: uuidv7(),
      _createdAt: now,
      _updatedAt: now,
      _vclock: { [this.peerId]: 1 },
      _lamport: now,
      _deleted: false,
    })) as Document<T>[];

    return wrapIDBOperation(
      ErrorCode.DB_WRITE_FAILED,
      `Failed to bulk insert into collection "${this.collectionName}"`,
      async () => {
        await this.table.bulkAdd(docs);
        for (const doc of docs) {
          this.emit("mutation", { collectionName: this.collectionName, doc, type: "insert" });
        }
        return docs.map((d) => ({ id: d._id }));
      }
    );
  }

  /**
   * Find documents matching a filter.
   * Excludes documents marked as deleted by default.
   */
  async find(filter: QueryFilter<T> = {}): Promise<Document<T>[]> {
    return wrapIDBOperation(
      ErrorCode.DB_READ_FAILED,
      `Failed to query collection "${this.collectionName}"`,
      async () => {
        const all = await this.table.toArray();
        const compiledFilter = this.precompileRegexes(filter);
        return all.filter((doc) => !doc._deleted && this.matchesFilter(doc, compiledFilter));
      }
    );
  }

  /**
   * Find a single document by its `_id`.
   */
  async findById(id: string): Promise<Document<T> | undefined> {
    return wrapIDBOperation(
      ErrorCode.DB_READ_FAILED,
      `Failed to get document "${id}" from "${this.collectionName}"`,
      async () => {
        const doc = await this.table.get(id);
        return doc && !doc._deleted ? doc : undefined;
      }
    );
  }

  /**
   * Update documents matching a filter.
   * Returns the number of updated documents.
   */
  async update(filter: QueryFilter<T>, spec: UpdateSpec<T>): Promise<number> {
    if (
      !spec ||
      Object.keys(spec).length === 0 ||
      ((!spec.$set || Object.keys(spec.$set).length === 0) &&
        (!spec.$unset || Object.keys(spec.$unset).length === 0))
    ) {
      throw new ZerithDBError(
        ErrorCode.DB_WRITE_FAILED,
        "Update spec cannot be empty. Must provide non-empty $set or $unset."
      );
    }
    return wrapIDBOperation(
      ErrorCode.DB_WRITE_FAILED,
      `Failed to update documents in "${this.collectionName}"`,
      async () => {
        const matches = await this.find(filter);
        const now = Date.now();

        const updatedDocs = matches.map((doc) => {
          const next = this.applyUpdateSpec(doc, spec, now);
          next._vclock = { ...doc._vclock, [this.peerId]: (doc._vclock[this.peerId] || 0) + 1 };
          next._lamport = Math.max(doc._lamport, now) + 1;
          return next;
        });

        await this.table.bulkPut(updatedDocs);
        for (const doc of updatedDocs) {
          this.emit("mutation", { collectionName: this.collectionName, doc, type: "update" });
        }

        return matches.length;
      }
    );
  }

  /**
   * Logical delete documents matching a filter (tombstone).
   * Returns the number of deleted documents.
   */
  async delete(filter: QueryFilter<T>): Promise<number> {
    return wrapIDBOperation(
      ErrorCode.DB_DELETE_FAILED,
      `Failed to delete documents from "${this.collectionName}"`,
      async () => {
        const matches = await this.find(filter);
        const now = Date.now();

        const deletedDocs = matches.map((doc) => ({
          ...doc,
          _deleted: true,
          _updatedAt: now,
          _vclock: { ...doc._vclock, [this.peerId]: (doc._vclock[this.peerId] || 0) + 1 },
          _lamport: Math.max(doc._lamport, now) + 1,
        }));

        await this.table.bulkPut(deletedDocs);
        for (const doc of deletedDocs) {
          this.emit("mutation", { collectionName: this.collectionName, doc, type: "delete" });
        }

        return matches.length;
      }
    );
  }

  /**
   * For internal use by SyncEngine to apply remote updates deterministically.
   */
  async applyRemoteUpdate(doc: Document<T>): Promise<void> {
    await this.table.put(doc);
    // Note: We don't emit "mutation" here to avoid echo loops in SyncEngine
  }

  /**
   * Delete every document in the collection (Hard delete).
   */
  async clearAll(): Promise<void> {
    return wrapIDBOperation(
      ErrorCode.DB_DELETE_FAILED,
      `Failed to clear collection "${this.collectionName}"`,
      () => this.table.clear()
    );
  }

  /** Alias for {@link clearAll} */
  async clear(): Promise<void> {
    return this.clearAll();
  }

  /**
   * Count documents matching a filter.
   */
  async count(filter: QueryFilter<T> = {}): Promise<number> {
    const docs = await this.find(filter);
    return docs.length;
  }

  private applyUpdateSpec(doc: Document<T>, spec: UpdateSpec<T>, updatedAt: number): Document<T> {
    const next = {
      ...doc,
      ...(spec.$set ?? {}),
      _updatedAt: updatedAt,
    } as Record<string, any>;

    for (const key of Object.keys(spec.$unset ?? {})) {
      delete next[key];
    }

    next._id = doc._id;
    next._createdAt = doc._createdAt;

    return next as Document<T>;
  }

  private matchesFilter(doc: Document<T>, filter: QueryFilter<T>): boolean {
    for (const [key, condition] of Object.entries(filter)) {
      const fieldValue = (doc as Record<string, any>)[key];

      if (condition === null || typeof condition !== "object") {
        if (fieldValue !== condition) return false;
        continue;
      }

      // Distinguish operator objects ({ $gt: 3 }) from plain object values ({ key: "v" }).
      // Only treat as operators if at least one key starts with "$".
      const conditions = condition as Record<string, any>;
      const isOperatorObject = Object.keys(conditions).some((k) => k.startsWith("$"));

      if (!isOperatorObject) {
        // Deep equality check for plain object / array values
        if (JSON.stringify(fieldValue) !== JSON.stringify(condition)) return false;
        continue;
      }

      if ("$eq" in conditions && fieldValue !== conditions["$eq"]) return false;
      if ("$ne" in conditions && fieldValue === conditions["$ne"]) return false;
      if ("$gt" in conditions && !((fieldValue as any) > (conditions["$gt"] as never)))
        return false;
      if ("$gte" in conditions && !((fieldValue as any) >= (conditions["$gte"] as never)))
        return false;
      if ("$lt" in conditions && !((fieldValue as any) < (conditions["$lt"] as never)))
        return false;
      if ("$lte" in conditions && !((fieldValue as any) <= (conditions["$lte"] as never)))
        return false;
      if ("$in" in conditions && !(conditions["$in"] as unknown[]).includes(fieldValue))
        return false;
      if ("$nin" in conditions && (conditions["$nin"] as unknown[]).includes(fieldValue))
        return false;
      if ("$regex" in conditions) {
        let re = conditions["$regex"];
        if (typeof fieldValue !== "string") return false;
        if (!(re instanceof RegExp)) {
          re = new RegExp(re);
        }
        re.lastIndex = 0;
        if (!re.test(fieldValue)) return false;
      }
    }
    return true;
  }

  private precompileRegexes(filter: QueryFilter<T>): QueryFilter<T> {
    const compiled: Record<string, any> = {};
    for (const [key, condition] of Object.entries(filter)) {
      if (condition !== null && typeof condition === "object") {
        const conditions = { ...condition } as Record<string, any>;
        const isOperatorObject = Object.keys(conditions).some((k) => k.startsWith("$"));
        if (isOperatorObject && "$regex" in conditions) {
          const regex = conditions["$regex"];
          conditions["$regex"] = regex instanceof RegExp ? regex : new RegExp(regex);
        }
        compiled[key] = conditions;
      } else {
        compiled[key] = condition;
      }
    }
    return compiled as QueryFilter<T>;
  }
}

/**
 * Internal Dexie subclass that manages dynamic collection creation.
 * Collections are added lazily via schema version upgrades.
 */
class ZerithDBDexie extends Dexie {
  private readonly tableMap = new Map<string, Table>();
  private _currentSchema: Record<string, string> = {};
  private _pendingVersion = 0;

  constructor(appId: string) {
    super(`zerithdb_${appId}`);
  }

  /**
   * Ensure a named collection exists, creating it via a Dexie version
   * upgrade if it has not been registered yet.
   *
   * @param name - The collection name to create or retrieve
   * @returns The Dexie {@link Table} handle for the collection
   */
  ensureCollection(name: string): Table {
    if (!this.tableMap.has(name)) {
      this._currentSchema[name] = "_id, _createdAt, _updatedAt, _lamport, _deleted";
      this._currentSchema["_sync_logs"] = "++_id, collectionName, docId, timestamp";
      
      // We must increment the version for every new collection added dynamically
      const nextVersion = Math.max(this.verno, this._pendingVersion) + 1;
      this._pendingVersion = nextVersion;

      if (this.isOpen()) {
        this.close();
      }

      this.version(nextVersion).stores(this._currentSchema);
      this.tableMap.set(name, this.table(name));
      this.tableMap.set("_sync_logs", this.table("_sync_logs"));
    }
    return this.tableMap.get(name)!;
  }

  get syncLogs(): Table {
    return this.table("_sync_logs");
  }
}

/**
 * Internal database client. Wraps Dexie and manages collection instances.
 * Use via {@link ZerithDBApp.db} — not instantiated directly.
 */
export class DbClient {
  private readonly dexie: ZerithDBDexie;
  private readonly appId: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private readonly collections = new Map<string, CollectionClient<any>>();
  public readonly peerId: string;

  constructor(config: ZerithDBConfig) {
    this.appId = config.appId;
    this.dexie = new ZerithDBDexie(config.appId);
    // Simplified peerId generation - in production this should be stable
    this.peerId = uuidv7();
  }

  collection<T extends Record<string, any>>(name: string): CollectionClient<T> {
    if (typeof name !== "string" || name.trim() === "") {
      throw new ZerithDBError(ErrorCode.DB_INIT_FAILED, "Collection name must be a non-empty string");
    }
    if (!this.collections.has(name)) {
      const table = this.dexie.ensureCollection(name);
      this.collections.set(
        name,
        new CollectionClient<T>(table as Table<Document<T>>, name, this.peerId)
      );
    }
    return this.collections.get(name) as CollectionClient<T>;
  }

  async logConflict(log: any): Promise<void> {
    await this.dexie.syncLogs.add(log);
  }

  async getSyncLogs(): Promise<any[]> {
    return this.dexie.syncLogs.toArray();
  }

  async getMemoryStats(): Promise<{ recordCount: number; collections: Record<string, number> }> {
    const collections: Record<string, number> = {};
    let recordCount = 0;

    for (const [name, client] of this.collections) {
      const count = await client.count();
      collections[name] = count;
      recordCount += count;
    }

    return { recordCount, collections };
  }

  /**
   * Returns names of collections that have been opened in this session.
   */
  collectionNames(): string[] {
    return Array.from(this.collections.keys());
  }

  /**
   * Returns names of all collections currently stored in IndexedDB.
   */
  allCollectionNames(): string[] {
    return this.dexie.tables.map((t) => t.name).filter((name) => !name.startsWith("_"));
  }

  /**
   * Export all collections to a JSON-serializable snapshot.
   * If options.collections is omitted, it exports ALL collections found in IndexedDB.
   */
  async exportSnapshot(options: BackupExportOptions = {}): Promise<BackupSnapshot> {
    return wrapIDBOperation(
      ErrorCode.DB_READ_FAILED,
      "Failed to export local backup snapshot",
      async () => {
        const collectionNames = options.collections ?? this.allCollectionNames();
        const collections: BackupSnapshot["collections"] = {};

        for (const name of collectionNames) {
          const table = this.dexie.ensureCollection(name);
          collections[name] = (await table.toArray()) as Document<Record<string, any>>[];
        }

        return {
          format: "zerithdb.local-backup.v1",
          appId: this.appId,
          generatedAt: new Date().toISOString(),
          collections,
        };
      }
    );
  }

  async dispose(): Promise<void> {
    this.dexie.close();
  }
}
