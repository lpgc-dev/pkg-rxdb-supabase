import { RxReplicationState } from "rxdb/plugins/replication";
import { Subject } from "rxjs";
const DEFAULT_LAST_MODIFIED_FIELD = "_modified";
const DEFAULT_DELETED_FIELD = "_deleted";
const POSTGRES_DUPLICATE_KEY_ERROR_CODE = "23505";
class SupabaseReplication extends RxReplicationState {
  constructor(options) {
    const realtimeChanges = new Subject();
    super(
      options.replicationIdentifier,
      options.collection,
      options.deletedField || DEFAULT_DELETED_FIELD,
      options.pull && {
        ...options.pull,
        stream$: realtimeChanges,
        handler: (lastPulledCheckpoint, batchSize) => this.pullHandler(
          lastPulledCheckpoint || { modified: "", primaryKeyValue: "" },
          batchSize
        )
      },
      options.push && {
        ...options.push,
        batchSize: 1,
        // TODO: support batch insertion
        handler: (rows) => this.pushHandler(rows)
      },
      typeof options.live === "undefined" ? true : options.live,
      typeof options.retryTime === "undefined" ? 5e3 : options.retryTime,
      typeof options.autoStart === "undefined" ? true : options.autoStart
    );
    this.options = options;
    this.realtimeChanges = realtimeChanges;
    this.table = options.table || options.collection.name;
    this.primaryKey = options.primaryKey || options.collection.schema.primaryPath;
    this.lastModifiedFieldName = options.pull?.lastModifiedField || DEFAULT_LAST_MODIFIED_FIELD;
    console.log("-------- PKG: THIS OBJECT", this);
    if (this.autoStart) {
      void this.start();
    }
  }
  table;
  primaryKey;
  lastModifiedFieldName;
  realtimeChanges;
  realtimeChannel;
  async start() {
    console.log(
      "-------- PKG: start",
      this.live,
      this.options.pull,
      this.options.pull?.realtimePostgresChanges
    );
    if (this.live && this.options.pull && (this.options.pull?.realtimePostgresChanges || typeof this.options.pull.realtimePostgresChanges === "undefined")) {
      console.log("-------- PKG: watching postgres changes");
      this.watchPostgresChanges();
    }
    console.log("-------- PKG: starting super.start");
    return super.start();
  }
  async cancel() {
    if (this.realtimeChannel) {
      return Promise.all([super.cancel(), this.realtimeChannel.unsubscribe()]);
    }
    return super.cancel();
  }
  /**
   * Pulls all changes since the last checkpoint from supabase.
   */
  async pullHandler(lastPulledCheckpoint, batchSize) {
    try {
      console.log("-------- PKG1: pullHandler", lastPulledCheckpoint);
      console.log("-------- PKG1: BATCH SIZE", batchSize);
      console.log("-------- PKG1: TABLE", this.table);
      let query = this.options.supabaseClient.from(this.table).select();
      console.log("-------- PKG: QUERY", query);
      console.log("-------- PKG: QUERY 2", JSON.stringify(query));
      if (lastPulledCheckpoint && lastPulledCheckpoint.modified) {
        const lastModified = JSON.stringify(lastPulledCheckpoint.modified);
        const lastPrimaryKey = JSON.stringify(lastPulledCheckpoint.primaryKeyValue);
        const isNewer = `${this.lastModifiedFieldName}.gt.${lastModified}`;
        const isSameAge = `${this.lastModifiedFieldName}.eq.${lastModified}`;
        query = query.or(`${isNewer},and(${isSameAge},${this.primaryKey}.gt.${lastPrimaryKey})`);
      }
      query = query.order(this.lastModifiedFieldName).order(this.primaryKey).limit(batchSize);
      console.log(
        "-------- PKG: Pulling changes since",
        lastPulledCheckpoint?.modified,
        "with query",
        query["url"].toString()
      );
      const { data, error } = await query;
      console.log("-------- PKG: DATA", data);
      console.log("-------- PKG: ERROR", error);
      if (error)
        throw error;
      if (!data || data.length === 0) {
        console.log("-------- PKG: No new data to pull");
        return {
          checkpoint: lastPulledCheckpoint,
          documents: []
        };
      } else {
        const documents = data.map(this.rowToRxDoc.bind(this));
        const checkpoint = this.rowToCheckpoint(data[data.length - 1]);
        console.log("-------- PKG: Returning documents:", documents);
        console.log("-------- PKG: New checkpoint:", checkpoint);
        return {
          checkpoint,
          documents
        };
      }
    } catch (error) {
      console.log("-------- PKG: pullHandler error", error);
      throw error;
    }
  }
  /**
   * Pushes local changes to supabase.
   */
  async pushHandler(rows) {
    if (rows.length != 1)
      throw new Error("Invalid batch size");
    const row = rows[0];
    return row.assumedMasterState ? this.handleUpdate(row) : this.handleInsertion(row.newDocumentState);
  }
  /**
   * Tries to insert a new row. Returns the current state of the row in case of a conflict.
   */
  async handleInsertion(doc) {
    const { error } = await this.options.supabaseClient.from(this.table).insert(doc);
    if (!error) {
      return [];
    } else if (error.code == POSTGRES_DUPLICATE_KEY_ERROR_CODE) {
      return [await this.fetchByPrimaryKey(doc[this.primaryKey])];
    } else {
      throw error;
    }
  }
  /**
   * Updates a row in supabase if all fields match the local state. Otherwise, the current
   * state is fetched and passed to the conflict handler.
   */
  async handleUpdate(row) {
    const updateHandler = this.options.push?.updateHandler || this.defaultUpdateHandler.bind(this);
    if (await updateHandler(row))
      return [];
    return [await this.fetchByPrimaryKey(row.newDocumentState[this.primaryKey])];
  }
  /**
   * Updates the row only if all database fields match the expected state.
   */
  async defaultUpdateHandler(row) {
    let query = this.options.supabaseClient.from(this.table).update(row.newDocumentState, { count: "exact" });
    Object.entries(row.assumedMasterState).forEach(([field, value]) => {
      const type = typeof value;
      if (type === "string" || type === "number") {
        query = query.eq(field, value);
      } else if (type === "boolean" || value === null) {
        query = query.is(field, value);
      } else {
        throw new Error(`replicateSupabase: Unsupported field of type ${type}`);
      }
    });
    const { error, count } = await query;
    if (error)
      throw error;
    return count == 1;
  }
  watchPostgresChanges() {
    this.realtimeChannel = this.options.supabaseClient.channel(`rxdb-supabase-${this.replicationIdentifier}`).on(
      "postgres_changes",
      { event: "*", schema: "public", table: this.table },
      (payload) => {
        console.log("-------- PKG: realtime event received", payload);
        if (payload.eventType === "DELETE" || !payload.new)
          return;
        this.realtimeChanges.next({
          checkpoint: this.rowToCheckpoint(payload.new),
          documents: [this.rowToRxDoc(payload.new)]
        });
      }
    ).subscribe();
  }
  async fetchByPrimaryKey(primaryKeyValue) {
    const { data, error } = await this.options.supabaseClient.from(this.table).select().eq(this.primaryKey, primaryKeyValue).limit(1);
    if (error)
      throw error;
    if (data.length != 1)
      throw new Error("No row with given primary key");
    return this.rowToRxDoc(data[0]);
  }
  rowToRxDoc(row) {
    delete row[this.lastModifiedFieldName];
    return row;
  }
  rowToCheckpoint(row) {
    console.log("-------- PKG: lastModifiedFieldName", this.lastModifiedFieldName);
    console.log("-------- PKG: primaryKey", this.primaryKey);
    console.log("-------- PKG: row", row);
    return {
      modified: row[this.lastModifiedFieldName],
      primaryKeyValue: row[this.primaryKey]
    };
  }
}
export {
  SupabaseReplication
};
//# sourceMappingURL=supabase-replication.js.map
