/* eslint-disable eslint-comments/disable-enable-pair */
/* eslint-disable @typescript-eslint/no-unsafe-argument */
/* eslint-disable @typescript-eslint/no-non-null-assertion */
/* eslint-disable eslint-comments/no-duplicate-disable */
/* eslint-disable @typescript-eslint/no-dynamic-delete */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-explicit-any */
import { RealtimeChannel, SupabaseClient } from "@supabase/supabase-js"
import {
  ReplicationOptions,
  ReplicationPullHandlerResult,
  ReplicationPullOptions,
  ReplicationPushOptions,
  RxReplicationPullStreamItem,
  RxReplicationWriteToMasterRow,
  WithDeleted,
} from "rxdb"
import { RxReplicationState } from "rxdb/plugins/replication"
import { Subject } from "rxjs"

const DEFAULT_LAST_MODIFIED_FIELD = "_modified"
const DEFAULT_DELETED_FIELD = "_deleted"
const POSTGRES_DUPLICATE_KEY_ERROR_CODE = "23505"
const DEFAULT_BATCH_SIZE = 1000

export type SupabaseReplicationOptions<RxDocType> = {
  /**
   * The SupabaseClient to replicate with.
   */
  supabaseClient: SupabaseClient

  /**
   * The table to replicate to, if different from the name of the collection.
   * @default the name of the RxDB collection.
   */
  table?: string

  /**
   * The primary key of the supabase table, if different from the primary key of the RxDB.
   * @default the primary key of the RxDB collection
   */
  // TODO: Support composite primary keys.
  primaryKey?: string

  /**
   * Options for pulling data from supabase. Set to {} to pull with the default
   * options, as no data will be pulled if the field is absent.
   */
  pull?: Omit<
    ReplicationPullOptions<RxDocType, SupabaseReplicationCheckpoint>,
    "handler" | "stream$"
  > & {
    /**
     * Whether to subscribe to realtime Postgres changes for the table. If set to false,
     * only an initial pull will be performed. Only has an effect if the live option is set
     * to true.
     * @default true
     */
    realtimePostgresChanges?: boolean

    /**
     * The name of the supabase field that is automatically updated to the last
     * modified timestamp by postgres. This field is required for the pull sync
     * to work and can easily be implemented with moddatetime in supabase.
     * @default '_modified'
     */
    lastModifiedField?: string

    /**
     * Amount of documents to fetch from Supabase in one request.
     * @default 1000
     */
        batchSize?: number

    /**
     * Additional options for the fetchData query builder
     */
    fetchOptions?: {
      /**
       * Whether to include deleted records in the pull
       * @default false
       */
      fetchDeleted?: boolean

      /**
       * Additional query parameters for filtering
       */
      queryParams?: Array<{
        field: string
        value: any
        opType?: string
      }>
    }
  }

  /**
   * Options for pushing data to supabase. Set to {} to push with the default
   * options, as no data will be pushed if the field is absent.
   */
  // TODO: enable custom batch size (currently always one row at a time)
  push?: Omit<ReplicationPushOptions<RxDocType>, "handler" | "batchSize"> & {
    /**
     * Handler for pushing row updates to supabase. Must return true iff the UPDATE was
     * applied to the supabase table. Returning false signalises a write conflict, in
     * which case the current state of the row will be fetched from supabase and passed to
     * the RxDB collection's conflict handler.
     * @default the default handler will update the row only iff all fields match the
     * local state (before the update was applied), otherwise the conflict handler is
     * invoked. The default handler does not support JSON fields at the moment.
     */
    // TODO: Support JSON fields
    updateHandler?: (row: RxReplicationWriteToMasterRow<RxDocType>) => Promise<boolean>
  }
} & Omit<
  // We don't support waitForLeadership. You should just run in a SharedWorker anyways, no?
  ReplicationOptions<RxDocType, any>,
  "pull" | "push" | "waitForLeadership"
>

/**
 * The checkpoint stores until which point the client and supabse have been synced.
 * For this to work, we require each row to have a datetime field that contains the
 * last modified time. In case two rows have the same timestamp, we use the primary
 * key to define a strict order.
 */
export interface SupabaseReplicationCheckpoint {
  modified: string
  primaryKeyValue: string | number
}

/**
 * Replicates the local RxDB database with the given Supabase client.
 *
 * See SupabaseReplicationOptions for the various configuration options. For a general introduction
 * to RxDB's replication protocol, see https://rxdb.info/replication.html
 */
export class SupabaseReplication<RxDocType> extends RxReplicationState<
  RxDocType,
  SupabaseReplicationCheckpoint
> {
  private readonly table: string
  private readonly primaryKey: string
  private readonly lastModifiedFieldName: string

  private readonly realtimeChanges: Subject<
    RxReplicationPullStreamItem<RxDocType, SupabaseReplicationCheckpoint>
  >
  private realtimeChannel?: RealtimeChannel

  constructor(private options: SupabaseReplicationOptions<RxDocType>) {
    const realtimeChanges = new Subject<
      RxReplicationPullStreamItem<RxDocType, SupabaseReplicationCheckpoint>
    >()
    super(
      options.replicationIdentifier,
      options.collection,
      options.deletedField || DEFAULT_DELETED_FIELD,
      options.pull && {
        ...options.pull,
        batchSize: options.pull.batchSize || DEFAULT_BATCH_SIZE,
        stream$: realtimeChanges,
        handler: (
          lastPulledCheckpoint: SupabaseReplicationCheckpoint | undefined,
          batchSize: number,
        ) =>
          this.pullHandler(
            lastPulledCheckpoint || { modified: "", primaryKeyValue: "" },
            batchSize,
          ),
      },
      options.push && {
        ...options.push,
        batchSize: 1, // TODO: support batch insertion
        handler: (rows: RxReplicationWriteToMasterRow<RxDocType>[]) => this.pushHandler(rows),
      },
      typeof options.live === "undefined" ? true : options.live,
      typeof options.retryTime === "undefined" ? 5000 : options.retryTime,
      typeof options.autoStart === "undefined" ? true : options.autoStart,
    )
    this.realtimeChanges = realtimeChanges
    this.table = options.table || options.collection.name
    this.primaryKey = options.primaryKey || options.collection.schema.primaryPath
    this.lastModifiedFieldName = options.pull?.lastModifiedField || DEFAULT_LAST_MODIFIED_FIELD
    if (this.autoStart) {
      void this.start()
    }
  }
  public override async start(): Promise<void> {
    if (
      this.live &&
      this.options.pull &&
      (this.options.pull?.realtimePostgresChanges ||
        typeof this.options.pull.realtimePostgresChanges === "undefined")
    ) {
      this.watchPostgresChanges()
    }
    return super.start()
  }

  public override async cancel(): Promise<any> {
    if (this.realtimeChannel) {
      return Promise.all([super.cancel(), this.realtimeChannel.unsubscribe()])
    }
    return super.cancel()
  }

  private getCurrentCollectionColumnNames(): string[] {
    return Object.keys(this.collection.internalStorageInstance.schema.properties).filter(
      (name) => !name.startsWith("_"),
    )
  }

  /**
   * Pulls all changes since the last checkpoint from supabase.
   */
  private async pullHandler(
    lastPulledCheckpoint: SupabaseReplicationCheckpoint,
    batchSize: number,
  ): Promise<ReplicationPullHandlerResult<RxDocType, SupabaseReplicationCheckpoint>> {
    try {
      const fetchOptions = this.options.pull?.fetchOptions || {}
      let query = this.options.supabaseClient.from(this.table).select("*")

      // Apply filter for deleted items if not fetching deleted
      if (!fetchOptions.fetchDeleted) {
        query = query.eq("is_deleted", false)
      }

      // Apply additional query parameters
      if (fetchOptions.queryParams?.length) {
        fetchOptions.queryParams.forEach(({ field, value, opType }) => {
          if (value === undefined || value === null) return

          switch (opType) {
            case "is":
              query = query.is(field, value)
              break
            case "cs":
              query = query.contains(field, `{"${value}"}`)
              break
            case "json_cs":
              query = query.contains(field, value)
              break
            case "or":
              if (Array.isArray(value)) {
                query = query.in(field, value)
              } else {
                query = query.eq(field, value)
              }
              break
            case "not_eq":
              query = query.neq(field, value)
              break
            case "between":
              if (value && value.from && value.to) {
                query = query.gte(field, value.from).lte(field, value.to)
              }
              break
            case "gt":
              query = query.gt(field, value)
              break
            case "lt":
              query = query.lt(field, value)
              break
            default:
              query = query.eq(field, value)
          }
        })
      }

      console.log("PKG-RXDB-SUPABASE QUERY after fetchData:", query)
      if (lastPulledCheckpoint && lastPulledCheckpoint.modified) {
        // Construct the PostgREST query for the following condition:
        // WHERE _modified > lastModified OR (_modified = lastModified AND primaryKey > lastPrimaryKey)
        const lastModified = JSON.stringify(lastPulledCheckpoint.modified)
        const lastPrimaryKey = JSON.stringify(lastPulledCheckpoint.primaryKeyValue) // TODO: Add test for a integer primary key
        const isNewer = `${this.lastModifiedFieldName}.gt.${lastModified}`
        const isSameAge = `${this.lastModifiedFieldName}.eq.${lastModified}`
        // This has tiebreaking by primary key if last modified is same, but that will not usually work for us as we use UUIDs
        query = query.or(`${isNewer},and(${isSameAge},${this.primaryKey}.gt.${lastPrimaryKey})`)
      }
      query = query.order(this.lastModifiedFieldName).order(this.primaryKey).limit(batchSize)
      console.log("PKG-RXDB-SUPABASE PROPERTIES :", this.getCurrentCollectionColumnNames())
      console.debug(
        "PKG-RXDB-SUPABASE : Pulling changes since",
        lastPulledCheckpoint?.modified,
        "with query",
        (query as any)["url"].toString(),
      )
      const { data, error } = await query
      if (error) throw error
      if (!data || data.length === 0) {
        return {
          checkpoint: lastPulledCheckpoint,
          documents: [],
        }
      } else {
        const documents = data.map(this.rowToRxDoc.bind(this))
        const checkpoint = this.rowToCheckpoint(data[data.length - 1])
        return {
          checkpoint,
          documents,
        }
      }
    } catch (error) {
      console.error("PKG-RXDB-SUPABASE : pullHandler error", error)
      throw error
    }
  }

  /**
   * Pushes local changes to supabase.
   */
  private async pushHandler(
    rows: RxReplicationWriteToMasterRow<RxDocType>[],
  ): Promise<WithDeleted<RxDocType>[]> {
    // TODO: check if user is online, if not do not push
    if (rows.length != 1) throw new Error("Invalid batch size")
    const row = rows[0]
    return row.assumedMasterState
      ? this.handleUpdate(row)
      : this.handleInsertion(row.newDocumentState)
  }

  /**
   * Tries to insert a new row. Returns the current state of the row in case of a conflict.
   */
  private async handleInsertion(doc: WithDeleted<RxDocType>): Promise<WithDeleted<RxDocType>[]> {
    const { error } = await this.options.supabaseClient.from(this.table).insert(doc)
    if (!error) {
      return [] // Success :)
    } else if (error.code == POSTGRES_DUPLICATE_KEY_ERROR_CODE) {
      // The row was already inserted. Fetch current state and let conflict handler resolve it.
      return [await this.fetchByPrimaryKey((doc as any)[this.primaryKey])]
    } else {
      throw error
    }
  }

  /**
   * Updates a row in supabase if all fields match the local state. Otherwise, the current
   * state is fetched and passed to the conflict handler.
   */
  private async handleUpdate(
    row: RxReplicationWriteToMasterRow<RxDocType>,
  ): Promise<WithDeleted<RxDocType>[]> {
    const updateHandler = this.options.push?.updateHandler || this.defaultUpdateHandler.bind(this)
    if (await updateHandler(row)) return [] // Success :)
    // Fetch current state and let conflict handler resolve it.
    return [await this.fetchByPrimaryKey((row.newDocumentState as any)[this.primaryKey])]
  }

  /**
   * Updates the row only if all database fields match the expected state.
   */
  private async defaultUpdateHandler(
    row: RxReplicationWriteToMasterRow<RxDocType>,
  ): Promise<boolean> {
    const id = (row.assumedMasterState as any).id
    if (!id) {
      throw new Error("Document id not found in assumedMasterState")
    }
    let query = this.options.supabaseClient
      .from(this.table)
      .update(row.newDocumentState)
      .eq("id", id)
    const { error } = await query
    if (error) {
      let _error = `PKG-RXDB-SUPABASE : Error updating row : ${JSON.stringify(error)}`
      console.error(_error)
      throw Error(_error)
    }
    return true
  }

  private watchPostgresChanges() {
    this.realtimeChannel = this.options.supabaseClient
      .channel(`rxdb-supabase-${this.replicationIdentifier}`)
      .on(
        "postgres_changes",
        { event: "*", schema: "public", table: this.table },
        (payload: { eventType: string; new: any }) => {
          if (payload.eventType === "DELETE" || !payload.new) return // Should have set _deleted field already
          //console.debug('Realtime event received:', payload)
          this.realtimeChanges.next({
            checkpoint: this.rowToCheckpoint(payload.new),
            documents: [this.rowToRxDoc(payload.new)],
          })
        },
      )
      .subscribe()
  }

  private async fetchByPrimaryKey(primaryKeyValue: any): Promise<WithDeleted<RxDocType>> {
    const { data, error } = await this.options.supabaseClient
      .from(this.table)
      .select()
      .eq(this.primaryKey, primaryKeyValue)
      .limit(1)
    if (error) throw error
    if (data.length != 1) throw new Error("No row with given primary key")
    return this.rowToRxDoc(data[0])
  }

  private rowToRxDoc(row: any): WithDeleted<RxDocType> {
    // Don't delete the field if it is part of the collection else drop the extra field, only sync fields that are part of the collection
    const _columns = this.getCurrentCollectionColumnNames()
    // if lastModifiedField is not part of the collection, throw an error
    if (!_columns.includes(this.lastModifiedFieldName)) {
      throw new Error(
        `lastModifiedField : '${this.lastModifiedFieldName}' not found in collection schema`,
      )
    }

    // Only keep columns that exist in the collection schema
    Object.keys(row).forEach((key) => {
      if (!_columns.includes(key)) {
        delete row[key]
      }
    })

    return row as WithDeleted<RxDocType>
  }

  private rowToCheckpoint(row: any): SupabaseReplicationCheckpoint {
    return {
      modified: row[this.lastModifiedFieldName],
      primaryKeyValue: row[this.primaryKey],
    }
  }
}
