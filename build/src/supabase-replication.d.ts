import { SupabaseClient } from "@supabase/supabase-js";
import { ReplicationOptions, ReplicationPullOptions, ReplicationPushOptions, RxReplicationWriteToMasterRow } from "rxdb";
import { RxReplicationState } from "rxdb/plugins/replication";
export type SupabaseReplicationOptions<RxDocType> = {
    /**
     * The SupabaseClient to replicate with.
     */
    supabaseClient: SupabaseClient;
    /**
     * The table to replicate to, if different from the name of the collection.
     * @default the name of the RxDB collection.
     */
    table?: string;
    /**
     * The primary key of the supabase table, if different from the primary key of the RxDB.
     * @default the primary key of the RxDB collection
     */
    primaryKey?: string;
    /**
     * Options for pulling data from supabase. Set to {} to pull with the default
     * options, as no data will be pulled if the field is absent.
     */
    pull?: Omit<ReplicationPullOptions<RxDocType, SupabaseReplicationCheckpoint>, "handler" | "stream$"> & {
        /**
         * Whether to subscribe to realtime Postgres changes for the table. If set to false,
         * only an initial pull will be performed. Only has an effect if the live option is set
         * to true.
         * @default true
         */
        realtimePostgresChanges?: boolean;
        /**
         * The name of the supabase field that is automatically updated to the last
         * modified timestamp by postgres. This field is required for the pull sync
         * to work and can easily be implemented with moddatetime in supabase.
         * @default '_modified'
         */
        lastModifiedField?: string;
        /**
         * Additional options for the fetchData query builder
         */
        fetchOptions?: {
            /**
             * Whether to include deleted records in the pull
             * @default false
             */
            fetchDeleted?: boolean;
            /**
             * Search text to filter records
             */
            searchText?: string | null;
            /**
             * Additional query parameters for filtering
             */
            queryParams?: Array<{
                field: string;
                value: any;
                opType?: string;
            }>;
        };
    };
    /**
     * Options for pushing data to supabase. Set to {} to push with the default
     * options, as no data will be pushed if the field is absent.
     */
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
        updateHandler?: (row: RxReplicationWriteToMasterRow<RxDocType>) => Promise<boolean>;
    };
} & Omit<ReplicationOptions<RxDocType, any>, "pull" | "push" | "waitForLeadership">;
/**
 * The checkpoint stores until which point the client and supabse have been synced.
 * For this to work, we require each row to have a datetime field that contains the
 * last modified time. In case two rows have the same timestamp, we use the primary
 * key to define a strict order.
 */
export interface SupabaseReplicationCheckpoint {
    modified: string;
    primaryKeyValue: string | number;
}
/**
 * Replicates the local RxDB database with the given Supabase client.
 *
 * See SupabaseReplicationOptions for the various configuration options. For a general introduction
 * to RxDB's replication protocol, see https://rxdb.info/replication.html
 */
export declare class SupabaseReplication<RxDocType> extends RxReplicationState<RxDocType, SupabaseReplicationCheckpoint> {
    private options;
    private readonly table;
    private readonly primaryKey;
    private readonly lastModifiedFieldName;
    private readonly realtimeChanges;
    private realtimeChannel?;
    constructor(options: SupabaseReplicationOptions<RxDocType>);
    start(): Promise<void>;
    cancel(): Promise<any>;
    private getCurrentCollectionColumnNames;
    /**
     * Pulls all changes since the last checkpoint from supabase.
     */
    private pullHandler;
    /**
     * Pushes local changes to supabase.
     */
    private pushHandler;
    /**
     * Tries to insert a new row. Returns the current state of the row in case of a conflict.
     */
    private handleInsertion;
    /**
     * Updates a row in supabase if all fields match the local state. Otherwise, the current
     * state is fetched and passed to the conflict handler.
     */
    private handleUpdate;
    /**
     * Updates the row only if all database fields match the expected state.
     */
    private defaultUpdateHandler;
    private watchPostgresChanges;
    private fetchByPrimaryKey;
    private rowToRxDoc;
    private rowToCheckpoint;
}
